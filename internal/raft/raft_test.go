package raft

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// errDisconnected simulates a network partition or a crashed node: RPCs to or
// from a disconnected node fail.
var errDisconnected = errors.New("raft: node disconnected")

// network wires a set of nodes together in memory and can connect/disconnect any
// of them to simulate partitions and crashes — the same knobs a real Raft test
// suite uses, without any real sockets.
type network struct {
	mu        sync.Mutex
	nodes     map[NodeID]*Raft
	connected map[NodeID]bool
}

func newNetwork() *network {
	return &network{nodes: map[NodeID]*Raft{}, connected: map[NodeID]bool{}}
}

func (n *network) add(r *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[r.id] = r
	n.connected[r.id] = true
}

func (n *network) setConnected(id NodeID, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = up
}

// deliverable reports whether an RPC between from and to can be delivered, and
// returns the target node. Both ends must be connected.
func (n *network) deliverable(from, to NodeID) (*Raft, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.connected[from] || !n.connected[to] {
		return nil, false
	}
	return n.nodes[to], true
}

// transport is one node's view of the network.
type transport struct {
	net  *network
	from NodeID
}

func (t *transport) SendRequestVote(to NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	target, ok := t.net.deliverable(t.from, to)
	if !ok {
		return RequestVoteReply{}, errDisconnected
	}
	return target.RequestVote(args), nil
}

func (t *transport) SendAppendEntries(to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	target, ok := t.net.deliverable(t.from, to)
	if !ok {
		return AppendEntriesReply{}, errDisconnected
	}
	return target.AppendEntries(args), nil
}

// cluster is n nodes on one network with fast (test-scaled) timers.
type cluster struct {
	net   *network
	nodes []*Raft

	appliedMu sync.Mutex
	applied   []map[uint64][]byte // per node: committed index -> command
	applyChs  []chan ApplyMsg
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	net := newNetwork()
	c := &cluster{
		net:      net,
		applied:  make([]map[uint64][]byte, n),
		applyChs: make([]chan ApplyMsg, n),
	}
	ids := make([]NodeID, n)
	for i := 0; i < n; i++ {
		ids[i] = NodeID(i)
	}
	for i := 0; i < n; i++ {
		var peers []NodeID
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, ids[j])
			}
		}
		c.applied[i] = map[uint64][]byte{}
		ch := make(chan ApplyMsg, 256)
		c.applyChs[i] = ch
		r := New(Config{
			ID:                 ids[i],
			Peers:              peers,
			Transport:          &transport{net: net, from: ids[i]},
			ApplyCh:            ch,
			HeartbeatInterval:  20 * time.Millisecond,
			ElectionTimeoutMin: 60 * time.Millisecond,
			ElectionTimeoutMax: 120 * time.Millisecond,
		})
		net.add(r)
		c.nodes = append(c.nodes, r)

		// Record everything this node applies, so tests can assert what committed.
		go func(idx int, ch chan ApplyMsg) {
			for msg := range ch {
				if !msg.CommandValid {
					continue
				}
				c.appliedMu.Lock()
				c.applied[idx][msg.CommandIndex] = msg.Command
				c.appliedMu.Unlock()
			}
		}(i, ch)
	}
	for _, r := range c.nodes {
		r.Run()
	}
	t.Cleanup(func() {
		for _, r := range c.nodes {
			r.Stop()
		}
		for _, ch := range c.applyChs {
			close(ch) // stop the recorder goroutines (nodes have stopped applying)
		}
	})
	return c
}

func (c *cluster) majority() int { return len(c.nodes)/2 + 1 }

// committedAt returns how many nodes applied the given index and the command
// they applied. If two nodes applied DIFFERENT commands at the same index that
// is a safety violation, signalled by count == -1.
func (c *cluster) committedAt(index uint64) (int, []byte) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	count := 0
	var cmd []byte
	for i := range c.nodes {
		got, ok := c.applied[i][index]
		if !ok {
			continue
		}
		if count > 0 && !bytes.Equal(cmd, got) {
			return -1, nil
		}
		cmd = got
		count++
	}
	return count, cmd
}

// one submits a command and waits until a majority has committed it at the same
// index, returning that index. It retries against whichever node is leader,
// absorbing elections — the standard Raft test workhorse.
func (c *cluster) one(t *testing.T, cmd []byte) uint64 {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		var index uint64
		submitted := false
		for _, r := range c.nodes {
			if idx, _, isLeader := r.Start(cmd); isLeader {
				index, submitted = idx, true
				break
			}
		}
		if submitted {
			until := time.Now().Add(1 * time.Second)
			for time.Now().Before(until) {
				count, got := c.committedAt(index)
				if count < 0 {
					t.Fatalf("conflicting commands committed at index %d", index)
				}
				if count >= c.majority() && bytes.Equal(got, cmd) {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("command %q never committed on a majority", cmd)
	return 0
}

// leadersByTerm returns every CONNECTED node claiming to be leader, grouped by
// term. Disconnected nodes are skipped on purpose: a leader isolated into a
// minority stays a stale leader (it just can't commit) until it rejoins and sees
// a higher term, which is correct Raft behavior — not a second live leader.
func (c *cluster) leadersByTerm() map[Term][]NodeID {
	byTerm := map[Term][]NodeID{}
	for _, r := range c.nodes {
		c.net.mu.Lock()
		up := c.net.connected[r.id]
		c.net.mu.Unlock()
		if !up {
			continue
		}
		r.mu.Lock()
		if r.state == Leader {
			byTerm[r.currentTerm] = append(byTerm[r.currentTerm], r.id)
		}
		r.mu.Unlock()
	}
	return byTerm
}

// checkOneLeader waits up to ~2s for exactly one leader in the latest term and
// returns it. Retrying like this absorbs the inherent timing of elections.
func (c *cluster) checkOneLeader(t *testing.T) NodeID {
	t.Helper()
	for iter := 0; iter < 20; iter++ {
		time.Sleep(100 * time.Millisecond)
		byTerm := c.leadersByTerm()
		lastTerm := Term(0)
		for term := range byTerm {
			if term > lastTerm {
				lastTerm = term
			}
		}
		if lastTerm == 0 {
			continue // no leader yet
		}
		if leaders := byTerm[lastTerm]; len(leaders) == 1 {
			// Reject if an OLDER term also still has a "leader" that hasn't stepped
			// down — that would be two leaders. Only the newest term should lead.
			for term, ls := range byTerm {
				if term != lastTerm && len(ls) > 0 {
					t.Fatalf("stale leader in term %d while term %d leads", term, lastTerm)
				}
			}
			return leaders[0]
		}
		if len(byTerm[lastTerm]) > 1 {
			t.Fatalf("term %d has %d leaders (want 1): %v", lastTerm, len(byTerm[lastTerm]), byTerm[lastTerm])
		}
	}
	t.Fatalf("no single leader elected within timeout")
	return None
}

func TestElectsSingleLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader(t)
}

func TestElectsLeaderFiveNodes(t *testing.T) {
	c := newCluster(t, 5)
	c.checkOneLeader(t)
}

func TestReElectsAfterLeaderDisconnect(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader(t)
	termBefore := c.nodes[leader].CurrentTerm()

	// "Crash" the leader: the other two must elect a new one.
	c.net.setConnected(leader, false)
	newLeader := c.checkOneLeader(t)
	if newLeader == leader {
		t.Fatalf("disconnected leader %d is still the leader", leader)
	}
	if got := c.nodes[newLeader].CurrentTerm(); got <= termBefore {
		t.Fatalf("new leader term %d should exceed old term %d", got, termBefore)
	}
}

func TestOldLeaderStepsDownAfterRejoin(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader(t)

	c.net.setConnected(leader, false) // isolate old leader
	newLeader := c.checkOneLeader(t)
	newTerm := c.nodes[newLeader].CurrentTerm()

	// Reconnect the old leader; it must discover the higher term and step down.
	c.net.setConnected(leader, true)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.nodes[leader].State() == Follower && c.nodes[leader].CurrentTerm() >= newTerm {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("old leader %d did not step down (state=%v term=%d, cluster term=%d)",
		leader, c.nodes[leader].State(), c.nodes[leader].CurrentTerm(), newTerm)
}

func TestIsolatedNodeCannotBecomeLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader(t)

	// Isolate a follower. Alone it can never gather a majority (1 of 3), so it
	// keeps timing out and re-running elections but must never win.
	follower := None
	for _, r := range c.nodes {
		if r.id != leader {
			follower = r.id
			break
		}
	}
	c.net.setConnected(follower, false)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if c.nodes[follower].State() == Leader {
			t.Fatalf("isolated node %d became leader without a majority", follower)
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Meanwhile the majority side (2 of 3) still has its leader.
	c.checkOneLeader(t)
}

// --- log replication (step 2) ---

func TestReplicateSingleCommand(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader(t)

	index := c.one(t, []byte("hello"))
	if index != 1 {
		t.Fatalf("first command should land at index 1, got %d", index)
	}
	// Every node (not just a majority) should converge on it shortly.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count, _ := c.committedAt(index); count == len(c.nodes) {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	count, _ := c.committedAt(index)
	t.Fatalf("only %d/%d nodes applied index %d", count, len(c.nodes), index)
}

func TestReplicateSequentialCommands(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader(t)

	for i := 1; i <= 5; i++ {
		cmd := fmt.Appendf(nil, "cmd-%d", i)
		index := c.one(t, cmd)
		if index != uint64(i) {
			t.Fatalf("command %d committed at index %d, want %d", i, index, i)
		}
	}
}

func TestNoCommitWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader(t)

	// Isolate the two followers → the leader is now a minority and cannot commit.
	var others []NodeID
	for _, r := range c.nodes {
		if r.id != leader {
			others = append(others, r.id)
		}
	}
	c.net.setConnected(others[0], false)
	c.net.setConnected(others[1], false)

	index, _, ok := c.nodes[leader].Start([]byte("orphan"))
	if !ok {
		t.Skip("leader stepped down before we could propose; timing — rerun")
	}
	time.Sleep(500 * time.Millisecond)
	if count, _ := c.committedAt(index); count > 0 {
		t.Fatalf("index %d committed (%d nodes) without a majority", index, count)
	}

	// Restore the majority; the cluster must be able to commit again.
	c.net.setConnected(others[0], true)
	c.net.setConnected(others[1], true)
	c.one(t, []byte("recovered"))
}

func TestFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 3)
	c.one(t, []byte("a"))

	leader := c.checkOneLeader(t)
	var follower NodeID = None
	for _, r := range c.nodes {
		if r.id != leader {
			follower = r.id
			break
		}
	}

	// While a follower is away, the majority keeps committing.
	c.net.setConnected(follower, false)
	c.one(t, []byte("b"))
	c.one(t, []byte("c"))

	// Reconnect: the lagging follower must catch up and apply all three entries.
	c.net.setConnected(follower, true)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c.appliedMu.Lock()
		n := len(c.applied[follower])
		c.appliedMu.Unlock()
		if n >= 3 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.appliedMu.Lock()
	n := len(c.applied[follower])
	c.appliedMu.Unlock()
	t.Fatalf("follower %d caught up only %d/3 entries", follower, n)
}
