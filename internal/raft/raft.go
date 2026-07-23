// Package raft implements the Raft consensus algorithm, the mechanism that will
// let the log survive a node failure (Phase 8). This is built incrementally, the
// way the Raft paper and the MIT 6.5840 labs do it:
//
//	step 1 — leader election      (this file)
//	step 2 — log replication      (AppendEntries carries entries; commit index)
//	step 3 — persistence          (term/votedFor/log survive restart)
//	step 4 — snapshots            (bound the log)
//	step 5 — integrate with the WAL (apply committed entries to segment.Manager)
//
// The node talks to peers only through the Transport interface, so a whole
// cluster can be wired together in-memory and tested deterministically before any
// TCP is involved. Nothing here touches the WAL yet.
//
// This file covers leader election: terms as a logical clock, randomized election
// timeouts, RequestVote, and heartbeats via (empty) AppendEntries. The safety
// property it guarantees is "at most one leader per term".
package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Term is Raft's logical clock: a monotonically increasing number that lets nodes
// detect stale leaders and order events without synchronized clocks.
type Term uint64

// NodeID identifies a node in the cluster.
type NodeID int

// None marks "no vote cast this term".
const None NodeID = -1

// State is a node's role. A node is exactly one of these at any moment.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Default timing. Election timeout must be comfortably larger than the heartbeat
// interval, or followers time out before a healthy leader's heartbeat arrives.
const (
	DefaultHeartbeatInterval  = 50 * time.Millisecond
	DefaultElectionTimeoutMin = 150 * time.Millisecond
	DefaultElectionTimeoutMax = 300 * time.Millisecond
)

// Config constructs a node.
type Config struct {
	ID        NodeID
	Peers     []NodeID // the OTHER nodes (self is not included)
	Transport Transport

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

// Raft is a single node in the cluster.
type Raft struct {
	mu sync.Mutex

	id        NodeID
	peers     []NodeID
	transport Transport

	heartbeatInterval  time.Duration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration

	// Persistent state (will be written to disk in step 3).
	currentTerm Term
	votedFor    NodeID

	// Volatile state.
	state            State
	leaderID         NodeID
	electionDeadline time.Time // when a follower/candidate will start an election

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// New builds a node in the Follower state. Call Start to run its timers.
func New(cfg Config) *Raft {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = DefaultElectionTimeoutMin
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin * 2
	}
	r := &Raft{
		id:                 cfg.ID,
		peers:              append([]NodeID(nil), cfg.Peers...),
		transport:          cfg.Transport,
		heartbeatInterval:  cfg.HeartbeatInterval,
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		currentTerm:        0,
		votedFor:           None,
		state:              Follower,
		leaderID:           None,
		stopCh:             make(chan struct{}),
	}
	r.resetElectionDeadlineLocked()
	return r
}

// Start launches the election and heartbeat loops.
func (r *Raft) Start() {
	r.wg.Add(2)
	go r.electionLoop()
	go r.heartbeatLoop()
}

// Stop halts the node's loops. Safe to call more than once.
func (r *Raft) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
	r.wg.Wait()
}

// State returns the node's current role.
func (r *Raft) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// CurrentTerm returns the node's current term.
func (r *Raft) CurrentTerm() Term {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

// clusterSize is the total number of nodes (peers + self).
func (r *Raft) clusterSize() int { return len(r.peers) + 1 }

// majority is the smallest number of votes that forms a majority.
func (r *Raft) majority() int { return r.clusterSize()/2 + 1 }

func (r *Raft) resetElectionDeadlineLocked() {
	span := r.electionTimeoutMax - r.electionTimeoutMin
	timeout := r.electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
	r.electionDeadline = time.Now().Add(timeout)
}

// becomeFollowerLocked steps down to Follower at the given (higher-or-equal) term.
func (r *Raft) becomeFollowerLocked(term Term) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = None
	r.leaderID = None
	r.resetElectionDeadlineLocked()
}

// electionLoop starts an election whenever a follower/candidate goes too long
// without hearing from a leader (or winning).
func (r *Raft) electionLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.Lock()
			timedOut := r.state != Leader && time.Now().After(r.electionDeadline)
			r.mu.Unlock()
			if timedOut {
				r.startElection()
			}
		}
	}
}

// heartbeatLoop makes the leader broadcast heartbeats so followers stay put.
func (r *Raft) heartbeatLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.sendHeartbeats()
		}
	}
}

// startElection transitions to Candidate, votes for self, and asks peers for votes.
func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.leaderID = None
	r.resetElectionDeadlineLocked()
	term := r.currentTerm
	lastLogTerm, lastLogIndex := r.lastLogTermIndexLocked()
	peers := append([]NodeID(nil), r.peers...)
	r.mu.Unlock()

	votes := 1 // vote for self

	for _, peer := range peers {
		go func(peer NodeID) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  r.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := r.transport.SendRequestVote(peer, args)
			if err != nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()
			// Ignore if we moved on (newer term or no longer a candidate for `term`).
			if r.state != Candidate || r.currentTerm != term {
				return
			}
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if !reply.VoteGranted {
				return
			}
			votes++
			if votes >= r.majority() {
				r.becomeLeaderLocked()
			}
		}(peer)
	}
}

// becomeLeaderLocked wins the election and immediately asserts authority with a
// heartbeat (so followers don't start competing elections).
func (r *Raft) becomeLeaderLocked() {
	if r.state != Candidate {
		return
	}
	r.state = Leader
	r.leaderID = r.id
	go r.sendHeartbeats()
}

// sendHeartbeats broadcasts empty AppendEntries if we are the leader.
func (r *Raft) sendHeartbeats() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	peers := append([]NodeID(nil), r.peers...)
	r.mu.Unlock()

	for _, peer := range peers {
		go func(peer NodeID) {
			args := AppendEntriesArgs{Term: term, LeaderID: r.id}
			reply, err := r.transport.SendAppendEntries(peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
			}
			r.mu.Unlock()
		}(peer)
	}
}

// lastLogTermIndexLocked returns the term and index of the last log entry. With
// no log yet (leader election only), both are zero; step 2 fills this in.
func (r *Raft) lastLogTermIndexLocked() (Term, uint64) {
	return 0, 0
}
