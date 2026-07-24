// Package raft implements the Raft consensus algorithm, the mechanism that lets
// the log survive a node failure (Phase 8). It is built incrementally, the way
// the Raft paper and the MIT 6.5840 labs do it:
//
//	step 1 — leader election      (terms, votes, heartbeats)
//	step 2 — log replication      (this file: entries, commit index, apply)  ← done
//	step 3 — persistence          (term/votedFor/log survive restart)
//	step 4 — snapshots            (bound the log)
//	step 5 — integrate with the WAL (apply committed entries to segment.Manager)
//
// The node talks to peers only through the Transport interface, so a whole
// cluster is wired in-memory and tested deterministically before any TCP is
// involved. Committed commands are delivered, in order, on an apply channel — the
// seam where the WAL will later plug in as the state machine.
package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Term is Raft's logical clock: a monotonically increasing number that lets nodes
// detect stale leaders and order events without synchronized clocks.
type Term uint64

// NodeID identifies a node in the cluster.
type NodeID int

// None marks "no vote cast this term" / "no leader known".
const None NodeID = -1

// LogEntry is one replicated command tagged with the term it was created in. The
// term is what makes the log self-describing enough for the consistency check.
type LogEntry struct {
	Term    Term
	Command []byte
}

// ApplyMsg is a committed entry handed to the state machine, in log order.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
}

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
	ApplyCh   chan ApplyMsg // committed commands are delivered here, in order
	Storage   Storage       // where term/votedFor/log are persisted; nil = in-memory

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

// Raft is a single node in the cluster.
type Raft struct {
	mu        sync.Mutex
	applyCond *sync.Cond

	id        NodeID
	peers     []NodeID
	transport Transport

	heartbeatInterval  time.Duration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration

	// Persistent state (will be written to disk in step 3).
	currentTerm Term
	votedFor    NodeID
	log         []LogEntry // log[0] is a sentinel; real entries start at index 1

	// Volatile state on all nodes.
	state            State
	leaderID         NodeID
	commitIndex      uint64 // highest log index known committed
	lastApplied      uint64 // highest log index handed to the state machine
	electionDeadline time.Time

	// Volatile state on leaders, reset on election.
	nextIndex  map[NodeID]uint64 // next index to send to each follower
	matchIndex map[NodeID]uint64 // highest index known replicated on each follower

	storage  Storage
	applyCh  chan ApplyMsg
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// New builds a node in the Follower state. Call Start (Run) to run its loops.
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
	if cfg.ApplyCh == nil {
		cfg.ApplyCh = make(chan ApplyMsg, 256)
	}
	if cfg.Storage == nil {
		cfg.Storage = NewMemoryStorage()
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
		log:                []LogEntry{{Term: 0}}, // sentinel at index 0
		state:              Follower,
		leaderID:           None,
		storage:            cfg.Storage,
		applyCh:            cfg.ApplyCh,
		stopCh:             make(chan struct{}),
	}
	r.applyCond = sync.NewCond(&r.mu)
	r.loadPersistedState()
	r.resetElectionDeadlineLocked()
	return r
}

// loadPersistedState restores term/votedFor/log from storage on startup, so a
// restarted node picks up exactly where it left off.
func (r *Raft) loadPersistedState() {
	state, ok, err := r.storage.Load()
	if err != nil {
		// A production node would refuse to start; for this project we log loudly
		// and start fresh rather than crash.
		log.Printf("raft %d: load persisted state: %v", r.id, err)
		return
	}
	if !ok {
		return
	}
	r.currentTerm = state.CurrentTerm
	r.votedFor = state.VotedFor
	if len(state.Log) > 0 {
		r.log = state.Log
	}
}

// persistLocked writes the current term/votedFor/log to stable storage. It must
// be called (under r.mu) after any change to those fields and BEFORE the RPC that
// caused the change replies — that ordering is what makes Raft crash-safe.
func (r *Raft) persistLocked() {
	state := PersistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         append([]LogEntry(nil), r.log...),
	}
	if err := r.storage.Save(state); err != nil {
		log.Printf("raft %d: persist: %v", r.id, err)
	}
}

// Run launches the election, heartbeat, and apply loops.
func (r *Raft) Run() {
	r.wg.Add(3)
	go r.electionLoop()
	go r.heartbeatLoop()
	go r.applyLoop()
}

// Stop halts the node's loops. Safe to call more than once.
func (r *Raft) Stop() {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		close(r.stopCh)
		r.applyCond.Broadcast() // wake the apply loop so it can exit
		r.mu.Unlock()
	})
	r.wg.Wait()
}

// Start proposes a command. On the leader it appends the command to the log and
// returns the index it will occupy once committed, the current term, and true.
// On a non-leader it returns isLeader=false and the caller should retry elsewhere.
func (r *Raft) Start(command []byte) (index uint64, term Term, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader {
		return 0, r.currentTerm, false
	}
	r.log = append(r.log, LogEntry{Term: r.currentTerm, Command: command})
	index = uint64(len(r.log) - 1)
	r.persistLocked()             // the new entry must be durable before we replicate it
	go r.broadcastAppendEntries() // replicate promptly rather than waiting for the tick
	return index, r.currentTerm, true
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

func (r *Raft) clusterSize() int { return len(r.peers) + 1 }
func (r *Raft) majority() int    { return r.clusterSize()/2 + 1 }

func (r *Raft) lastIndexLocked() uint64 { return uint64(len(r.log) - 1) }

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

func (r *Raft) heartbeatLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.broadcastAppendEntries()
		}
	}
}

// applyLoop delivers committed entries to the state machine in index order.
func (r *Raft) applyLoop() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		for r.lastApplied >= r.commitIndex {
			select {
			case <-r.stopCh:
				r.mu.Unlock()
				return
			default:
			}
			r.applyCond.Wait()
		}
		r.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      r.log[r.lastApplied].Command,
			CommandIndex: r.lastApplied,
		}
		r.mu.Unlock()

		select {
		case r.applyCh <- msg:
		case <-r.stopCh:
			return
		}
	}
}

func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.leaderID = None
	r.resetElectionDeadlineLocked()
	r.persistLocked() // new term + self-vote must be durable before we ask for votes
	term := r.currentTerm
	lastLogTerm, lastLogIndex := r.lastLogTermIndexLocked()
	peers := append([]NodeID(nil), r.peers...)
	r.mu.Unlock()

	votes := 1 // self
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
			if r.state != Candidate || r.currentTerm != term {
				return
			}
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				r.persistLocked()
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

// becomeLeaderLocked wins the election, initializes per-follower replication
// state, and immediately asserts authority with a heartbeat.
func (r *Raft) becomeLeaderLocked() {
	if r.state != Candidate {
		return
	}
	r.state = Leader
	r.leaderID = r.id
	last := r.lastIndexLocked()
	r.nextIndex = make(map[NodeID]uint64, len(r.peers))
	r.matchIndex = make(map[NodeID]uint64, len(r.peers))
	for _, p := range r.peers {
		r.nextIndex[p] = last + 1
		r.matchIndex[p] = 0
	}
	go r.broadcastAppendEntries()
}

func (r *Raft) broadcastAppendEntries() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	peers := append([]NodeID(nil), r.peers...)
	r.mu.Unlock()
	for _, peer := range peers {
		go r.replicateTo(peer)
	}
}

// replicateTo sends the follower the entries it is missing (or a bare heartbeat),
// then updates match/next index and the commit point based on the reply.
func (r *Raft) replicateTo(peer NodeID) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	ni := r.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	prevIndex := ni - 1
	prevTerm := r.log[prevIndex].Term
	entries := append([]LogEntry(nil), r.log[ni:]...)
	leaderCommit := r.commitIndex
	r.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply, err := r.transport.SendAppendEntries(peer, args)
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader || r.currentTerm != term {
		return
	}
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		return
	}
	if reply.Success {
		r.matchIndex[peer] = prevIndex + uint64(len(entries))
		r.nextIndex[peer] = r.matchIndex[peer] + 1
		r.advanceCommitLocked()
		return
	}
	// Rejected: back up nextIndex toward the conflict and retry on the next tick.
	if reply.ConflictIndex > 0 {
		r.nextIndex[peer] = reply.ConflictIndex
	} else if r.nextIndex[peer] > 1 {
		r.nextIndex[peer]--
	}
}

// advanceCommitLocked moves commitIndex forward to the highest index replicated
// on a majority — but only for an entry from the CURRENT term. Committing an
// older-term entry only indirectly (by committing a current-term entry above it)
// is the Figure-8 safety rule; skipping it can lose committed data.
func (r *Raft) advanceCommitLocked() {
	for n := r.lastIndexLocked(); n > r.commitIndex; n-- {
		if r.log[n].Term != r.currentTerm {
			continue
		}
		count := 1 // self
		for _, p := range r.peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count >= r.majority() {
			r.commitIndex = n
			r.applyCond.Signal()
			return
		}
	}
}

// lastLogTermIndexLocked returns the term and index of the last log entry (used
// by the election restriction so a stale-log candidate cannot win).
func (r *Raft) lastLogTermIndexLocked() (Term, uint64) {
	idx := r.lastIndexLocked()
	return r.log[idx].Term, idx
}
