package raft

// Transport is how a node reaches its peers. Abstracting it means the whole
// cluster can be wired in-memory for tests today and over TCP later, with the
// exact same node logic. Each method is one Raft RPC to a single peer.
type Transport interface {
	SendRequestVote(to NodeID, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// RequestVoteArgs is a candidate asking a peer for its vote (Raft paper §5.2).
// The LastLog fields let a voter refuse a candidate whose log is behind its own
// (the "election restriction").
type RequestVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  Term
}

type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

// AppendEntriesArgs is the leader replicating entries — and, with an empty
// Entries list, the heartbeat that keeps followers from starting elections.
// PrevLog{Index,Term} anchor the entries for the log-matching consistency check.
type AppendEntriesArgs struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply carries ConflictIndex so a leader whose PrevLog check failed
// can back up nextIndex quickly instead of one entry per round-trip.
type AppendEntriesReply struct {
	Term          Term
	Success       bool
	ConflictIndex uint64
}

// RequestVote handles an incoming vote request. A node grants its vote at most
// once per term (that is what guarantees a single leader per term), and only to a
// candidate whose log is at least as up to date as its own.
func (r *Raft) RequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}

	reply := RequestVoteReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply // stale candidate; reject
	}

	alreadyVoted := r.votedFor != None && r.votedFor != args.CandidateID
	if !alreadyVoted && r.candidateUpToDateLocked(args) {
		r.votedFor = args.CandidateID
		r.resetElectionDeadlineLocked() // granting a vote counts as "heard from" the cluster
		reply.VoteGranted = true
	}
	return reply
}

// AppendEntries handles heartbeats and log replication. It runs the log-matching
// consistency check, appends/overwrites entries, and advances the commit index.
func (r *Raft) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply := AppendEntriesReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply // stale leader; reject
	}

	// A valid leader for this term: adopt its term, become a follower, reset timer.
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}
	r.state = Follower
	r.leaderID = args.LeaderID
	r.resetElectionDeadlineLocked()
	reply.Term = r.currentTerm

	// Consistency check: we must already have PrevLogIndex with a matching term,
	// or the leader must back up. ConflictIndex tells it how far.
	if args.PrevLogIndex > r.lastIndexLocked() {
		reply.ConflictIndex = r.lastIndexLocked() + 1 // our log is too short
		return reply
	}
	if r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Skip the whole conflicting term so the leader backs up in one hop.
		conflictTerm := r.log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex
		for ci > 1 && r.log[ci-1].Term == conflictTerm {
			ci--
		}
		reply.ConflictIndex = ci
		return reply
	}

	// The prefix matches. Append new entries, truncating only on a real conflict
	// (a matching duplicate must NOT truncate — it may cover committed entries).
	for i := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= r.lastIndexLocked() && r.log[idx].Term == args.Entries[i].Term {
			continue
		}
		r.log = append(r.log[:idx], args.Entries[i:]...)
		break
	}

	// Advance our commit point, but never past the last entry this RPC delivered.
	if args.LeaderCommit > r.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		r.commitIndex = min(args.LeaderCommit, lastNew)
		r.applyCond.Signal()
	}

	reply.Success = true
	return reply
}

// candidateUpToDateLocked implements the election restriction: a candidate's log
// must be at least as up to date as ours to earn our vote. "More up to date" =
// higher last term, or same last term with a longer log.
func (r *Raft) candidateUpToDateLocked(args RequestVoteArgs) bool {
	myLastTerm, myLastIndex := r.lastLogTermIndexLocked()
	if args.LastLogTerm != myLastTerm {
		return args.LastLogTerm > myLastTerm
	}
	return args.LastLogIndex >= myLastIndex
}
