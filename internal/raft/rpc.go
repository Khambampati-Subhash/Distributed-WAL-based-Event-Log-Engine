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
// (the "election restriction"); with no log yet they are zero.
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
// Entries list, the heartbeat that keeps followers from starting elections. The
// log fields (PrevLog*, Entries, LeaderCommit) arrive in step 2.
type AppendEntriesArgs struct {
	Term     Term
	LeaderID NodeID
}

type AppendEntriesReply struct {
	Term    Term
	Success bool
}

// RequestVote handles an incoming vote request. A node grants its vote at most
// once per term (that is what guarantees a single leader per term), and only to a
// candidate whose log is at least as up to date as its own.
func (r *Raft) RequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Any message from a newer term makes us a follower of that term first.
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

// AppendEntries handles an incoming heartbeat / replication request. For leader
// election it establishes that a legitimate leader exists for the term: the node
// steps down if needed, records the leader, and resets its election timer.
func (r *Raft) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply := AppendEntriesReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply // stale leader; reject (Success = false)
	}

	// A valid leader for this term (>= ours): adopt its term, become a follower,
	// and reset our election timer so we don't challenge it.
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}
	r.state = Follower
	r.leaderID = args.LeaderID
	r.resetElectionDeadlineLocked()

	reply.Term = r.currentTerm
	reply.Success = true // log consistency checks come in step 2
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
