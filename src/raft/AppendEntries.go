package raft

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of the log entry at prevLogIndex
	Entries      []LogEntry // log entries to store, empty for heartbeat messages
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // current term, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int  // Term of conflicting entry
	ConflictIndex int  // Index of first entry with conflicting term
	ConflictLen   int  // length of the follower's log
}
