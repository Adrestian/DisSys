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

// TODO: Unfinished!
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check RPC request's term, if higher, convert to follower
	rf.ConvertToFollowerIfNeeded(args.Term)

	// set the term in reply regardless
	// reply.Term = rf.currentTerm

	// ignore outdated RPC
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// otherwise, it's a valid RPC, reset the timer
	// This will actually cancel the election if this raft instance is in follower state
	resetFollowerTimer()
	rf.state = Follower

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// or prevLogIndex points beyond the end of the log
	if args.PrevLogIndex >= len(rf.log) {
		reply.Term = rf.currentTerm
		reply.Success = false
		// Case 3: follower doesn't have the log
		// Follower : [4]
		// Leader   : [4 6 6 6]
		reply.ConflictTerm = -1
		reply.ConflictIndex = -1
		reply.ConflictLen = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Handle case 1 and 2
		// Case 1: leader doesn't have follower's term
		// F: [4 5 5]
		// L: [4 6 6 6]
		//
		// Case 2: leader does have follower's term
		// F: [4 4 4]
		// L: [4 6 6 6]
		reply.Term = rf.currentTerm
		reply.Success = false
		var logIndex = args.PrevLogIndex
		var entryTerm = rf.log[logIndex].Term
		var conflictingIndex = rf.getConflictingIndex(logIndex, entryTerm)
		reply.ConflictLen = len(rf.log)
		reply.ConflictTerm = entryTerm
		reply.ConflictIndex = conflictingIndex
		return
	}

	// 3 Cases
	// log      [1 2 3 4 5 6]
	// entries              [7, 8]
	var thisLastLogIndex = len(rf.log) - 1
	if args.PrevLogIndex == thisLastLogIndex {
		Printf("[Server %v] Just append the log %+v\n", rf.me, args.Entries)
		// just append
		rf.log = append(rf.log, args.Entries...)
		rf.persist()

		reply.Term = rf.currentTerm
		reply.Success = true // set reply.Success == True if folloer contained entry matching prevLogIndex and prevLogTerm
	} else if rf.EntryInBound(args.PrevLogIndex) && args.PrevLogIndex+len(args.Entries) < len(rf.log) {
		// check if match, otherwise clip the log,
		// prevLogIndex == 3 in this case
		// log      [1 2 3 4 5 6]
		// entries        [4 5 6]
		var argsPrevLogIndex = args.PrevLogIndex
		var curr = argsPrevLogIndex + 1
		var i = 0
		for i < len(args.Entries) {
			var logIdx = curr + i
			if rf.log[logIdx].Term != args.Entries[i].Term {
				rf.log = rf.log[:logIdx] // clip the log
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			i++
		}
		rf.persist()

		reply.Term = rf.currentTerm
		reply.Success = true // success
	} else if rf.EntryInBound(args.PrevLogIndex) && args.PrevLogIndex+len(args.Entries) >= len(rf.log) {
		// log      [1 2 3 4 5 6]
		// entries          [5 6 7 8]
		//                   c
		var argsPrevLogIndex = args.PrevLogIndex
		var curr = argsPrevLogIndex + 1

		// Check inconsistency (discard the log if necessary)
		var logConsistent = true
		var i = 0
		for i = 0; curr+i < len(rf.log); i++ {
			var logIdx = curr + i
			if rf.log[logIdx].Term != args.Entries[i].Term {
				logConsistent = false
				// Clip the log, append the remainder, done
				rf.log = rf.log[:logIdx]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
		// append the remainder logs if no parts of the log is discarded
		if logConsistent {
			rf.log = append(rf.log, args.Entries[i:]...)
		}
		rf.persist()
		// TODO: Figure 2 AppendEntries RPC Receiver Implememtation $3, $4
		// Starting from prevLogIndex
		reply.Term = rf.currentTerm
		reply.Success = true // success
	} else {
		reply.Success = false
		Printf("[Server %v] AppendEntries RPC handler: Should never happen: args: %+v\n", rf.me, *args)
	}
	// should all success at this point unless the args.PrevLogIndex is not in bound, <0 (Unchecked and unhandled)
	// Printf("[Server %v] AppendEntries RPC Handler returns %v\n", rf.me, reply.Success)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.cond.Broadcast()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Request AppendEntries RPC call,
// this function does not acquire the lock inside raft instance
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	return reply, ok
}

// Returns a (pointer to) prepared AppendEntriesArgs struct
// this function does not hold lock while doing so
func (rf *Raft) NewAppendEntriesArgs(server int, useEmptyEntry bool) *AppendEntriesArgs {
	var prevLogIndex = rf.nextIndex[server] - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      rf.log[rf.nextIndex[server]:],
		LeaderCommit: rf.commitIndex,
	}
	if useEmptyEntry { // if useEmptyEntry flag is set to true, attach empty log to it
		args.Entries = make([]LogEntry, 0)
		if len(args.Entries) != 0 {
			panic("not empty entry")
		}
	}
	return args
}
