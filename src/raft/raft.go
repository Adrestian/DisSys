package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const (
	CHECK_COMMIT_INTERVAL int = 500 // Currently not used anywhere

	TICKER_SLEEP_INTERVAL int = 100 // used in {@code ticker()} to sleep rather than spinning

	LEADER_HB_INTERVAL int = 100 // Time interval where leader sends out heartbeat to all followers

	ELECTION_TIMER_LO int = 200 // Candidate's election timeout lower and upper bound
	ELECTION_TIMER_HI int = 500 // on timeout, increment term and start a new election

	HB_TIMER_LOWERBOUND int = 200  // Follower's heartbeat timeout lower and upper bound
	HB_TIMER_UPPERBOUND int = 1000 // on timeout, follower become candidate and start a new election

	SEND_FOLLOWER_SLEEP int = 100
)

// Init to 2, 4, 8 for no particular reason, as long as not 0
// to avoid equal to default initialized value
const (
	Leader    int = 2
	Candidate int = 4
	Follower  int = 8

	NULL int = -1
)

// {@code AppendEntries} RPC handler will push something into this channel to notify there's an hb from the leader
var (
	//If election timeout elapses without receiving AppendEntries RPC from
	// current leader or granting vote to candidate: convert to candidate
	hb chan interface{} = make(chan interface{})

	lastVotedMu sync.Mutex
	lastVoted   time.Time
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// Look at the paper's Figure 2 for a description of what
// state a Raft server must maintain.
// Your data here (2A, 2B, 2C).
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int

	// *Persistent* State on ALL SERVERS
	// !IMPORTANT: Updated on stable storage before responding to RPCs
	currentTerm int        // last term server has seen, init to 0 on boot, increase monotonically
	votedFor    int        // candidateId that received vote in current term, null if none
	log         []LogEntry /* log entries;
	     each entry contains command for state machine,
		 and term when entry was received by leader,
		 first index is 1, on init we add a place holder log entry into the log
		 to deal with this 1-index problem
	*/

	// Volatile State on ALL SERVERS
	commitIndex int // index of highest log entry known to be committed(init to 0, increase monotonically)
	lastApplied int // Index of highest log entry applied to state machine (init to 0, incrase monotonically)

	// Volatile state on leaders
	// (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (init to leader's last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (init to 0, increase monotonically)

	// election related
	majorityVotes int
	currentVotes  int
}

// return currentTerm and whether this server
// believes it is the leader.
// Your code here (2A).
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	//Printf("[Server %v]:GetState(): Term %v, isLeader: %v, voted For: %v\n", rf.me, term, isLeader, rf.votedFor)
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Your code here (2C).
func (rf *Raft) persist() {
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
//// Your code here (2C).
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log = make([]LogEntry, len(data))
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil { // is this going to work? Decode into a slice?
		panic("[Error from: rf.radPersist()]] Decoding Data")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// Lab 2D
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

//Lab 2D
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// Example:
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediatedly preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store, empty for heartbeat, may send more than one for efficiency
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // current term, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int  // Term of conflicting entry
	ConflictIndex int  // index of first entry with Conflicting Term(Xterm)
	ConflictLen   int  // length of the follower's log
}

// Helper function to check if this raft instance should become a follower
// when receiving RPC, return true if this raft instance should become a follower
// false otherwise
// This function DOES acquire the lock (rf.mu)
func (rf *Raft) ConvertToFollowerIfNeeded(argsTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if argsTerm > rf.currentTerm {
		rf.becomeFollower(argsTerm)
		return true
	}
	return false
}

func (rf *Raft) findConflictTerm(xterm int) int {
	var i int
	for i = len(rf.log) - 1; i >= 1; i-- {
		if rf.log[i].Term == xterm {
			break
		}
	}
	if i <= 0 {
		i = 1
	}
	return i
}

// To implement heartbeats, define an AppendEntries RPC struct
// (though you may not need all the arguments yet),
// and have the leader send them out periodically.
// Write an AppendEntries RPC handler method that resets the election timeout
// so that other servers don't step forward as leaders when one has already been elected.
// Note AppendEntries RPC is on follower
// args is filled by the leader
// reply is filled by follower
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// check RPC requst's term
	rf.ConvertToFollowerIfNeeded(args.Term)
	var term = args.Term
	// var leaderId = args.LeaderId
	var prevLogIndex = args.PrevLogIndex
	var prevLogTerm = args.PrevLogTerm
	var entries = args.Entries
	var leaderCommit = args.LeaderCommit

	rf.mu.Lock() // hold the lock from here <------------
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm // send out currentTerm regardless

	// outdated RPC
	if term < rf.currentTerm {
		Printf("Outdated TERM: %v, currentTerm :%v\n", term, rf.currentTerm)
		reply.Success = false
		return
	} // Reply false if term < currentTerm, Section 5.1(outdated information)

	go func() {
		hb <- struct{}{}
	}() // reset the timer

	// 3 cases Lecture 7  @ 27:00
	if prevLogIndex < 0 {
		panic("leader sends a invalid entry")
	}

	// if prevLogIndex >= len(rf.log) { // case 3, follower missing the entry
	// 	Printf("[server %v]Follower missing entry\n", rf.me)
	// 	setReply(reply, false)
	// 	reply.ConflictLen = len(rf.log)
	// 	reply.ConflictTerm = rf.log[len(rf.log)-1].Term
	// 	reply.ConflictIndex = rf.findXIndex(rf.log[len(rf.log)-1].Term)
	// 	Printf("Conflicting Index is %v\n", reply.ConflictIndex)
	// 	// TODO: further modify and give leader information
	// 	// Printf("[server %v]Reply is %+v\n", rf.me, reply)
	// 	// Printf("[server %v] Log is %+v\n", rf.me, rf.log)
	// 	return
	// 	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// } else if prevLogIndex < len(rf.log) && rf.log[prevLogIndex].Term != prevLogTerm {
	// 	// case 1 and 2
	// 	// Printf("Follower log doesn't match")
	// 	setReply(reply, false)
	// 	reply.ConflictLen = len(rf.log)
	// 	reply.ConflictTerm = rf.log[len(rf.log)-1].Term
	// 	reply.ConflictIndex = rf.findXIndex(rf.log[len(rf.log)-1].Term)
	// 	Printf("Conflicting Index is %v\n", reply.ConflictIndex)
	// 	return
	// }

	// Reply false IMMEDIATELY if
	// the log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if !rf.EntryInBound(prevLogIndex) || rf.log[prevLogIndex].Term != prevLogTerm {
		// if !rf.EntryInBound(prevLogIndex) {
		// 	Printf("[Follower %v]: Reject the log because it is out of bound. Index: %v\n", rf.me, prevLogIndex)
		// }
		// if rf.log[prevLogIndex].Term != prevLogTerm {
		// 	Printf("[Follower %v]: Reject the log, the term doesn't match, prevLogTerm: %v, lastTerm: %v", rf.me, prevLogTerm, rf.log[prevLogIndex].Term)
		// }
		reply.Success = false
		return
	}

	/* 3. If an existing entry conflicts with a new one (same index but different terms),
	delete the existing entry and all that follow it (5.3) */
	var currLogIdx = prevLogIndex + 1

	if rf.EntryInBound(currLogIdx) && len(entries) != 0 && rf.log[currLogIdx].Term != entries[0].Term {
		// Have an existing entry at currLogIdx and conflicting(same index but different terms)
		// Truncate the log and append
		rf.log = rf.log[:currLogIdx]
		rf.log = append(rf.log, entries...)
		setReply(reply, true)

		Printf("[server %v]Clip the log and append\n", rf.me)
	} else if currLogIdx == len(rf.log) && rf.log[prevLogIndex].Term == prevLogTerm { /* 4. Append any new entry not already in the log */
		// ideal scenario, just append
		rf.log = append(rf.log, entries...)
		setReply(reply, true)
		Printf("[Server %v] after append: log: %+v\n", rf.me, rf.log)
	} else {
		// TODO: bug
		Printf("[Follower %v], log: %+v\n", rf.me, rf.log)
		Printf("[Follower %v] Cannot append, currLogIdx: %v\n, log length: %v thisLastEntryTerm: %v\n", rf.me, currLogIdx, len(rf.log), rf.log[prevLogIndex].Term)
		Printf("[Follower %v] Cannot append, prevLogIndex: %v, log length: %v\n", rf.me, prevLogIndex, len(rf.log))
		Printf("[Follower %v] receive args %+v\n", rf.me, args)
		setReply(reply, false)
	}

	/* 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) */

	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, len(rf.log)-1)
	}

}

// Check if the index is in bound of the log
func (rf *Raft) EntryInBound(index int) bool {
	if index < 0 || index >= len(rf.log) {
		return false
	}
	return true
}

func setReply(reply *AppendEntriesReply, success bool) {
	if success {
		reply.Success = true
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		reply.ConflictLen = -1
	} else {
		reply.Success = false
	}
}

// Find the minimum amoug all the integers
func min(nums ...int) int {
	if len(nums) == 0 {
		panic("Empty Params")
	}
	var currentMin = nums[0]

	for _, num := range nums {
		if num < currentMin {
			currentMin = num
		}
	}
	return currentMin
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//// Your data here (2A, 2B).
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// Your data here (2A).
type RequestVoteReply struct {
	Term        int  // current term, for candidate to update it self
	VoteGranted bool // true means candidates received vote
}

// This function will hold the rf.mu lock
// example RequestVote RPC handler.
// Your code here (2A, 2B).
// RequestVote is invoked on the follower from candidate!,
// args are filled by candidate
// reply are filled by follower(this handler)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.ConvertToFollowerIfNeeded(args.Term)

	candidateTerm := args.Term
	candidateId := args.CandidateId
	candidateLastEntryIndex := args.LastLogIndex
	candidateLastEntryTerm := args.LastLogTerm

	rf.mu.Lock() // <--------------------------lock the raft struct from here
	defer rf.mu.Unlock()

	var lastLogEntryIndex = len(rf.log) - 1
	var lastLogEntryTerm = rf.log[lastLogEntryIndex].Term

	currentTerm := rf.currentTerm
	reply.Term = currentTerm // set Term in reply to currentTerm regardless
	/* Receiver Implementation:
	*  reply false if candidate's term < currentTerm
	*  if votedFor is null or candidateId,
	*  and candidate's log is at least up-to-date as receiver's log,
	*  grant vote
	 */
	if candidateTerm < currentTerm { // candidate is out of date!
		reply.VoteGranted = false
		return
	}
	/* If the logs have last entries with different terms, then
	   the log with the later term is more up-to-date. If the logs
	   end with the same term, then whichever log is longer is
	   more up-to-date. Vote yes if candidate's log is at least as up-to-date as this raft instance
	*/
	var candidateLogOk = isCandidateLogOk(lastLogEntryIndex, lastLogEntryTerm, candidateLastEntryIndex, candidateLastEntryTerm)
	if candidateLogOk && (rf.votedFor == NULL || rf.votedFor == candidateId) {
		// Grant Vote
		if rf.state == Follower {
			reply.VoteGranted = true
			rf.votedFor = candidateId
			lastVotedMu.Lock()
			defer lastVotedMu.Unlock()
			lastVoted = time.Now()
		}
		return
	}

	// Otherwise, vote no
	reply.VoteGranted = false
	return
}

// Check if the candidate log is ok
// Return true if CANDIDATE is at least as up to date as this receiver's log, this is only ONE of the conditions to vote yes
// Return false if candidate has out of date log
func isCandidateLogOk(thisLastEntryIndex, thisLastEntryTerm, candidateLastEntryIndex, candidateLastEntryTerm int) bool {
	/* From Paper: Raft determines which of two logs is more up-to-date
	by comparing
	the index and term of the last entries in the logs.
	If the logs have last entries with different terms, then
	the log with the later term is more up-to-date. If the logs
	end with the same term, then whichever log is longer is
	more up-to-date.
	*/
	if candidateLastEntryTerm > thisLastEntryTerm ||
		(thisLastEntryTerm == candidateLastEntryTerm && candidateLastEntryIndex >= thisLastEntryIndex) {
		return true
	}
	return false
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// NOTE: maybe we should let this function capture what to sen, not building args inside this function
// should think about the interface, what to return
// SendRequestVote is a wrapper function over sendRequestVote, return the pointer to the reply and an boolean(ok in sendRequestVote)
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	if server == rf.me {
		panic("Send request vote to itself, wtf?")
	}

	reply := &RequestVoteReply{}

	ok := rf.sendRequestVote(server, args, reply)
	return reply, ok
}

// Construct a new RequestVoteArgs struct and return the pointer to the struct
// This function does NOT acquire the lock inside the raft instance i.e {@code rf.mu}
func (rf *Raft) NewRequestVoteArgs() *RequestVoteArgs {
	var lastLogIndex = len(rf.log) - 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}
	return args
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// Your code here (2B).
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()

	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == Leader
	if isLeader {
		// make a new Entry
		var newEntry = LogEntry{Term: term, Command: command}
		// append entry to local log
		rf.log = append(rf.log, newEntry)

		// send folloer all the log
	}
	rf.mu.Unlock()

	// TODO: and start agreement here
	return
}

//TODO: Very buggy!
// If successful: update nextIndex and matchIndex for
//follower (§5.3)
// If AppendEntries fails because of log inconsistency:
//decrement nextIndex and retry (§5.3)
// This Go routing repeatedly check and send follower log if necessary
func (rf *Raft) SendFollowerLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		var state = rf.state
		rf.mu.Unlock()
		if state != Leader {
			time.Sleep(time.Duration(SEND_FOLLOWER_SLEEP) * time.Millisecond) // 100 ms
			continue
		}

		for server := range rf.peers {
			if server != rf.me {
				// make a log and send follower the log
				//If last log index ≥ nextIndex for a follower:
				rf.mu.Lock() // buggy
				var lastLogIndex = len(rf.log) - 1
				var followerNextIndex = rf.nextIndex[server]
				if lastLogIndex >= followerNextIndex {
					// send follower the log!
					var args = rf.NewAppendEntriesArgs(server, false)
					// BUGGY! TODO:
					Printf("[at leader %v], send to follower %v, with args %+v\n", rf.me, server, args)
					Printf("[at leader %v], leader log %+v\n", rf.me, rf.log)
					Printf("[at leader %v], rf.nextIndex[server]: %v\n", rf.me, rf.nextIndex[server])
					//Printf2B("Sending out args: %+v at %v\n", args, rf.me)
					//send AppendEntries RPC with log entries starting at nextIndex

					go func(server int, args *AppendEntriesArgs) {
						reply, ok := rf.SendAppendEntries(server, args)
						rf.ConvertToFollowerIfNeeded(reply.Term)
						// no dead lock here
						//Printf("[Server %v] Check reply after append entries call, no deadlock \n", rf.me)

						if ok && reply.Success {
							rf.mu.Lock()
							var entrylen = len(args.Entries)
							rf.nextIndex[server] = rf.nextIndex[server] + entrylen
							rf.matchIndex[server] = rf.nextIndex[server] + entrylen - 1
							Printf("[Leader %v]nextIndex[%v] is updated to %v\n", rf.me, server, rf.nextIndex[server])
							rf.mu.Unlock()
							return

						} else if ok && !reply.Success { // TODO: handle RPC failures! BUGGY !
							Printf("[server %v] Follower %v Reject the log\n", rf.me, server)
							rf.mu.Lock()
							rf.nextIndex[server] = max(rf.nextIndex[server]-1, 1) // for now
							//rf.nextIndex[server] = rf.nextIndex[server] - 1       // for now // big BUG!
							rf.mu.Unlock()
						} else {
							Printf("[Server %v]RPC from leader %v to %v failed\n", rf.me, rf.me, server)
						}
					}(server, args)

				}
				rf.mu.Unlock() // buggy
			}
		}
	}
}

func max(nums ...int) int {
	if len(nums) == 0 {
		panic("slice is zero length in max()")
	}
	var current = nums[0]
	for _, num := range nums {
		if num > current {
			current = num
		}
	}
	return current
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		var state = rf.state
		rf.mu.Unlock()
		if state == Follower {
			// On every iteration, pick a new random timeout
			var randomAmount = GetRandomTimeout(HB_TIMER_LOWERBOUND, HB_TIMER_UPPERBOUND, time.Millisecond)
			// check if a leader election should
			// be started and to randomize sleeping time using time.Sleep().
			// make a electionTimeoutCh on each iteration
			var electionTimeoutCh, _ = makeTimeoutChan(randomAmount)
			var startTime = time.Now()

			select {
			case <-hb:
				continue
			case <-electionTimeoutCh:
				// crucial, so in the next iteration of {@code ticker()}
				lastVotedMu.Lock()
				var lastVotedTime = lastVoted
				lastVotedMu.Unlock()
				if inTimeSpan(startTime, time.Now(), lastVotedTime) {
					Printf("[server %v] Voted during last election timeout\n", rf.me)
					continue
				}

				rf.mu.Lock()
				rf.state = Candidate
				rf.mu.Unlock()
				go rf.startElection()

			}
		} else if state == Candidate { // state == Candidate, do nothing for a while
			time.Sleep(time.Duration(TICKER_SLEEP_INTERVAL) * time.Millisecond)

		} else if state == Leader { // state == Leader, do nothing for a while
			time.Sleep(time.Duration(TICKER_SLEEP_INTERVAL) * time.Millisecond)

		} else { // unknown raft state
			time.Sleep(time.Duration(TICKER_SLEEP_INTERVAL) * time.Millisecond)
			Printf("[server %v] in unknown state\n", rf.me)
		}
	}
}

// stolen from
// https://stackoverflow.com/questions/55093676/checking-if-current-time-is-in-a-given-interval-golang/55093788
func inTimeSpan(start, end, check time.Time) bool {
	if start.Before(end) {
		return !check.Before(start) && !check.After(end)
	}
	if start.Equal(end) {
		return check.Equal(start)
	}
	panic("[error]: inTimeSpan() start after end!")
}

func makeTimeoutChan(timeout time.Duration) (chan interface{}, chan interface{}) {
	var ch = make(chan interface{})
	var cancel = make(chan interface{})
	// Fork off a goroutine, sleep for {@code timeout} and try to pull from cancel
	// if cancelled, go routine returns, otherwise, goroutine pushes pushes to the timeout channel
	go func() {
		time.Sleep(timeout)
		select {
		case <-cancel:
			return
		default:
			ch <- struct{}{}
		}
	}()
	return ch, cancel
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// No need to init rf.dead, default init to 0
	rf.state = Follower // On boot state is follower

	// To persistent storage
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]LogEntry, 0, 10)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: "Place Holder"}) // Maybe?, first term is 1

	// Volatile State on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile State on leaders
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	} // init to leader last log index + 1
	rf.matchIndex = make([]int, len(peers)) // init to 0

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	var now = time.Now()
	lastVotedMu.Lock()
	lastVoted = now.Add(time.Duration(-HB_TIMER_UPPERBOUND) * time.Millisecond)
	lastVotedMu.Unlock()
	// start ticker goroutine to start elections
	go rf.ticker()
	// go rf.heartbeatRoutine(time.Duration(LEADER_HB_INTERVAL) * time.Millisecond)
	go rf.SendFollowerLog()

	return rf
}

// func (rf *Raft) monitorAndCommit(timeout time.Duration) {
// 	time.Sleep(timeout)
// 	rf.mu.Lock()

// 	for rf.commitIndex > rf.lastApplied {
// 		rf.lastApplied++
// 		// apply log[lastApplied] to state machine
// 	}

// 	rf.mu.Unlock()
// }

// return true if this raft instance is elected, false otherwise
func (rf *Raft) startElection() bool {

	for rf.killed() == false {
		rf.mu.Lock()
		rf.becomeCandidate()
		var state = rf.state
		rf.mu.Unlock()

		if state != Candidate {
			return false
		}
		var timeout = GetRandomTimeout(ELECTION_TIMER_LO, ELECTION_TIMER_HI, time.Millisecond)
		Printf("[Info]: %v starts election with election timeout %v\n", rf.me, timeout)

		leaderElectionTimeout, _ := makeTimeoutChan(timeout)
		leaderElectionSuccessCh := make(chan interface{})

		for i := range rf.peers {
			if i != rf.me {
				rf.mu.Lock()
				var requestVoteArgs = rf.NewRequestVoteArgs()
				rf.mu.Unlock()

				go func(server int, args *RequestVoteArgs) {
					reply, ok := rf.SendRequestVote(server, args)

					if convertToFollower := rf.ConvertToFollowerIfNeeded(reply.Term); convertToFollower {
						return
					}
					rf.mu.Lock() // <---- lock here!
					if ok && reply.VoteGranted {
						rf.currentVotes++
					} else if ok { // vote not granted
						//Printf("[server %v] Peer %v voted no\n", rf.me, server)
					} else if !ok {
						//Printf("[Server %v] RPC failed\n", rf.me)
						// RPC Failed
					}

					if rf.currentVotes == rf.majorityVotes {
						rf.becomeLeader()
						rf.mu.Unlock()                   // <----------one way to exit
						rf.SendHeartbeatToAllFollowers() //immediatedly sends out one round of broadcast heartbeat
						leaderElectionSuccessCh <- struct{}{}
						return
					}
					rf.mu.Unlock() // <---- another way to exit, unlock here
				}(i, requestVoteArgs) // send RPC in parallel
			}
		}

		select {
		case <-leaderElectionTimeout:
			Printf("[server %v] Election Timeout!, make a new one!\n", rf.me)
			continue
		case <-leaderElectionSuccessCh:
			return true
		case <-hb: // new appendEntries arrived!
			// row back currentTerm?
			rf.mu.Lock()
			rf.state = Follower
			rf.mu.Unlock()
			return false
		}
	}
	return false
}

func GetRandomTimeout(lo, hi int, unit time.Duration) time.Duration {
	if hi-lo < 0 || lo < 0 || hi < 0 {
		panic("[WARNING]: getRandomTimeout: Param Error")
	}
	return time.Duration(lo+rand.Intn(hi-lo)) * unit
}

// Periodically send heartbeatRoutine to every node if this node is leader
func (rf *Raft) heartbeatRoutine(heartbeatTimeInterval time.Duration) {
	for rf.killed() == false {
		rf.mu.Lock()
		var state = rf.state
		rf.mu.Unlock()

		if state == Leader {
			rf.SendHeartbeatToAllFollowers()
		}

		time.Sleep(heartbeatTimeInterval)
	}
}

// Returns a (pointer to) prepared AppendEntriesArgs struct
// this function does not hold lock while doing so
func (rf *Raft) NewAppendEntriesArgs(server int, useEmptyEntry bool) *AppendEntriesArgs {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		Println(r)
	// 		Printf2B("PrevLogIndex: %v, rf.log: %+v \n", rf.nextIndex[server]-1, rf.log)
	// 	}
	// }()
	//Printf2B("PrevLogIndex: %v, log: %+v, rf.nextIndex: %+v\n", rf.nextIndex[server]-1, rf.log, rf.nextIndex)
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

// Send AppendEntries RPC call with empty log entries, will require locks
func (rf *Raft) SendHeartBeat(server int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, args, reply)
	return reply, ok
}

// This function is called by the leader periodically
func (rf *Raft) SendHeartbeatToAllFollowers() {
	rf.mu.Lock()
	var state = rf.state
	rf.mu.Unlock()

	if state != Leader {
		return
	}
	for i := range rf.peers {
		if i != rf.me {

			rf.mu.Lock()
			var appendEntriesArgs = rf.NewAppendEntriesArgs(i, true)
			rf.mu.Unlock()

			go func(server int, args *AppendEntriesArgs) { // maybe some issue?
				reply, ok := rf.SendHeartBeat(server, args)
				rf.ConvertToFollowerIfNeeded(reply.Term)

				if ok && reply.Success { // RPC success and follower reply yes

				} else if ok {

				} else { // RPC failed
				}
			}(i, appendEntriesArgs)
		}
	}
}

// This function does not require the lock!
// change the state to Follower,
// set the currentTerm to newTerm and reset votedFor
func (rf *Raft) becomeFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = NULL
	rf.majorityVotes = 9999999
	rf.currentVotes = -9999999
	rf.state = Follower
}

// Does not acquire the lock inside the raft struct
// set the state to candidate, increment term, vote for itself
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.majorityVotes = (len(rf.peers) / 2) + 1
	rf.currentVotes = 1 // vote for itself
}

// Set the current raft instance to leader, init everything necessary
// This method does NOT hold the lock i.e.{@code rf.mu}
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.leaderInit()
}

// IMPORTANT: Reinitialized after election
// nextIndex[]: For each server, index of the next log entry to send to that server
// init to leader last log index + 1
// matchIndex[]: for each server, index of highest log entry known to be replicated on server
// init to 0, increase monotonically
// This method does NOT hold the lock
func (rf *Raft) leaderInit() {
	var leaderLastLogIndex = len(rf.log) - 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = leaderLastLogIndex + 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
}

// IMPORTANT: Updated on stable storage before respondng to RPCs.
// This method does NOT hold the lock inside raft struct, synchonization needed
// Flush the persistent state {@code rf.currentTerm, rf.votedFor, rf.log[]}
// on this raft instance to stable storage
func (rf *Raft) fsync() {
	// TODO:
}
