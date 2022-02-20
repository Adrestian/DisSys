package raft

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

import (
	"bytes"
	"log"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Leader    int = 32
	Candidate int = 64
	Follower  int = 128
	// Seems like init to 0 is a bad idea
	NULL int = -1

	// Lower and Upper bound for the timeout where the follower becomes candidate
	FOLLOWER_TIMEOUT_LOWER int = 300
	// if no appendentries RPC has been received from leader or voted for other candidates
	FOLLOWER_TIMEOUT_UPPER int = 1200

	SEND_LOG_INTERVAL int = 100 // highest rate capped at 10/sec
	TICKER_INTERVAL   int = 10

	ELECTION_TIMEOUT_LOWER     int = 300
	ELECTION_TIMEOUT_UPPER     int = 1000
	ELECTION_CHECKING_INTERVAL int = 10

	APPLY_LOG_INTERVAL int = 5

	NO_OP_CMD = "__NO_OP"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	LastReceivedMu  sync.Mutex
	LastReceived    time.Time     // AppendEntries/RequestVote RPC may reset the timer
	FollowerTimeout time.Duration // on timeout become candidate
	// always between FOLLOWER_HB_TIMEOUT_LOWER and FOLLOWER_HB_TIMEOUT_UPPER

	ElectionStartedMu sync.Mutex
	ElectionStarted   time.Time
	// on timeout, increment currentTerm, transition to candidate
	ElectionTimeout time.Duration

	applyLogEntryCh chan ApplyMsg
	applySnapshotCh chan ApplyMsg

	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int

	// *Persistent* State on ALL SERVERS
	// UPDATE on stable storage BEFORE responding to RPCs!
	currentTerm int        // last term this server has seen, initialize to 0 on boot, increase monotonically
	votedFor    int        // candidateId that received vote in currentTerm, NULL if not voted
	log         []LogEntry /* Log Entries, each entry contains command for state machine,
	 * and term when entry was received by leader, first index is 1, on init we add a place holder log entry
	 * into the log to deal with this starting with index 1 problem
	 */

	// *Volatile State* on ALL SERVERS
	commitIndex int // index of highest log entry known to be committed(init to 0, increase monotonically)
	lastApplied int // Index of highest log entry applied to state machine (init to 0, incrase monotonically)

	// *Volatile State* on LEADER, reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (init to leader's last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (init to 0, increase monotonically)

	// election Related, simple book-keeping, violatile
	majorityVotes int
	currentVotes  int

	// Persist on stable storage, related to snapshot(2D)
	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

// Generate a new No-op log entry from current term
// Synchronization needed, Caller require {@code rf.mu}
func (rf *Raft) NewNoOpLogEntry() *LogEntry {
	var entry = LogEntry{
		Term:    rf.lastIncludedTerm,
		Command: NO_OP_CMD,
	}
	return &entry
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	return
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Synchronization needed, Caller require {@code rf.mu}
// Code Example:
// w := new(bytes.Buffer)
// e := labgob.NewEncoder(w)
// e.Encode(rf.xxx)
// e.Encode(rf.yyy)
// data := w.Bytes()
// rf.persister.SaveRaftState(data)
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(len(rf.log)) // encode the log length
	e.Encode(rf.log)
	data := w.Bytes()
	//rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

// restore previously persisted state.
// Return true if raft state is restored from {@param data}
// Return false if the state is untouched
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
func (rf *Raft) readPersist(data []byte) bool {
	if len(data) == 0 { // nothing to read
		//Printf("[Server %v] Nothing to read\n", rf.me)
		return false
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex, lastIncludedTerm, currentTerm, votedFor, logLength int

	if err := d.Decode(&lastIncludedIndex); err != nil {
		log.Fatalf("[Error] Server %v Cannot Decode lastIncludedIndex\n", rf.me)
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		log.Fatalf("[Error] Server %v Cannot Decode lastIncludedTerm\n", rf.me)
	}
	if err := d.Decode(&currentTerm); err != nil {
		log.Fatalf("[Error] Server %v Cannot Decode Current Term\n", rf.me)
	}
	if err := d.Decode(&votedFor); err != nil {
		log.Fatalf("[Error] Server %v Cannot Decode votedFor\n", rf.me)
	}
	if err := d.Decode(&logLength); err != nil {
		log.Fatalf("[Error] Server %v Cannot Decode log length\n", rf.me)
	}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor

	var logEntry = make([]LogEntry, logLength)
	if err := d.Decode(&logEntry); err != nil {
		log.Fatalf("[Error] Server %v Cannot Decode log\n", rf.me)
	}
	rf.log = logEntry
	//rf.leaderInit()
	//Printf("[Server %v] Read From Disk: Log %+v\n", rf.me, rf.log)
	return true
}

// Deprecated
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(logicalLastIncludedIndex int, snapshot []byte) {
	Printf("[Server %v] Snapshot() \n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Leader cannot snapshot uncommitted log entry
	if logicalLastIncludedIndex > rf.commitIndex {
		log.Fatalf("[Server %v] Cannot Snapshot uncommitted entry\n", rf.me)
	}

	// if !rf.EntryInBound(logicalIndex) {
	// 	log.Fatalf("[Server %v] Snapshot(): Invalid index\n", rf.me)
	// }
	rf.Compact(logicalLastIncludedIndex, snapshot) // compact log
	// flush to stable storage
	rf.persist()
	runtime.GC()
}

// Compact the log according to snapshot
// {@param lastIndex are logical index!}
func (rf *Raft) Compact(logicalLastIncludedIndex int, snapshot []byte) {
	// Optimize later ##############################################################

	// Convert Logicial to Physical
	var physicalLastIncludedIndex = rf.logicalToPhysicalIndex(logicalLastIncludedIndex)
	if !rf.EntryInBound(physicalLastIncludedIndex) {
		log.Fatalf("[Server %v]: Compact() Out of bound\n", rf.me)
	}
	var lastInclTerm = rf.log[physicalLastIncludedIndex].Term
	rf.lastIncludedIndex = logicalLastIncludedIndex
	rf.lastIncludedTerm = lastInclTerm

	// Trim the log
	var tmp = rf.log[physicalLastIncludedIndex+1:] // slice the log
	var dummy = []LogEntry{*rf.NewNoOpLogEntry()}
	rf.log = append(dummy, tmp...)
	var cpy = make([]LogEntry, len(rf.log)) // create a new slice

	if n := copy(cpy, rf.log); n != len(rf.log) {
		log.Fatalf("[Server %v] Compact(): Copy log error \n", rf.me)
	}
	if len(rf.log) != len(tmp)+1 {
		log.Fatalf("[Server %v] Compact() \n", rf.me)
	}
	// Update internal data structures
	// rf.log now points to the new compacted log
	rf.log = cpy
	rf.snapshot = snapshot
	//################################################################################
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ConvertToFollowerIfHigherTerm(args.Term) // check the term in request args

	// Candidate term is too low, vote no immediately
	var candidateTerm = args.Term
	if candidateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	var candidateId = args.CandidateId
	var candidateLastEntryIndex = args.LastLogIndex
	var candidateLastEntryTerm = args.LastLogTerm

	var lastEntryIndex, lastEntryTerm = rf.getLogicalLastLogEntryIndexTerm()

	var candidateLogOK = isCandidateLogOk(lastEntryIndex, lastEntryTerm, candidateLastEntryIndex, candidateLastEntryTerm)

	if candidateLogOK && (rf.votedFor == NULL || rf.votedFor == candidateId) { // grant vote
		rf.state = Follower // make sure
		rf.currentTerm = candidateTerm
		rf.votedFor = candidateId
		rf.persist()
		//Printf("[Server %v] Voted YES For %v\n", rf.me, rf.votedFor)
		rf.resetFollowerTimer() // reset the follower timer
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}
	// otherwise vote no
	//Printf("[Server %v] Voted NO For %v\n", rf.me, args.CandidateId)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	// Don't reset Follower Timer
}

// @params: ALL params {@code thisLastEntryIndex, candidateLastEntryIndex } are logical!
// Check if the candidate log is ok, should compare *Logical Log Len* NOT *Physical Log Len*
// Return true if CANDIDATE is at least as up to date as this receiver's log, this is only ONE of the conditions to vote yes
// Return false if candidate has out of date log
func isCandidateLogOk(thisLastEntryIndex, thisLastEntryTerm, candidateLastEntryIndex, candidateLastEntryTerm int) bool {
	/* Raft determines which of two logs is more up-to-date, by comparing
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

// Reset the follower timer
func (rf *Raft) resetFollowerTimer() {
	rf.LastReceivedMu.Lock()
	defer rf.LastReceivedMu.Unlock()
	rf.LastReceived = time.Now()
	rf.FollowerTimeout = GetRandomTimeout(FOLLOWER_TIMEOUT_LOWER, FOLLOWER_TIMEOUT_UPPER, time.Millisecond)
	//Printf("[Server %v]: Reset Follower Time out: %v\n", rf.me, rf.FollowerTimeout)
}

// Accessor function for LastReceived and ddl for timeout
func (rf *Raft) getLastReceived() (time.Time, time.Time) {
	rf.LastReceivedMu.Lock()
	defer rf.LastReceivedMu.Unlock()
	return rf.LastReceived, rf.LastReceived.Add(rf.FollowerTimeout)
}

// Reset the election timer
func (rf *Raft) resetElectionTimer() {
	rf.ElectionStartedMu.Lock()
	defer rf.ElectionStartedMu.Unlock()
	rf.ElectionTimeout = GetRandomTimeout(ELECTION_TIMEOUT_LOWER, ELECTION_TIMEOUT_UPPER, time.Millisecond)
	rf.ElectionStarted = time.Now()
	//Printf("[Server %v]: Reset Election Timer \n", rf.me)
}

// Return when the election started, and when the election expires
func (rf *Raft) getElectionTimer() (time.Time, time.Time) {
	rf.ElectionStartedMu.Lock()
	defer rf.ElectionStartedMu.Unlock()
	var ddl = rf.ElectionStarted.Add(rf.ElectionTimeout)
	return rf.ElectionStarted, ddl
}

// Check if the index is in bound of the log
// @Param index: Physical Index
func (rf *Raft) EntryInBound(physicalIndex int) bool {
	if physicalIndex < 0 || physicalIndex >= len(rf.log) {
		return false
	}
	return true
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
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == Leader

	if isLeader {
		var newEntry = LogEntry{Term: rf.currentTerm, Command: command}
		rf.log = append(rf.log, newEntry)
		Printf("[Leader %v] Start agree with %+v\n", rf.me, newEntry)
		rf.persist()
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
func (rf *Raft) ticker(interval time.Duration) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Follower {
			var lastReceived, deadline = rf.getLastReceived()
			var now = time.Now()
			if !inTimeSpan(lastReceived, deadline, now) {
				// valid AppendEntries/RequestVote RPC should re-set the LastReceived
				// and recompute random timeout by calling GetRandomTimeout()
				rf.state = Candidate // set the state to candidate so ticker would do nothing
				Printf("[Server %v] Timed out, become Candidate \n", rf.me)
				go rf.startElection(time.Duration(ELECTION_CHECKING_INTERVAL) * time.Millisecond)
			}
		}
		rf.mu.Unlock()
		time.Sleep(interval)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.majorityVotes = (len(rf.peers) / 2) + 1
	rf.currentVotes = 1 // vote for itself
	rf.persist()        // save state to disk
	rf.resetElectionTimer()
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
func (rf *Raft) leaderInit() {
	// matchIndex[] and nextIndex[] are ALL logical index
	var leaderLastLogIndex, _ = rf.getLogicalLastLogEntryIndexTerm()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = leaderLastLogIndex + 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
}

// This function does not require the lock!
// change the state to Follower,
// set the currentTerm to newTerm and reset votedFor
func (rf *Raft) becomeFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = NULL
	rf.majorityVotes = math.MaxInt
	rf.currentVotes = -999999
	rf.state = Follower
	rf.persist() // write to disk
}

// Check if newTerm is higher the current term
// If so, convert to follower and return true
// otherwise do nothing and return false
func (rf *Raft) ConvertToFollowerIfHigherTerm(newTerm int) bool {
	if newTerm > rf.currentTerm {
		rf.becomeFollower(newTerm)
		return true
	}
	return false
}

func (rf *Raft) HaveLogEntry(server int) bool {
	var logicalIndex = rf.nextIndex[server]
	return logicalIndex > rf.lastIncludedIndex
}

func (rf *Raft) SendLog(interval time.Duration) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			time.Sleep(interval)
			continue
		}
		// Send Follower log
		for server := range rf.peers {
			if server == rf.me {
				continue
			} // Don't send to itself

			// TODO: FIX THIS Later
			if rf.HaveLogEntry(server) { // use AE RPC //BUG!!
				Printf("[Leader %v] Send AE to %v\n", rf.me, server)
				var args = rf.NewAppendEntriesArgs(server, false)
				// Printf("[Leader %v] term: %v send out log to follower %v\n", rf.me, rf.currentTerm, server)
				go func(server int, args *AppendEntriesArgs) {
					var sentInTerm = args.Term
					var reply, ok = rf.SendAppendEntries(server, args)

					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state != Leader || sentInTerm != reply.Term ||
						rf.currentTerm != args.Term || rf.ConvertToFollowerIfHigherTerm(reply.Term) {
						return
					}

					rf.AppendEntriesReplyHandler(server, args, reply)

				}(server, args)
			} else {
				Printf("[Leader %v] Send Installsnapshot to %v\n", rf.me, server)
				// Follower lagging behind
				// Send Install snapshot RPC
				var args = rf.NewInstallSnapshotArgs(server)

				go func(server int, args *InstallSnapshotArgs) {
					var sentInTerm = args.Term
					reply, ok := rf.SendInstallSnapshot(server, args)

					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state != Leader || sentInTerm != reply.Term ||
						rf.currentTerm != sentInTerm || rf.ConvertToFollowerIfHigherTerm(reply.Term) {
						return
					}

					rf.InstallSnapshotReplyHandler(server, args, reply)

				}(server, args)

			}
		}
		rf.mu.Unlock()
		time.Sleep(interval)
	}
}

func (rf *Raft) NewInstallSnapshotArgs(server int) *InstallSnapshotArgs {
	var args = InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	if len(args.Data) == 0 {
		panic("Empty snapshot wtf?")
	}
	return &args
}

func (rf *Raft) InstallSnapshotReplyHandler(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if !reply.Success {
		log.Printf("[Leader %v] Follower %v reject snapshot\n", rf.me, server)
		return
	}
	// otherwise update
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex

}

// Should set raft state to Candidate before calling startElection()
func (rf *Raft) startElection(checkInterval time.Duration) bool {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			return false
		}
		rf.becomeCandidate() // increment the term, reset the timer

		// send RequestVotes RPC to every peer
		for i := range rf.peers {
			if i == rf.me {
				continue // don't send to itself
			}

			var requestVoteArgs = rf.NewRequestVoteArgs()

			go func(server int, args *RequestVoteArgs) {
				reply, ok := rf.SendRequestVote(server, args)
				var sentInTerm = args.Term

				rf.mu.Lock() // lock acquired here
				defer rf.mu.Unlock()
				// Check for current Term, avoid outdated RPC reply
				if !ok || rf.ConvertToFollowerIfHigherTerm(reply.Term) ||
					sentInTerm != reply.Term || args.Term != rf.currentTerm ||
					rf.state != Candidate {
					// !ok -> RPC failed
					// sentInTerm != reply.Term || args.Term != rf.currentTerm -> OUTDATED RPC
					return
				}

				if reply.VoteGranted {
					rf.currentVotes++
				}

				if rf.currentVotes == rf.majorityVotes {
					Printf("[Server %v] Received majority votes, become leader, term: %v\n", rf.me, rf.currentTerm)
					rf.becomeLeader()
					rf.heartBeat() // Immediately send one round of heartbeat to followers
				}
			}(i, requestVoteArgs)
		}

		rf.mu.Unlock()
		// ###### This section of code is kind of shit ############
		// wait until Election Timeout
		var electionStarted, electionDeadline = rf.getElectionTimer()
		var now = time.Now()
		for inTimeSpan(electionStarted, electionDeadline, now) {
			time.Sleep(checkInterval)
			rf.mu.Lock()
			if rf.state == Leader {
				rf.mu.Unlock() // exit 1
				return true
			}
			if rf.state == Follower { // Candidate receive valid RPC AppendEntries
				rf.mu.Unlock() // exit 2
				return false
			}
			rf.mu.Unlock() // exit 3
			now = time.Now()
		} // on timeout, go to the next iteration, increment term, send RPCs, etc
		if Debug {
			rf.mu.Lock()
			Printf("[Server %v] Not Enough Votes, received %v votes for term %v\n", rf.me, rf.currentVotes, rf.currentTerm)
			rf.mu.Unlock()
		}
		// ##########################################################
	}
	return false
}

// LeaderCommit go routine is for leader only, follower do nothing
func (rf *Raft) LeaderCommit(interval time.Duration) {
	for !rf.killed() {
		rf.checkCommit()
		time.Sleep(interval)
	}
}

// Internal implementation of {@code rf.LeaderCommit()}
func (rf *Raft) checkCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Only leader check and set commitIndex
	if rf.state != Leader {
		return
	}
	// Compute majority, e.g.
	// if total==3, majority==2, required==1, since leader is not counted,()
	// if total==4, majority==3, required=2
	var nPeers = len(rf.peers) - 1
	var required int
	if nPeers%2 == 0 {
		required = nPeers / 2
	} else {
		required = nPeers/2 + 1
	}

	// only check with log entryies whose term are equal to leader's currentTerm
	var physicalCommitIndex = rf.logicalToPhysicalIndex(rf.commitIndex)
	// N is physical index into the log
	for N := len(rf.log) - 1; N > max(0, physicalCommitIndex) && rf.log[N].Term == rf.currentTerm; N-- {
		var count = 0
		var logicalN = rf.physicalToLogicalIndex(N)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= logicalN { // matchIndex[] stores logical index!
				count++
			}
		}
		if count >= required {
			rf.commitIndex = logicalN // commitIndex also stores logical Index
			Printf("[Leader %v] Leader Commit Set to %v\n", rf.me, N)
			rf.matchIndex[rf.me] = logicalN // necessary?
			rf.cond.Broadcast()
			break
		}
	}
}

// Send Heartbeat message with empty entries to follower
func (rf *Raft) heartBeat() {
	if rf.state != Leader {
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		var appendEntriesArgs = rf.NewAppendEntriesArgs(i, true)
		go func(server int, args *AppendEntriesArgs) {
			reply, ok := rf.SendAppendEntries(server, args)
			var sentInTerm = args.Term
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok || rf.ConvertToFollowerIfHigherTerm(reply.Term) || rf.state != Leader ||
				sentInTerm != reply.Term || rf.currentTerm != sentInTerm {
				// RPC failed or outdated
				return
			}

			rf.AppendEntriesReplyHandler(server, args, reply)
		}(i, appendEntriesArgs)
	}
}

// Fast log index backup, in case follower lagging behind, 1 RPC per entry -> 1 RPC per term
func (rf *Raft) AppendEntriesReplyHandler(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		var entryLen = len(args.Entries)
		rf.matchIndex[server] = args.PrevLogIndex + entryLen
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		return
	}
	// Follower reject the log because it doesn't have that entry
	if followerMissingLog(reply) {
		rf.nextIndex[server] = reply.XLen // both ConflictLen and ConflictIndex are logical, NOT physical
		return
	}
	// Follower reject the log because of comflict
	rf.nextIndex[server] = reply.XIndex
}

func followerMissingLog(reply *AppendEntriesReply) bool {
	return reply.XTerm == -1 && reply.XIndex == -1
}

// Kick {@code (rf *Raft) ApplyLog()} periodically
func (rf *Raft) ApplyLogKicker(interval time.Duration) {
	for !rf.killed() {
		rf.mu.Lock()
		rf.cond.Broadcast()
		rf.mu.Unlock()
		time.Sleep(interval)
	}
}

// If commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine (§5.3)
func (rf *Raft) ApplyLog(applyLogEntryCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()

		for !(rf.commitIndex > rf.lastApplied) {
			rf.cond.Wait()
		}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyLogEntry(rf.lastApplied, applyLogEntryCh)
		}

		rf.mu.Unlock()
	}
}

// @Param applyIndex is logical
func (rf *Raft) applyLogEntry(applyIndex int, applyLogEntryCh chan ApplyMsg) {
	var applyMsg = rf.NewApplyLogEntry(applyIndex)
	applyLogEntryCh <- *applyMsg
	if Debug {
		if rf.state == Leader {
			Printf("[Leader %v] applied entry: %+v with index %v\n", rf.me, applyMsg.Command, applyMsg.CommandIndex)
		} else {
			Printf("[Server %v] applied entry: %+v with index %v\n", rf.me, applyMsg.Command, applyMsg.CommandIndex)
		}
	}
}

// @Param applyIndex is logical
func (rf *Raft) NewApplyLogEntry(applyIndex int) *ApplyMsg {
	var phyIndex = rf.logicalToPhysicalIndex(applyIndex)
	var applyMsg = ApplyMsg{
		CommandValid: true,
		Command:      rf.log[phyIndex].Command,
		CommandIndex: applyIndex,
	}
	return &applyMsg
}

// Create a fan in
func (rf *Raft) applyChHelper(applyCh chan<- ApplyMsg, applyLogEntryCh <-chan ApplyMsg, applySnapshotCh <-chan ApplyMsg) {
	for {
		select {
		case msg := <-applyLogEntryCh:
			applyCh <- msg
		case msg := <-applySnapshotCh:
			applyCh <- msg
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyLogEntryCh = make(chan ApplyMsg)
	rf.applySnapshotCh = make(chan ApplyMsg)

	var recoverFromCrash = rf.readPersist(persister.ReadRaftState())
	// initialize from persisted state on disk before a crash
	if recoverFromCrash {
		Printf("[Server %v] Recover from crash\n", rf.me)
		// just recovered from crash
		rf.state = Follower
		rf.snapshot = persister.ReadSnapshot()
	} else { // Not recovering from crash
		// To Persistent Storage
		Printf("[Server %v] Init\n", rf.me)
		rf.log = append(rf.log, *rf.NewNoOpLogEntry())
		rf.snapshot = nil
		// Your initialization code here (2A, 2B, 2C).
		rf.becomeFollower(0) // on boot init to follower

		if Debug {
			if rf.log[0].Term != 0 || rf.log[0].Command != NO_OP_CMD {
				panic("rf.NewNoOpLogEntry() Failed")
			}
		}
	}

	// Volatile State on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile State on leaders
	rf.nextIndex = make([]int, len(peers))  // No need to init when the state is follower
	rf.matchIndex = make([]int, len(peers)) // (should init to 0)

	rf.resetFollowerTimer()

	// start ticker goroutine to start elections
	go rf.ticker(time.Duration(TICKER_INTERVAL) * time.Millisecond)            // for state == Follower
	go rf.SendLog(time.Duration(SEND_LOG_INTERVAL) * time.Millisecond)         // for state == Leader
	go rf.LeaderCommit(time.Duration(APPLY_LOG_INTERVAL) * time.Millisecond)   // for checking commit, state == leader
	go rf.ApplyLogKicker(time.Duration(APPLY_LOG_INTERVAL) * time.Millisecond) // for apply changes
	go rf.applyChHelper(applyCh, rf.applyLogEntryCh, rf.applySnapshotCh)
	go rf.ApplyLog(rf.applyLogEntryCh)

	if Debug {
		go func() {
			for {
				log.Printf("[Server %v] Alive\n", rf.me)
				time.Sleep(1 * time.Second)
			}
		}()
	}
	return rf
}

// TODO: Modify for 2D
// Compute the index of first entry with conflicting term(xTerm)
// @param physcialStartingIndex: NOT LOGICAL INDEX
// @return Physical XIndex
func (rf *Raft) getConflictingIndex(physicalStartingIndex, xTerm int) int {
	var i = physicalStartingIndex - 1
	for i >= 1 && rf.log[i].Term >= xTerm {
		i--
	}
	i++ // make sure it doesn't fall off the bound
	return i
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of the log entry at prevLogIndex
	Entries      []LogEntry // log entries to store, empty for heartbeat messages
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // current term, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // Term of conflicting entry
	XIndex  int  // Index of first entry with conflicting term
	XLen    int  // length of the follower's log
}

// TODO: Modify for 2D
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check RPC request's term, if higher, convert to follower
	rf.ConvertToFollowerIfHigherTerm(args.Term)

	// ignore outdated RPC
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// otherwise, it's a valid RPC, reset the timer
	// This will actually cancel the election if this raft instance is in follower state
	rf.resetFollowerTimer()
	rf.state = Follower

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// or prevLogIndex points beyond the end of the log
	if phyPrevLogIndex := rf.logicalToPhysicalIndex(args.PrevLogIndex); phyPrevLogIndex >= len(rf.log) {
		reply.Term = rf.currentTerm
		reply.Success = false
		// Case 3: follower doesn't have the log
		// Follower : [4]
		// Leader   : [4 6 6 6]
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.getLogicalLogLength()
		return
	}
	if phyPrevLogIndex := rf.logicalToPhysicalIndex(args.PrevLogIndex); rf.log[phyPrevLogIndex].Term != args.PrevLogTerm {
		// Handle case 1 and 2
		// Case 1: leader doesn't have follower's term
		// F: [4 5 5]
		// L: [4 6 6 6]
		// Case 2: leader does have follower's term
		// F: [4 4 4]
		// L: [4 6 6 6]
		reply.Term = rf.currentTerm
		reply.Success = false
		var entryTerm = rf.log[phyPrevLogIndex].Term
		var phyConflictingIndex = rf.getConflictingIndex(phyPrevLogIndex, entryTerm)

		reply.XLen = len(rf.log)
		reply.XTerm = entryTerm
		// getConflictingIndex returns a physcial index, convert it to a *logical* index
		var conflictingIndex = rf.physicalToLogicalIndex(phyConflictingIndex)
		reply.XIndex = conflictingIndex
		return
	}

	// 3 Cases
	// log      [1 2 3 4 5 6]
	// entries              [7, 8]
	var thisLastLogIndex, _ = rf.getLogicalLastLogEntryIndexTerm()
	if args.PrevLogIndex == thisLastLogIndex {
		if Debug && len(args.Entries) != 0 {
			Printf("[Server %v] Just append the log %+v\n", rf.me, args.Entries)
		}
		// just append
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		reply.Term = rf.currentTerm
		reply.Success = true // set reply.Success == True if folloer contained entry matching prevLogIndex and prevLogTerm

	} else if phyPrevLogIndex := rf.logicalToPhysicalIndex(args.PrevLogIndex); rf.EntryInBound(phyPrevLogIndex) && phyPrevLogIndex+len(args.Entries) < len(rf.log) {
		// check if match, otherwise clip the log,
		// prevLogIndex == 3 in this case
		// log      [1 2 3 4 5 6]
		// entries        [4 5 6]
		var curr = phyPrevLogIndex + 1 // curr is physical index
		var i = 0
		for i < len(args.Entries) {
			// i is physical index, used for indexing into args.Entries[],
			// logIdx is a physical index, used for indexing into rf.log[]
			var logIdx = curr + i // logIdx is init to curr
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

	} else if phyPrevLogIndex := rf.logicalToPhysicalIndex(args.PrevLogIndex); rf.EntryInBound(phyPrevLogIndex) && phyPrevLogIndex+len(args.Entries) >= len(rf.log) {
		// log      [1 2 3 4 5 6]
		// entries          [5 6 7 8]
		//                   c
		var curr = phyPrevLogIndex + 1

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
		reply.Term = rf.currentTerm
		Printf("[Server %v] AppendEntries RPC handler: Should never happen: args: %+v\n", rf.me, *args)
	}
	// should all success at this point unless the args.PrevLogIndex is not in bound, <0 (Unchecked and unhandled)
	// Printf("[Server %v] AppendEntries RPC Handler returns %v\n", rf.me, reply.Success)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.cond.Broadcast()
	}
	Printf("[Server %v]: log %+v\n", rf.me, rf.log)
	Printf("[Server %v]: Commit Index: %v\n", rf.me, rf.commitIndex)
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
	var logicalPrevLogEntryIndex = rf.nextIndex[server] - 1 // nextIndex[server] is a logical index
	//var physicalPrevLogEntryIndex = rf.logicalToPhysicalIndex(logicalPrevLogEntryIndex)
	// prevLogEntryIndex is physical index now
	var prevLogEntryTerm = rf.getLogEntryTerm(logicalPrevLogEntryIndex)

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: logicalPrevLogEntryIndex,
		PrevLogTerm:  prevLogEntryTerm,
		Entries:      rf.log[rf.nextIndex[server]:], // TODO: Optimization!
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current term, for candidate to update it self
	VoteGranted bool // true means candidates received vote
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
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

// Wrapper around rf.sendRequestVote()
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	if server == rf.me {
		log.Fatalf("[Error: Server %v] Send RequestVote RPC to itself, wtf?\n", rf.me)
	}
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	return reply, ok
}

func (rf *Raft) NewRequestVoteArgs() *RequestVoteArgs {
	var lastLogEntryIndex, lastLogEntryTerm = rf.getLogicalLastLogEntryIndexTerm()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntryIndex,
		LastLogTerm:  lastLogEntryTerm,
	}
	return args
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int // Receiver's term, for leader to update itself
	Success bool
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs) (*InstallSnapshotReply, bool) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	return reply, ok
}

//TODO: BROKEN!
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ConvertToFollowerIfHigherTerm(args.Term)

	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.lastIncludedIndex {
		// ignore outdated RPC and ignore outdated snapshot
		// Since the snapshot is already committed, no need to do term check
		// instead make sure our receiving peer does not go back to an older state with
		// lastIncludedIndex check
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//TODO: BROKEN!
	var logicalLastIncludedIndex = args.LastIncludedIndex
	var physicalLastIncludedIndex = rf.logicalToPhysicalIndex(logicalLastIncludedIndex)

	if rf.EntryInBound(physicalLastIncludedIndex) { // already have the data, do a local compaction, throw away log
		rf.Compact(logicalLastIncludedIndex, args.Data)
	} else { // don't have any data, just save
		rf.snapshot = args.Data
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.log = []LogEntry{*rf.NewNoOpLogEntry()}
	}
	rf.persist() // fsync before reply to RPC
	rf.resetFollowerTimer()

	var msg = rf.NewApplyMsgSnapshot()
	rf.applySnapshotCh <- *msg
	// Todo:
	// Commit this snapshot
	// commit index?
	// Reply to RPC
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	reply.Term = rf.currentTerm
	reply.Success = true
	Printf("[Server %v] Snapshot Applied\n", rf.me)

}

func min(nums ...int) int {
	if len(nums) == 0 {
		log.Fatalf("[Error]: in min(nums ...int), Zero Arguments\n")
	}
	var minimum = nums[0]
	for _, num := range nums {
		if num < minimum {
			minimum = num
		}
	}
	return minimum
}

func max(nums ...int) int {
	if len(nums) == 0 {
		log.Fatalf("[Error]: in max(nums ...int), Zero Arguments\n")
	}
	var maximum = nums[0]
	for _, num := range nums {
		if num > maximum {
			maximum = num
		}
	}
	return maximum
}

// stolen from https://stackoverflow.com/questions/55093676/checking-if-current-time-is-in-a-given-interval-golang/55093788
func inTimeSpan(start, end, check time.Time) bool {
	if start.Before(end) {
		return !check.Before(start) && !check.After(end)
	}
	if start.Equal(end) {
		return check.Equal(start)
	}
	panic("[Error]: inTimeSpan() start after end!")
}

func GetRandomTimeout(lo, hi int, unit time.Duration) time.Duration {
	if hi-lo < 0 || lo < 0 || hi < 0 {
		panic("[WARNING]: getRandomTimeout: Param Error")
	}
	return time.Duration(lo+rand.Intn(hi-lo)) * unit
}

// if call to logicalToPhysicalIndex() returns 0
// then caller should use rf.lastIncludedIndex and rf.lastIncludedTerm
func (rf *Raft) logicalToPhysicalIndex(logicalIndex int) int {
	/* Example:         0 1 2 3 4 5
	original           [0 1 2 3 4 5] lastIncludedIndex = 0, lastIncludedTerm = 0, log length = 6
	after compaction:  [0 4 5]     lastIncludedIndex = 3, lastIncludedTerm = 3, log length = 3
	now want: index 5, actual => 5 - 3 = 2
	now want index 3, actual => 3 - 3 = 0 // this is already invalid, remember the first entry is an invalid placeholder
	now want index 1, actual => = -2, already included in snapshot, cannot get it
	*/
	var physicalIndex = logicalIndex - rf.lastIncludedIndex
	if physicalIndex < 0 {
		log.Printf("[Server %v] logicalToPhysicalIndex() returns < 0\n", rf.me)
	}
	return physicalIndex
}

// Logical Index are the index described in the raft paper
// Physical Index are used to index into rf.log[] (after compaction, physical index != logical index)
func (rf *Raft) physicalToLogicalIndex(physicalIndex int) int {
	return rf.lastIncludedIndex + physicalIndex
}

// @return: logical last index on the log(may be compacted) and its associated term
func (rf *Raft) getLogicalLastLogEntryIndexTerm() (int, int) {
	if Debug {
		if rf.lastIncludedIndex < 0 {
			log.Fatalf("[Server %v] lastIncludedIndex < 0\n", rf.me)
		}
	}
	if len(rf.log) == 1 {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	var idx = len(rf.log) - 1 // idx is physical index
	var logicalIdx = rf.physicalToLogicalIndex(idx)
	return logicalIdx, rf.log[idx].Term
}

// @Param logicalIndex
// Given a logicalIndex, return its corresponding term
func (rf *Raft) getLogEntryTerm(logicalIndex int) int {
	phyIdx := rf.logicalToPhysicalIndex(logicalIndex)
	if phyIdx == 0 {
		return rf.lastIncludedTerm
	}
	if !rf.EntryInBound(phyIdx) {
		log.Fatalf("[Server %v] getLogicalLogEntryTerm(), Out of Bound\n", rf.me)
	}
	return rf.log[phyIdx].Term
}

func (rf *Raft) getLogicalLogLength() int {
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) NewApplyMsgSnapshot() *ApplyMsg {
	var m = ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	return &m
}
