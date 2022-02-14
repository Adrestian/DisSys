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
	"log"
	"math"
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
	NULL      int = -1 // Seems like init to 0 is a bad idea
)

const (
	FOLLOWER_HB_TIMEOUT_LOWER int = 250 // Lower and Upper bound for the timeout where the follower becomes candidate
	FOLLOWER_HB_TIMEOUT_UPPER int = 900 // if no appendentries RPC has been received from leader or voted for other candidates

	SEND_LOG_INTERVAL int = 75 // highest rate capped at 10/sec
	TICKER_INTERVAL   int = 5

	ELECTION_TIMEOUT_LOWER     int = 300
	ELECTION_TIMEOUT_UPPER     int = 800
	ELECTION_CHECKING_INTERVAL int = 10

	APPLY_LOG_INTERVAL int = 5

	NO_OP_CMD = "__NO_OP"
)

var (
	LastReceivedMu  sync.Mutex
	LastReceived    time.Time     // AppendEntries/RequestVote RPC may reset the timer
	FollowerTimeout time.Duration // on timeout become candidate
	// between FOLLOWER_HB_TIMEOUT_LOWER and FOLLOWER_HB_TIMEOUT_UPPER

	ElectionStartedMu sync.Mutex
	ElectionStarted   time.Time
	ElectionTimeout   time.Duration // on timeout, start a new term, transition to candidate
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// *Persistent* State on ALL SERVERS
	// UPDATE on stable storage BEFORE responding to RPCs!
	currentTerm int        // last term this server has seen, initialize to 0 on boot, increase monotonically
	votedFor    int        // candidateId that received vote in currentTerm, NULL if not voted
	log         []LogEntry /* Log Entries
	each entry contains command for state machine,
	and term when entry was received by leader,
	first index is 1, on init we add a place holder log entry into the log
	to deal with this 1-index problem
	*/

	// *Volatile State* on ALL SERVERS
	commitIndex int // index of highest log entry known to be committed(init to 0, increase monotonically)
	lastApplied int // Index of highest log entry applied to state machine (init to 0, incrase monotonically)

	// *Volatile State* on LEADER, reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (init to leader's last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (init to 0, increase monotonically)

	// election Related, simple book-keeping
	majorityVotes int
	currentVotes  int
}

// Generate a new No-op log entry from current term
// Sync needed, Caller require {@code rf.mu}
func (rf *Raft) NewNoOpLogEntry() *LogEntry {
	var entry = LogEntry{
		Term:    rf.currentTerm,
		Command: NO_OP_CMD,
	}
	return &entry
}

// return currentTerm and whether this server
// believes it is the leader.
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(len(rf.log)) // encode the log length
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 { // nothing to read
		if Debug {
			log.Printf("[Server %v] Read from disk, nothing to read\n", rf.me)
		}
		return
	}
	/*// Your code here (2C).
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
	*/

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cTerm, vFor, logLength int

	if err := d.Decode(&cTerm); err != nil {
		log.Fatalf("[Error] %v Cannot Decode Current Term", rf.me)
	}
	if err := d.Decode(&vFor); err != nil {
		log.Fatalf("[Error] %v Cannot Decode votedFor\n", rf.me)
	}
	if err := d.Decode(&logLength); err != nil {
		log.Fatalf("[Error] %v Cannot Decode log length\n", rf.me)
	}

	var logEntry = make([]LogEntry, logLength)
	rf.log = logEntry
	rf.leaderInit()
}

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
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ConvertToFollowerIfNeeded(args.Term) // check the term in request args

	// reply with this raft instance's term regardless
	reply.Term = rf.currentTerm
	// Candidate term is too low, vote no immediately
	var candidateTerm = args.Term
	if candidateTerm < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	var candidateId = args.CandidateId
	var candidateLastEntryIndex = args.LastLogIndex
	var candidateLastEntryTerm = args.LastLogTerm

	var lastEntryIndex = len(rf.log) - 1
	var lastEntryTerm = rf.log[lastEntryIndex].Term

	var candidateLogOK = isCandidateLogOk(lastEntryIndex, lastEntryTerm, candidateLastEntryIndex, candidateLastEntryTerm)

	if candidateLogOK && (rf.votedFor == NULL || rf.votedFor == candidateId) { // grant vote
		rf.state = Follower // make sure
		rf.votedFor = candidateId
		rf.persist()
		//Printf("[Server %v] Voted YES For %v\n", rf.me, rf.votedFor)
		resetFollowerTimer() // reset the follower timer
		reply.VoteGranted = true
		return
	}
	// otherwise vote no
	//Printf("[Server %v] Voted NO For %v\n", rf.me, args.CandidateId)
	reply.VoteGranted = false
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

// Reset the follower timer
func resetFollowerTimer() {
	LastReceivedMu.Lock()
	defer LastReceivedMu.Unlock()
	LastReceived = time.Now()
	FollowerTimeout = GetRandomTimeout(FOLLOWER_HB_TIMEOUT_LOWER, FOLLOWER_HB_TIMEOUT_UPPER, time.Millisecond)
}

// Access wrapper function
func getLastReceived() (time.Time, time.Duration) {
	LastReceivedMu.Lock()
	defer LastReceivedMu.Unlock()
	return LastReceived, FollowerTimeout
}

func getDeadline() time.Time {
	LastReceivedMu.Lock()
	defer LastReceivedMu.Unlock()
	return LastReceived.Add(FollowerTimeout)
}

func getElectionDeadline() time.Time {
	ElectionStartedMu.Lock()
	defer ElectionStartedMu.Unlock()
	return ElectionStarted.Add(ElectionTimeout)
}

// Reset the election timer
func resetElectionTimer() {
	ElectionStartedMu.Lock()
	defer ElectionStartedMu.Unlock()
	ElectionTimeout = GetRandomTimeout(ELECTION_TIMEOUT_LOWER, ELECTION_TIMEOUT_UPPER, time.Millisecond)
	ElectionStarted = time.Now()
}

// Return when the election started, and when the election expires
func getElectionTimer() (time.Time, time.Time) {
	ElectionStartedMu.Lock()
	defer ElectionStartedMu.Unlock()
	var ddl = ElectionStarted.Add(ElectionTimeout)
	return ElectionStarted, ddl
}

// Check if the index is in bound of the log
func (rf *Raft) EntryInBound(index int) bool {
	if index < 0 || index >= len(rf.log) {
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
		var newEntry = LogEntry{Term: term, Command: command}
		rf.log = append(rf.log, newEntry)
		rf.persist()
	}
	// Your code here (2B).

	return index, term, isLeader
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
func (rf *Raft) ticker(interval time.Duration) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Follower {
			var lastReceived, _ = getLastReceived()
			var deadline = getDeadline()
			var now = time.Now()
			if !inTimeSpan(lastReceived, deadline, now) {
				// Note:
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
	resetElectionTimer()
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

// This function does not require the lock!
// change the state to Follower,
// set the currentTerm to newTerm and reset votedFor
func (rf *Raft) becomeFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = NULL
	rf.majorityVotes = math.MaxInt
	rf.currentVotes = math.MinInt
	rf.state = Follower
	rf.persist() // write to disk
}

// Check if newTerm is higher the current term
// If so, convert to follower and return true
// otherwise do nothing and return false
func (rf *Raft) ConvertToFollowerIfNeeded(newTerm int) bool {
	if newTerm > rf.currentTerm {
		rf.becomeFollower(newTerm)
		return true
	}
	return false
}

func (rf *Raft) SendLog(interval time.Duration) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader { // Send Follower log
			for server := range rf.peers {
				if server == rf.me {
					continue
				}

				var args = rf.NewAppendEntriesArgs(server, false)
				Printf("[Leader %v] with term: %v send out log to follower %v\n", rf.me, rf.currentTerm, server)
				go func(server int, args *AppendEntriesArgs) {
					var reply, ok = rf.SendAppendEntries(server, args)
					var sentInTerm = args.Term

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if !ok || rf.state != Leader || sentInTerm != reply.Term || rf.currentTerm != args.Term {
						return
					}

					if rf.ConvertToFollowerIfNeeded(reply.Term) {
						return
					}
					rf.AppendEntriesReplyHandler(server, args, reply)

				}(server, args)
			}
		}
		rf.mu.Unlock()
		time.Sleep(interval)
	}
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
				if !ok || rf.ConvertToFollowerIfNeeded(reply.Term) ||
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
					// immediately send one round of heartbeat to followers
					rf.sendHB()
				}
			}(i, requestVoteArgs)
		}

		rf.mu.Unlock()
		// ###### This section of code is kind of shit ############
		// wait until Election Timeout
		var electionStarted, electionDeadline = getElectionTimer()
		var now = time.Now()
		for inTimeSpan(electionStarted, electionDeadline, now) {
			time.Sleep(checkInterval)
			rf.mu.Lock()
			var state = rf.state
			if state == Leader {
				rf.mu.Unlock() // exit 1
				return true
			} else if state == Follower { // Candidate receive valid RPC AppendEntries
				rf.mu.Unlock() // exit 2
				return false
			}
			rf.mu.Unlock() // exit 3
			now = time.Now()
		} // on timeout, go to the next iteration, increment term, send RPCs, etc

		// ##########################################################
	}
	return false
}

// LeaderCommit routing is for leader only
func (rf *Raft) LeaderCommit(interval time.Duration) {
	for !rf.killed() {
		rf.checkCommit()
		time.Sleep(interval)
	}
}

// Internal implementation of {@code func (rf *Raft) Commit(interval time.Duration)}
func (rf *Raft) checkCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	} // Only leader check and set commitIndex
	var nPeers = len(rf.peers) - 1
	var majority = nPeers/2 + 1
	if nPeers%2 == 0 {
		majority = nPeers / 2
	}

	for N := len(rf.log) - 1; N > rf.commitIndex && rf.log[N].Term == rf.currentTerm; N-- {
		var count = 0
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				count++
			}
		}
		if count >= majority {
			rf.commitIndex = N
			rf.matchIndex[rf.me] = N
			rf.cond.Broadcast()
			break
		}
	}
}

// Send Heartbeat message with empty entries to follower
func (rf *Raft) sendHB() {
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
			if !ok || rf.ConvertToFollowerIfNeeded(reply.Term) || rf.state != Leader ||
				sentInTerm != reply.Term || rf.currentTerm != args.Term {
				// RPC failed or outdated
				return
			}

			rf.AppendEntriesReplyHandler(server, args, reply)

		}(i, appendEntriesArgs)
	}
}

func (rf *Raft) AppendEntriesReplyHandler(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		var entryLen = len(args.Entries)
		rf.matchIndex[server] = args.PrevLogIndex + entryLen
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		return
	}
	// Follower reject the log because it doesn't have that entry
	if followerMissingLog(reply) {
		rf.nextIndex[server] = reply.ConflictLen
		return
	}
	// Follower reject the log because of comflict
	rf.nextIndex[server] = reply.ConflictIndex
}

func followerMissingLog(reply *AppendEntriesReply) bool {
	return reply.ConflictTerm == -1 && reply.ConflictIndex == -1
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
func (rf *Raft) ApplyLog(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()

		for !(rf.commitIndex > rf.lastApplied) {
			rf.cond.Wait()
		}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyLogEntry(rf.lastApplied, applyCh)
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) applyLogEntry(applyIndex int, applyCh chan ApplyMsg) {
	var applyMsg = rf.NewApplyMsg(applyIndex)
	applyCh <- *applyMsg

}

func (rf *Raft) NewApplyMsg(applyIndex int) *ApplyMsg {
	var applyMsg = ApplyMsg{
		CommandValid: true,
		Command:      rf.log[applyIndex].Command,
		CommandIndex: applyIndex,
	}
	return &applyMsg
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
	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.currentTerm == 0 && len(rf.log) == 0 && rf.votedFor == 0 { // Go's default init, not starting from crash
		// To Persistent Storage
		rf.log = append(rf.log, *rf.NewNoOpLogEntry())

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

	resetFollowerTimer()

	// start ticker goroutine to start elections
	go rf.ticker(time.Duration(TICKER_INTERVAL) * time.Millisecond)            // for state == Follower
	go rf.SendLog(time.Duration(SEND_LOG_INTERVAL) * time.Millisecond)         // for state == Leader
	go rf.LeaderCommit(time.Duration(APPLY_LOG_INTERVAL) * time.Millisecond)   // for checking commit, state == leader
	go rf.ApplyLogKicker(time.Duration(APPLY_LOG_INTERVAL) * time.Millisecond) // for apply changes
	go rf.ApplyLog(applyCh)
	return rf
}

// Compute the index of first entry with conflicting term(xTerm)
func (rf *Raft) getConflictingIndex(startingIndex, xTerm int) int {
	var i = startingIndex - 1
	for i >= 1 && rf.log[i].Term >= xTerm {
		i--
	}
	i++ // make sure it doesn't fall off the bound
	return i
}
