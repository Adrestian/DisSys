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
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	CHECK_COMMIT_INTERVAL int = 500 // Currently not used anywhere

	TICKER_SLEEP_INTERVAL int = 25 // used in {@code ticker()} to sleep rather than spinning

	LEADER_HB_INTERVAL int = 100 // Time interval where leader sends out heartbeat to all followers

	ELECTION_TIMER_LO int = 200 // Candidate's election timeout lower and upper bound
	ELECTION_TIMER_HI int = 500 // on timeout, increment term and start a new election

	HB_TIMER_LOWERBOUND int = 200  // Follower's heartbeat timeout lower and upper bound
	HB_TIMER_UPPERBOUND int = 1000 // on timeout, follower become candidate and start a new election
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
var hb chan interface{} = make(chan interface{})

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
	Printf("[Server %v]:GetState(): Term %v, isLeader: %v, voted For: %v\n", rf.me, term, isLeader, rf.votedFor)
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

//
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
	var votedFor *int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil {
		Println("[Error] Decoding currentTerm")
	}

	if d.Decode(&votedFor) != nil {
		Println("[Error] Decoding votedFor")
	}

	if d.Decode(&log) != nil {
		Println("[Error] Decoding Log")
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
	Term    int  // current term, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
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

// To implement heartbeats, define an AppendEntries RPC struct
// (though you may not need all the arguments yet),
// and have the leader send them out periodically.
// Write an AppendEntries RPC handler method that resets the election timeout
// so that other servers don't step forward as leaders when one has already been elected.
// Note AppendEntries RPC is on follower
// args is filled by the leader
// reply is filled by follower
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var term = args.Term
	rf.ConvertToFollowerIfNeeded(args.Term)
	// var leaderId = args.LeaderId
	var prevLogIndex = args.PrevLogIndex
	var prevLogTerm = args.PrevLogTerm
	var entries = args.Entries
	var leaderCommit = args.LeaderCommit

	rf.mu.Lock() // hold the lock from here <------------
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm // send out currentTerm regardless

	if term < rf.currentTerm {
		reply.Success = false
		return
	} // Reply false if term < currentTerm, Section 5.1(outdated information)

	go func() {
		hb <- struct{}{}
	}() // reset the timer

	if prevLogIndex >= len(rf.log) || prevLogIndex < 0 || rf.log[prevLogIndex].Term != prevLogTerm {
		reply.Success = false
		return
	} // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm

	/* 3. If an existing entry conflicts with a new one (same index but different terms),
	delete the existing entry and all that follow it (5.3) */

	/* 4. Append any new entry not already in the log */

	if prevLogIndex < len(rf.log) && prevLogIndex >= 0 {
		rf.log = rf.log[:prevLogIndex+1] // clip the follower log
		if len(entries) != 0 {
			rf.log = append(rf.log, entries...)
		} // append the entries to follower log
	}

	/* 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) */

	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, len(rf.log)-1)
	}

}

// Find the minimum amoug all the integers
func min(nums ...int) int {
	if len(nums) == 0 {
		panic("Empty Slice")
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
	var candidateLogOk = isCandidateAsUpToDate(lastLogEntryIndex, lastLogEntryTerm, candidateLastEntryIndex, candidateLastEntryTerm)
	if candidateLogOk && (rf.votedFor == NULL || rf.votedFor == candidateId) {
		// Grant Vote
		if rf.state == Follower {
			reply.VoteGranted = true
			rf.votedFor = candidateId
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
func isCandidateAsUpToDate(thisLastEntryIndex, thisLastEntryTerm, candidateLastEntryIndex, candidateLastEntryTerm int) bool {
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.state == Leader

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
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		var state = rf.state
		rf.mu.Unlock()
		if state == Follower {
			// On every iteration, pick a new random timeout
			var randomAmountOfTime = GetRandomTimeout(HB_TIMER_LOWERBOUND, HB_TIMER_UPPERBOUND, time.Millisecond)
			// check if a leader election should
			// be started and to randomize sleeping time using time.Sleep().
			var electionTimeoutCh, _ = makeTimeoutChan(randomAmountOfTime)

			select {
			case <-hb:
				continue
			case <-electionTimeoutCh:
				Printf("[Server %v] Haven't received any HB, Election Timeout\n", rf.me)
				rf.mu.Lock()
				rf.state = Candidate
				rf.mu.Unlock()
				// crucial, so in the next iteration of {@code ticker()} {@code else if state == Candidate} branch will be taken regardless how the runtime schedule rf.startElection go routing
				go rf.startElection() // after {@code rf.startElection()}, the state become Candidate so ticker() will do nothing
			}
		} else if state == Candidate { // state == Candidate, do nothing for a while
			time.Sleep(time.Duration(TICKER_SLEEP_INTERVAL) * time.Millisecond)

		} else if state == Leader { // state == Leader, do nothing for a while
			time.Sleep(time.Duration(TICKER_SLEEP_INTERVAL) * time.Millisecond)

		} else { // unknown raft state
			time.Sleep(time.Duration(TICKER_SLEEP_INTERVAL) * time.Millisecond)
		}
	}
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

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatRoutine(time.Duration(LEADER_HB_INTERVAL) * time.Millisecond)

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

		leaderElectionTimeout, cancelElectionTimeout := makeTimeoutChan(timeout)
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

					} else if !ok {
						// RPC Failed
					}

					if rf.currentVotes == rf.majorityVotes {
						rf.state = Leader
						rf.mu.Unlock()
						rf.SendHeartbeatToAllFollowers() //immediatedly sends out one round of broadcast heartbeat
						go func() {
							leaderElectionSuccessCh <- struct{}{}
						}()
						return
					}
					rf.mu.Unlock() // <---- unlock here
				}(i, requestVoteArgs) // send RPC in parallel
			}
		}

		select {
		case <-leaderElectionTimeout:
			Printf("[server %v] Election Timeout!, make a new one!\n", rf.me)
			continue
		case <-leaderElectionSuccessCh:
			cancelElectionTimeout <- struct{}{}
			return true
		case <-hb: // new appendEntries arrived!
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
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		Entries:      rf.log[rf.nextIndex[server]-1:],
		LeaderCommit: rf.commitIndex,
	}
	if useEmptyEntry { // if useEmptyEntry flag is set to true, attach empty log to it
		args.Entries = []LogEntry{}
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
		Printf("%v Sending HB but not leader, return\n", rf.me)
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
func (rf *Raft) becomeFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = NULL
	rf.majorityVotes = 999
	rf.currentVotes = -999
	rf.state = Follower
}

// Does not require the lock!
// this function set the state to candidate, increment term, vote for itself
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.majorityVotes = (len(rf.peers) / 2) + 1
	rf.currentVotes = 1 // vote for itself
}
