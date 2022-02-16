package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int // Receiver's term, for leader to update itself
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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ConvertToFollowerIfHigherTerm(args.Term)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return // ignore outdated RPC
	}

	if args.LastIncludedTerm < rf.lastIncludedTerm || args.LastIncludedIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		return // already snapshotted! Ignore
	}

	rf.Compact(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	rf.persist() // fsync before reply to RPC
	reply.Term = rf.currentTerm
}
