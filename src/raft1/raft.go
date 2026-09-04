package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"6.5840/labgob"
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Role int

const (
	LEADER Role = iota
	CANDIDATE
	FOLLOWER
)

type LogEntry struct {
	Term    int
	Command any
}

const (
	heartbeatInterval     = 100 * time.Millisecond
	replicationRPCTimeout = 200 * time.Millisecond

	electionCheckIntervalMin = 10 * time.Millisecond
	electionCheckIntervalMax = 30 * time.Millisecond
	electionTimeoutMin       = 400 * time.Millisecond
	electionTimeoutMax       = 800 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	delta := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin +
		time.Duration(rand.Int63n(int64(delta)))
}
func randomElectionCheck() time.Duration {
	delta := electionCheckIntervalMax - electionCheckIntervalMin
	return electionCheckIntervalMin + time.Duration(rand.Int63n(int64(delta)))
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	voteFor     int
	role        Role
	log         []LogEntry

	logOffset int

	deadline    time.Time
	commitIndex int
	lastApplied int

	nextIndex   []int
	matchIndex  []int
	replicateCh []chan struct{}
	snapshot    []byte

	pendingSnapshotValid bool
	pendingSnapshot      []byte
	pendingSnapshotTerm  int
	pendingSnapshotIndex int

	applyCh chan raftapi.ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// rf.log[0] represents the entry covered by the latest snapshot. These
// helpers keep logical Raft indexes separate from slice indexes.
func (rf *Raft) lastLogIndexLocked() int {
	return rf.logOffset + len(rf.log) - 1
}

func (rf *Raft) logSliceIndexLocked(index int) int {
	return index - rf.logOffset
}

func (rf *Raft) logTermLocked(index int) int {
	return rf.log[rf.logSliceIndexLocked(index)].Term
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntryReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.role == LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.logOffset)

	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var log []LogEntry
	var logOffset int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&logOffset) != nil {
		return
	}

	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.log = log
	rf.logOffset = logOffset
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// The service may only compact entries it has already applied, and the
	// requested index must still be present in this Raft log.
	if index <= rf.logOffset ||
		index > rf.lastApplied ||
		index > rf.lastLogIndexLocked() {
		return
	}

	snapshotTerm := rf.logTermLocked(index)
	firstRetained := rf.logSliceIndexLocked(index) + 1

	// Allocate a new backing array so commands in the discarded prefix are
	// no longer reachable and can be reclaimed by the garbage collector.
	newLog := make([]LogEntry, 1, 1+len(rf.log)-firstRetained)
	newLog[0] = LogEntry{Term: snapshotTerm}
	newLog = append(newLog, rf.log[firstRetained:]...)
	rf.log = newLog
	rf.logOffset = index
	rf.snapshot = append([]byte(nil), snapshot...)
	rf.persist()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.role = FOLLOWER
		rf.persist()
	}
	reply.Term = rf.currentTerm
	canVote := rf.voteFor == -1 || rf.voteFor == args.CandidateId
	if canVote && rf.candidateLogIsUpToDate(args) {
		rf.voteFor = args.CandidateId
		rf.resetElectionTimeoutLocked()
		reply.VoteGranted = true
		rf.persist()
		return

	}
	reply.VoteGranted = false

}

func (rf *Raft) candidateLogIsUpToDate(args *RequestVoteArgs) bool {
	myLastIndex := rf.lastLogIndexLocked()
	myLastTerm := rf.logTermLocked(myLastIndex)
	if args.LastLogTerm != myLastTerm {
		return args.LastLogTerm > myLastTerm
	}
	return args.LastLogIndex >= myLastIndex
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
	}
	rf.role = FOLLOWER
	rf.resetElectionTimeoutLocked()

	reply.Term = rf.currentTerm

	if args.PrevLogIndex < rf.logOffset {
		// The follower has already compacted this prefix. Ask the leader to
		// retry immediately after the snapshot boundary.
		reply.XTerm = -1
		reply.XLen = rf.logOffset + 1
		return
	}

	if args.PrevLogIndex > rf.lastLogIndexLocked() {
		reply.XTerm = -1
		reply.XLen = rf.lastLogIndexLocked() + 1
		return
	}

	if rf.logTermLocked(args.PrevLogIndex) != args.PrevLogTerm {
		reply.XTerm = rf.logTermLocked(args.PrevLogIndex)
		reply.XLen = rf.lastLogIndexLocked() + 1
		firstIndex := args.PrevLogIndex
		for firstIndex > rf.logOffset &&
			rf.logTermLocked(firstIndex-1) == reply.XTerm {
			firstIndex--
		}
		reply.XIndex = firstIndex
		return
	}

	for i := 0; i < len(args.Entries); i++ {
		logIdx := args.PrevLogIndex + 1 + i
		if logIdx >= len(rf.log)+rf.logOffset {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}

		if rf.log[logIdx-rf.logOffset].Term != args.Entries[i].Term {
			rf.log = rf.log[:logIdx-rf.logOffset]
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	lastMatchedIndex :=
		args.PrevLogIndex + len(args.Entries)

	newCommitIndex :=
		min(args.LeaderCommit, lastMatchedIndex)

	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
	}
	reply.Success = true

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
	}
	rf.role = FOLLOWER
	rf.resetElectionTimeoutLocked()
	reply.Term = rf.currentTerm

	// The follower already knows that everything through commitIndex is
	// committed, so installing an older snapshot could only move it backward.
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	retainSuffix := args.LastIncludedIndex >= rf.logOffset &&
		args.LastIncludedIndex <= rf.lastLogIndexLocked() &&
		rf.logTermLocked(args.LastIncludedIndex) == args.LastIncludedTerm

	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Term: args.LastIncludedTerm}
	if retainSuffix {
		firstRetained := rf.logSliceIndexLocked(args.LastIncludedIndex) + 1
		newLog = make([]LogEntry, 1, 1+len(rf.log)-firstRetained)
		newLog[0] = LogEntry{Term: args.LastIncludedTerm}
		newLog = append(newLog, rf.log[firstRetained:]...)
	}

	rf.log = newLog
	rf.logOffset = args.LastIncludedIndex
	rf.snapshot = append([]byte(nil), args.Data...)
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.pendingSnapshotValid = true
	rf.pendingSnapshot = append([]byte(nil), args.Data...)
	rf.pendingSnapshotTerm = args.LastIncludedTerm
	rf.pendingSnapshotIndex = args.LastIncludedIndex
	rf.persist()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	if rf.role != LEADER {
		rf.mu.Unlock()
		return -1, term, false
	}
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	index := rf.lastLogIndexLocked()

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()
	rf.mu.Unlock()
	rf.signalReplication()
	return index, term, true
}

func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()

		if rf.role != LEADER && time.Now().After(rf.deadline) {
			args, electionTerm := rf.beginElectionLocked()
			rf.mu.Unlock()
			rf.sendRequestVotes(args, electionTerm)
		} else {
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := randomElectionCheck()
		time.Sleep(ms)
	}
}

func (rf *Raft) resetElectionTimeoutLocked() {
	rf.deadline = time.Now().Add(randomElectionTimeout())
}

func (rf *Raft) beginElectionLocked() (RequestVoteArgs, int) {
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.persist()
	rf.resetElectionTimeoutLocked()
	myLastIndex := rf.lastLogIndexLocked()
	myLastTerm := rf.logTermLocked(myLastIndex)
	requester := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  myLastTerm,
		LastLogIndex: myLastIndex,
	}
	return requester, rf.currentTerm
}

func (rf *Raft) sendRequestVotes(args RequestVoteArgs, term int) {
	supporters := 1
	majority := len(rf.peers)/2 + 1
	// 单点
	if supporters >= majority {
		becameLeader := false

		rf.mu.Lock()
		if rf.role == CANDIDATE &&
			rf.currentTerm == term {

			rf.role = LEADER
			rf.leaderInit()
			becameLeader = true
		}
		rf.mu.Unlock()

		if becameLeader {
			rf.signalReplication()
		}
		return
	}

	//多点
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(peer, &args, &reply) {
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.role = FOLLOWER
				rf.persist()
				rf.resetElectionTimeoutLocked()
				rf.mu.Unlock()
				return
			}
			if rf.role != CANDIDATE || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				supporters++
				if supporters >= majority {
					rf.role = LEADER
					rf.leaderInit()
					rf.mu.Unlock()
					rf.signalReplication()
				} else {
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Unlock()
			}
		}(peer)
	}
}
func (rf *Raft) leaderInit() {
	lastLogIndex := rf.lastLogIndexLocked()
	for peer := range rf.peers {
		rf.nextIndex[peer] = lastLogIndex + 1
		rf.matchIndex[peer] = 0
	}

	rf.matchIndex[rf.me] = lastLogIndex
	rf.nextIndex[rf.me] = lastLogIndex + 1
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.resetElectionTimeoutLocked()

	rf.currentTerm = 0
	rf.role = FOLLOWER
	rf.log = []LogEntry{{0, 0}}
	rf.voteFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.replicateCh = make([]chan struct{}, len(rf.peers))
	for peer := range rf.peers {
		if peer != rf.me {
			rf.replicateCh[peer] = make(chan struct{}, 1)
		}
	}

	// initialize from state persisted before a crash
	rf.snapshot = persister.ReadSnapshot()
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.logOffset
	rf.lastApplied = rf.logOffset
	//rf.leaderIdx = -1
	// start ticker goroutine to start elections
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.replicationWorker(peer)
		}
	}
	go rf.ticker()
	go rf.startHeartbeat()
	go rf.applier()
	return rf
}
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		if rf.pendingSnapshotValid {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      append([]byte(nil), rf.pendingSnapshot...),
				SnapshotTerm:  rf.pendingSnapshotTerm,
				SnapshotIndex: rf.pendingSnapshotIndex,
			}
			rf.pendingSnapshotValid = false
			rf.pendingSnapshot = nil
			rf.mu.Unlock()

			rf.applyCh <- msg
		} else if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			index := rf.lastApplied
			entry := rf.log[rf.logSliceIndexLocked(index)]

			rf.mu.Unlock()

			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: index,
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}
func (rf *Raft) beginHeartbeat(peer int) (AppendEntryArgs, int) {
	next := rf.nextIndex[peer]

	prevLogIndex := next - 1
	prevLogTerm := rf.logTermLocked(prevLogIndex)
	entries := append(
		[]LogEntry(nil),
		rf.log[rf.logSliceIndexLocked(next):]...,
	)
	leaderCommit := rf.commitIndex
	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	return args, rf.currentTerm
}

func (rf *Raft) beginInstallSnapshot() (InstallSnapshotArgs, int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.logOffset,
		LastIncludedTerm:  rf.logTermLocked(rf.logOffset),
		Data:              append([]byte(nil), rf.snapshot...),
	}
	return args, rf.currentTerm
}

func (rf *Raft) signalReplication() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		select {
		case rf.replicateCh[peer] <- struct{}{}:
		default:
		}
	}
}

func (rf *Raft) replicationWorker(peer int) {
	type appendResult struct {
		reply AppendEntryReply
		ok    bool
	}
	type snapshotResult struct {
		reply InstallSnapshotReply
		ok    bool
	}

	for range rf.replicateCh[peer] {
		for {
			rf.mu.Lock()
			if rf.role != LEADER {
				rf.mu.Unlock()
				break
			}

			if rf.nextIndex[peer] <= rf.logOffset {
				args, term := rf.beginInstallSnapshot()
				rf.mu.Unlock()

				resultCh := make(chan snapshotResult, 1)
				go func(args InstallSnapshotArgs) {
					var reply InstallSnapshotReply
					ok := rf.sendInstallSnapshot(peer, &args, &reply)
					resultCh <- snapshotResult{reply: reply, ok: ok}
				}(args)

				var result snapshotResult
				timedOut := false
				select {
				case result = <-resultCh:
				case <-time.After(replicationRPCTimeout):
					timedOut = true
				}
				if timedOut || !result.ok {
					break
				}

				rf.mu.Lock()
				if result.reply.Term > rf.currentTerm {
					rf.currentTerm = result.reply.Term
					rf.role = FOLLOWER
					rf.voteFor = -1
					rf.persist()
					rf.resetElectionTimeoutLocked()
					rf.mu.Unlock()
					break
				}
				if rf.role != LEADER || rf.currentTerm != term {
					rf.mu.Unlock()
					break
				}

				if args.LastIncludedIndex > rf.matchIndex[peer] {
					rf.matchIndex[peer] = args.LastIncludedIndex
				}
				if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
					rf.nextIndex[peer] = args.LastIncludedIndex + 1
				}
				rf.mu.Unlock()
				continue
			}

			args, term := rf.beginHeartbeat(peer)
			sentNextIndex := args.PrevLogIndex + 1
			rf.mu.Unlock()

			resultCh := make(chan appendResult, 1)
			go func(args AppendEntryArgs) {
				var reply AppendEntryReply
				ok := rf.sendAppendEntry(peer, &args, &reply)
				resultCh <- appendResult{reply: reply, ok: ok}
			}(args)

			var result appendResult
			timedOut := false
			select {
			case result = <-resultCh:
			case <-time.After(replicationRPCTimeout):
				timedOut = true
			}
			if timedOut || !result.ok {
				break
			}
			reply := result.reply

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = FOLLOWER
				rf.voteFor = -1
				rf.persist()
				rf.resetElectionTimeoutLocked()
				rf.mu.Unlock()
				break
			}

			if rf.role != LEADER || rf.currentTerm != term {
				rf.mu.Unlock()
				break
			}

			if reply.Success {
				matched := args.PrevLogIndex + len(args.Entries)
				if matched > rf.matchIndex[peer] {
					rf.matchIndex[peer] = matched
				}
				if matched+1 > rf.nextIndex[peer] {
					rf.nextIndex[peer] = matched + 1
				}
				rf.pushForwardLocked()

				rf.mu.Unlock()
				break
			}

			if rf.nextIndex[peer] != sentNextIndex ||
				rf.nextIndex[peer] <= rf.matchIndex[peer]+1 {
				rf.mu.Unlock()
				break
			}
			if reply.XTerm == -1 {
				rf.nextIndex[peer] = reply.XLen
			} else {
				lastIndex := -1
				for index := rf.lastLogIndexLocked(); index >= rf.logOffset; index-- {
					if rf.logTermLocked(index) == reply.XTerm {
						lastIndex = index
						break
					}
				}

				if lastIndex >= 0 {
					rf.nextIndex[peer] = lastIndex + 1
				} else {
					rf.nextIndex[peer] = reply.XIndex
					//
				}
			}

			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) pushForwardLocked() {
	majority := len(rf.peers)/2 + 1

	for N := rf.lastLogIndexLocked(); N > rf.commitIndex; N-- {

		if rf.logTermLocked(N) != rf.currentTerm {
			continue
		}

		accepted := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= N {
				accepted++
			}
		}

		if accepted >= majority {
			rf.commitIndex = N
			break
		}
	}
}

func (rf *Raft) startHeartbeat() {
	for {
		shouldSend := false

		rf.mu.Lock()

		if rf.role == LEADER {
			rf.pushForwardLocked()

			shouldSend = true
		}

		rf.mu.Unlock()

		if shouldSend {
			rf.signalReplication()
		}

		time.Sleep(heartbeatInterval)
	}
}
