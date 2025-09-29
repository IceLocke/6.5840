package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	// "fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type RaftState int
const (
	Follower RaftState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *tester.Persister   // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	state		  RaftState
	applyCh       chan raftapi.ApplyMsg
	applyChBuffer chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry

	// additional state
	heartbeat          bool
	votes              int

	// snapshot state
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
}

type PersistedState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func truncateAndCopyBefore(log []LogEntry, index int) []LogEntry {
	newLog := make([]LogEntry, index+1)
	copy(newLog, log[:index+1])
	return newLog
}

func truncateAndCopyAfter(log []LogEntry, index int) []LogEntry {
	newLog := make([]LogEntry, len(log)-index)
	copy(newLog[1:], log[index+1:])
	return newLog
}

func deepcopySnapshot(snapshot []byte) []byte {
	if snapshot == nil {
		return nil
	}
	newSnapshot := make([]byte, len(snapshot))
	copy(newSnapshot, snapshot)
	return newSnapshot
}

// convert global index to real index in rf.log
func (rf *Raft) realIdx(i int) int {
	return i - rf.lastIncludedIndex
}

// convert real index in rf.log to global index
func (rf *Raft) globalIdx(i int) int {
	return i + rf.lastIncludedIndex
}

func (rf *Raft) prtLog() {
	for i := 0; i < len(rf.log); i++ {
		DPrintf("\t[%d](term=%d) log[%d] = (term=%d, cmd=%.8v)\n", rf.me, rf.currentTerm, rf.globalIdx(i), rf.log[i].Term, rf.log[i].Command)
	}
}

//
//// RequestVoteRPC
//

type RequestVoteArgs struct {
	Term         int
	CandidateId int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// DPrintf("[%d](term=%d) received RequestVote from [%d](term=%d)\n", rf.me, rf.currentTerm, args.CandidiateId, args.Term)
	defer rf.mu.Unlock()

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// term T > currentTerm, set currentTerm = T, convert to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}
	if rf.votedFor != -1 {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// if votedFor is null or candidateId
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := rf.log[len(rf.log)-1].Term
		// need to convert to global index
		lastLogIndex := rf.globalIdx(len(rf.log) - 1)
		// at least as up-to-date as receiver's log
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.heartbeat = true
			rf.persist()
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			DPrintf("[%d](term=%d) voted for [%d](term=%d)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	}
}

func (rf *Raft) callSendRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.VoteGranted {
			rf.votes += 1
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.persist()
		}
	}
}

//
//// AppendEntriesRPC
//

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.lastIncludedIndex > args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= rf.globalIdx(len(rf.log)) || rf.log[rf.realIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		if args.PrevLogIndex >= rf.globalIdx(len(rf.log)) {
			// Case 1: Follower's log is too short.
			reply.ConflictIndex = rf.globalIdx(len(rf.log))
			reply.ConflictTerm = -1 // Sentinel value indicates log is too short
		} else {
			// Case 2: Term mismatch.
			reply.ConflictTerm = rf.log[rf.realIdx(args.PrevLogIndex)].Term
			// Find the first index of the conflicting term.
			firstIndex := args.PrevLogIndex
			for firstIndex > rf.lastIncludedIndex && rf.log[rf.realIdx(firstIndex-1)].Term == reply.ConflictTerm {
				firstIndex--
			}
			reply.ConflictIndex = firstIndex
		}
		return
	}

	// if an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	for i, entry := range args.Entries {
		if args.PrevLogIndex+1+i < rf.globalIdx(len(rf.log)) {
			// if conflict, delete the existing entry and all that follow it
			if rf.log[rf.realIdx(args.PrevLogIndex+1+i)].Term != entry.Term {
				// avoid memory leak
				rf.log = truncateAndCopyBefore(rf.log, rf.realIdx(args.PrevLogIndex+i))
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				DPrintf("[%d](term=%d) delete log at index %d and after due to conflict, last appended index %d\n", rf.me, rf.currentTerm, args.PrevLogIndex+i, rf.globalIdx(len(rf.log))-1)
				break
			}
		}
		// append any new entries not already in the log
		if args.PrevLogIndex+1+i >= rf.globalIdx(len(rf.log)) {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			DPrintf("[%d](term=%d) append new log entries at index %d and after, last appended index %d\n", rf.me, rf.currentTerm, args.PrevLogIndex+1+i, rf.globalIdx(len(rf.log))-1)
			break
		}
	}

	// if leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		DPrintf("[%d](term=%d) update commitIndex %d -> %d\n", rf.me, rf.currentTerm, rf.commitIndex, min(args.LeaderCommit, lastNewEntryIndex))
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
	}
	reply.Success = true
	reply.Term = args.Term
	// accept the AppendEntries RPC
	// reset election timeout and convert to follower
	rf.currentTerm = args.Term
	rf.heartbeat = true
	rf.state = Follower
	rf.persist()
}

//
//// InstallSnapshotRPC
///

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// accept the InstallSnapshot RPC
	DPrintf("[%d](term=%d) received InstallSnapshot from [%d](term=%d) lastIncludedIndex=%d lastIncludedTerm=%d\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.persist()
	reply.Term = rf.currentTerm

	// no more new information
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	DPrintf("[%d](term=%d) install snapshot lastIncludedIndex %d -> %d, lastIncludedTerm %d -> %d\n", rf.me, rf.currentTerm, rf.lastIncludedIndex, args.LastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedTerm)
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	needApplySnap := rf.lastApplied <= args.LastIncludedIndex
	rf.snapshot = deepcopySnapshot(args.Data)
	msg := raftapi.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      deepcopySnapshot(rf.snapshot),
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// if existing log entry has same index and term as snapshot's
	if args.LastIncludedIndex > rf.lastIncludedIndex {
		if args.LastIncludedIndex < rf.globalIdx(len(rf.log)) && rf.log[rf.realIdx(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
			rf.log = truncateAndCopyAfter(rf.log, rf.realIdx(args.LastIncludedIndex))
			rf.log[0].Term = args.LastIncludedTerm
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.persist()
			if needApplySnap {
				go func() { rf.applyChBuffer <- msg }()
				rf.lastApplied = rf.lastIncludedIndex
			}
			DPrintf("[%d](term=%d) keep log after snapshot: ", rf.me, rf.currentTerm)
			rf.prtLog()
			return
		}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	// DPrintf("[%d](term=%d) discard log and keep only snapshot\n", rf.me, rf.currentTerm)
	rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	go func() { rf.applyChBuffer <- msg }()
	rf.lastApplied = rf.lastIncludedIndex
	rf.prtLog()
	rf.heartbeat = true
	rf.persist()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(
		PersistedState{
			CurrentTerm:       rf.currentTerm,
			VotedFor:          rf.votedFor,
			Log:               rf.log,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
		},
	)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state PersistedState

	if d.Decode(&state) != nil {
		DPrintf("error: decoding persisted state")
	} else {
		rf.currentTerm = state.CurrentTerm
		rf.votedFor = state.VotedFor
		rf.log = state.Log
		rf.lastIncludedIndex = state.LastIncludedIndex
		rf.lastIncludedTerm = state.LastIncludedTerm
		rf.log[0].Term = state.LastIncludedTerm
		rf.lastApplied = state.LastIncludedIndex
		rf.commitIndex = state.LastIncludedIndex
		rf.snapshot = rf.persister.ReadSnapshot()
	}
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
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.lastIncludedIndex && index <= rf.lastApplied {
		rf.lastIncludedTerm = rf.log[rf.realIdx(index)].Term
		rf.log = truncateAndCopyAfter(rf.log, rf.realIdx(index))
		rf.lastIncludedIndex = index
		rf.log[0].Term = rf.lastIncludedTerm
		DPrintf("[%d](term=%d) create snapshot at index %d\n", rf.me, rf.currentTerm, index)
		rf.prtLog()
		rf.snapshot = deepcopySnapshot(snapshot)
		rf.persist()
		for i := range rf.peers {
			if i != rf.me && rf.nextIndex[i] <= rf.lastIncludedIndex {
				go rf.callSendInstallSnapshot(i)
			}
		}
	}
}

func (rf *Raft) callSendInstallSnapshot(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if rf.nextIndex[server] > rf.lastIncludedIndex {
			rf.mu.Unlock()
			return
		}
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              deepcopySnapshot(rf.snapshot),
		}
		rf.mu.Unlock()
		reply := &InstallSnapshotReply{}
		if ok := rf.sendInstallSnapshot(server, args, reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.persist()
				return
			}
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
			rf.matchIndex[server] = rf.lastIncludedIndex
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) checkCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		for n := rf.commitIndex + 1; n < rf.globalIdx(len(rf.log)); n++ {
			if rf.realIdx(n) < 0 || rf.log[rf.realIdx(n)].Term != rf.currentTerm {
				continue
			}
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				// DPrintf("[%d](term=%d) increase commitIndex %d -> %d\n", rf.me, rf.currentTerm, rf.commitIndex, n)
				rf.commitIndex = n
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index, term := rf.globalIdx(len(rf.log)), rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (3B).
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.persist()
		DPrintf("[%d](term=%d) received command %.8v, appended to log at index %d\n", rf.me, rf.currentTerm, command, index)
		go func() {}()
		for i := range rf.peers {
			if i != rf.me {
				go rf.callSendAppendEntries(i)
			}
		}
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the~
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) buildAppendEntriesArgs(server int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.log[rf.realIdx(rf.nextIndex[server]-1)].Term,
		Entries:      make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}
	for i := rf.nextIndex[server]; i < rf.globalIdx(len(rf.log)); i++ {
		args.Entries = append(args.Entries, rf.log[rf.realIdx(i)])
	}
	return args
}

func (rf *Raft) callSendAppendEntries(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if rf.nextIndex[server] <= rf.lastIncludedIndex {
			go rf.callSendInstallSnapshot(server)
			rf.mu.Unlock()
		} else {
			args := rf.buildAppendEntriesArgs(server)
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}
			if ok := rf.sendAppendEntries(server, args, reply); ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Follower
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					rf.mu.Unlock()
					return
				} else {
					// optimize by fast backoff
					if reply.ConflictTerm == -1 {
						// Follower's log is too short, jump to the end of its log
						rf.nextIndex[server] = reply.ConflictIndex
					} else {
						// Search for the last entry in leader's log with ConflictTerm
						lastIndexWithConflictTerm := -1
						for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
							if rf.realIdx(i-1) >= len(rf.log) || rf.realIdx(i-1) < 0 {
								continue
							}
							if rf.log[rf.realIdx(i-1)].Term == reply.ConflictTerm {
								lastIndexWithConflictTerm = i
								break
							}
						}
						if lastIndexWithConflictTerm != -1 {
							// If found, set nextIndex to the index after that entry
							rf.nextIndex[server] = lastIndexWithConflictTerm
						} else {
							// If not found, leader doesn't have ConflictTerm,
							// so jump to the first index of that term on the follower
							rf.nextIndex[server] = reply.ConflictIndex
						}
					}
					// Ensure nextIndex is at least 1
					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
					}
					rf.mu.Unlock()
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				go rf.callSendAppendEntries(i)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) startAsLeader() {
	rf.mu.Lock()
	DPrintf("[%d](term=%d) became leader", rf.me, rf.currentTerm)
	rf.state = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = rf.globalIdx(len(rf.log))
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	// heartbeat cycle
	go rf.sendHeartbeat()
	// check commit cycle
	go rf.checkCommit()
}

func (rf *Raft) checkElection() {
	// save the current term
	// if the term changes, indicating that either
	// a leader has been elected or a new election
	// has started, return
	// otherwise, if votes > n/2, become leader
	// else return
	rf.mu.Lock()
	electionTerm := rf.currentTerm
	rf.mu.Unlock()

	for !rf.killed() {
		// check if election timeout or a leader has been elected
		rf.mu.Lock()
		if electionTerm != rf.currentTerm || rf.state != Candidate {
			rf.mu.Unlock()
			return
		}
		// if won the election
		if rf.votes > len(rf.peers)/2 {
			rf.mu.Unlock()
			go rf.startAsLeader()
			break
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("[%d](term=%d) starts election", rf.me, rf.currentTerm+1)
	rf.votedFor = rf.me
	rf.currentTerm += 1
	rf.persist()
	rf.votes = 1
	rf.state = Candidate
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.globalIdx(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			go rf.callSendRequestVote(i, args)
		}
	}

	go rf.checkElection()
}

func (rf *Raft) sendApplyMsg() {
	for msg := range rf.applyChBuffer {
		if rf.killed() {
			close(rf.applyChBuffer)
			close(rf.applyCh)
			return
		}
		rf.applyCh <- msg
	}
}

// Started as a goroutine to apply committed entries to state machine
func (rf *Raft) applyCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			applyMsgs := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				if rf.realIdx(i) < 0 || rf.realIdx(i) >= len(rf.log) {
					continue
				}
				applyMsgs = append(applyMsgs, raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.realIdx(i)].Command,
					CommandIndex: i,
				})
			}
			rf.lastApplied = rf.commitIndex
			for _, msg := range applyMsgs {
				if rf.killed() {
					rf.mu.Unlock()
					return
				}
				DPrintf("[%d](term=%d) apply log at index %d: %.8v\n", rf.me, rf.currentTerm, msg.CommandIndex, msg.Command)
				rf.applyChBuffer <- msg
			}
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
    for !rf.killed() {
        timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond

        rf.mu.Lock()
        if rf.state == Leader {
            rf.mu.Unlock()
            time.Sleep(50 * time.Millisecond)
            continue
        }

        if rf.heartbeat {
            rf.heartbeat = false
            rf.mu.Unlock()
            time.Sleep(timeout)
            continue
        }
        rf.mu.Unlock()

        time.Sleep(timeout)

        rf.mu.Lock()
        if !rf.heartbeat && rf.state != Leader {
            rf.state = Candidate
            go rf.startElection()
        }
        rf.heartbeat = false
        rf.mu.Unlock()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.log = []LogEntry{{0, nil}} // 1-indexed
	rf.applyCh = applyCh
	rf.applyChBuffer = make(chan raftapi.ApplyMsg, 1024)
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// fmt.Printf("Initialized raft node %d\n", rf.me)
	go rf.ticker()
	go rf.applyCommit()
	go rf.sendApplyMsg()

	return rf
}
