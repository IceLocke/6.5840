package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int
	heartbeat   bool
	leaderId    int
	votedFor    int
	votes       int
	log         []LogEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

	// reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// if an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	for i, entry := range args.Entries {
		if args.PrevLogIndex+1+i < len(rf.log) {
			// if conflict, delete the existing entry and all that follow it
			if rf.log[args.PrevLogIndex+1+i].Term != entry.Term {
				rf.log = rf.log[:args.PrevLogIndex+1+i]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
		// append any new entries not already in the log
		if args.PrevLogIndex+1+i >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
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
	reply.Term = rf.currentTerm
	// accept the AppendEntries RPC
	// reset election timeout and convert to follower
	rf.heartbeat = true
	rf.leaderId = args.LeaderId
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.leaderId == rf.me
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidiateId int
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
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
		rf.leaderId = -1
	}
	if rf.votedFor != -1 {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// if votedFor is null or candidateId
	if rf.votedFor == -1 || rf.votedFor == args.CandidiateId {
		lastLogTerm := rf.log[len(rf.log)-1].Term
		lastLogIndex := len(rf.log) - 1
		// at least as up-to-date as receiver's log
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidiateId
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			DPrintf("[%d](term=%d) voted for [%d](term=%d)\n", rf.me, rf.currentTerm, args.CandidiateId, args.Term)
		}
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

func (rf *Raft) checkCommit() {
	for !rf.killed() && rf.isLeader() {
		rf.mu.Lock()
		for n := rf.commitIndex + 1; n < len(rf.log); n++ {
			if rf.log[n].Term != rf.currentTerm {
				continue
			}
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				if rf.log[n].Term == rf.currentTerm {
					rf.commitIndex = n
				}
				DPrintf("[%d](term=%d) commit log at index %d\n", rf.me, rf.currentTerm, n)
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
	index, term := len(rf.log), rf.currentTerm
	isLeader := rf.leaderId == rf.me

	// Your code here (3B).
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		DPrintf("[%d](term=%d) received command %.8v, appended to log at index %d\n", rf.me, rf.currentTerm, command, index)
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderId == rf.me
}

func (rf *Raft) noHeartbeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return !rf.heartbeat
}

func (rf *Raft) resetHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeat = false
}

func (rf *Raft) noVote() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor == -1
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
			rf.leaderId = -1
		}
	}
}

func (rf *Raft) buildAppendEntriesArgs(server int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		Entries:      make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}
	for i := rf.nextIndex[server]; i < len(rf.log); i++ {
		args.Entries = append(args.Entries, rf.log[i])
	}

	return args
}

func (rf *Raft) callSendAppendEntries(server int) {
	for !rf.killed() && rf.isLeader() {
		args := rf.buildAppendEntriesArgs(server)
		reply := &AppendEntriesReply{}
		if ok := rf.sendAppendEntries(server, args, reply); ok {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.leaderId = -1
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
				rf.nextIndex[server] = (rf.nextIndex[server] + 1) / 2
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
				rf.mu.Unlock()
			}
		}
		time.Sleep(35 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat() {
	for !rf.killed() && rf.isLeader() {
		for i := range rf.peers {
			if i != rf.me {
				go rf.callSendAppendEntries(i)
			}
		}
		time.Sleep(35 * time.Millisecond)
	}
}

func (rf *Raft) startAsLeader() {
	rf.mu.Lock()
	DPrintf("[%d](term=%d) became leader", rf.me, rf.currentTerm)
	rf.leaderId = rf.me
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
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
		if electionTerm != rf.currentTerm || rf.leaderId != -1 {
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
	// DPrintf("[%d](term=%d) starts election", rf.me, rf.currentTerm+1)
	rf.votes = 1
	rf.votedFor = rf.me
	rf.leaderId = -1
	rf.currentTerm += 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidiateId: rf.me,
		LastLogIndex: len(rf.log) - 1,
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

// Started as a goroutine to apply committed entries to state machine
func (rf *Raft) applyCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			applyMsgs := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsgs = append(applyMsgs, raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				})
			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
			for _, msg := range applyMsgs {
				DPrintf("[%d](term=%d) apply log at index %d: %.8v\n", rf.me, rf.currentTerm, msg.CommandIndex, msg.Command)
				rf.applyCh <- msg
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// For followers: if election timeout elapses without
		// - receiving AppendEntries RPCs from current leader i.e. no heartbeat
		// - granting vote to candidate
		// convert to candidate and start election
		// For candidates: if election timeout elapses without
		// - election won
		// - receiving AppendEntries RPCs from new leader
		// convert to candidate and start new election
		if !rf.isLeader() && (rf.noHeartbeat() || rf.noVote()) {
			go rf.startElection()
		}

		rf.resetHeartbeat()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.votedFor = -1
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// fmt.Printf("Initialized raft node %d\n", rf.me)
	go rf.ticker()
	go rf.applyCommit()

	return rf
}
