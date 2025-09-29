package rsm

import (
	// "log"
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int // client id
	Id  int // request id, unique for each client
	Req any // the actual request
}

type ReqChan struct {
	ch   chan any
	term int
	id   int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	waitingReq map[int]ReqChan // index -> chan to receive result
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waitingReq:   make(map[int]ReqChan),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.read()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) read() {
	for msg := range rsm.applyCh {
		rsm.mu.Lock()
		if msg.CommandValid {
			res := rsm.sm.DoOp(msg.Command.(Op).Req)
			if reqChan, ok := rsm.waitingReq[msg.CommandIndex]; ok {
				reqChan.ch <- res
			}
			// TODO: snapshot truncate
		} // else if msg.SnapshotValid {
			// rsm.sm.Restore(msg.Snapshot)
		// }
		rsm.mu.Unlock()
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	op := Op{Me: rsm.me, Id: rand.Int(), Req: req}
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan any, 1)
	// log.Printf("rsm: submit op: %v, term: %d, index: %d\n", op, term, index)
	rsm.waitingReq[index] = ReqChan{ch: ch, term: term, id: op.Id}
	rsm.mu.Unlock()

	select {
	case res := <-ch:
		rsm.mu.Lock()
		defer rsm.mu.Unlock()
		// check if still leader
		term, isLeader := rsm.rf.GetState()
		if !isLeader || term != rsm.waitingReq[index].term {
			return rpc.ErrWrongLeader, nil
		}
		if reqCh, ok := rsm.waitingReq[index]; ok && reqCh.id == op.Id {
			// log.Printf("rsm: submit ok, res: %v\n", res)
			delete(rsm.waitingReq, index)
			return rpc.OK, res
		} else {
			return rpc.ErrWrongLeader, nil
		}
	case <-time.After(2 * time.Second):
		rsm.mu.Lock()
		delete(rsm.waitingReq, index)
		defer rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
}
