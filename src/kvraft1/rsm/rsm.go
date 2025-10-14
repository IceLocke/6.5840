package rsm

import (
	// "log"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
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
	dead  	 	int32
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

func (rsm *RSM) Kill() {
	atomic.StoreInt32(&rsm.dead, 1)

    rsm.mu.Lock()
    defer rsm.mu.Unlock()
    for index, reqCh := range rsm.waitingReq {
        close(reqCh.ch)
        delete(rsm.waitingReq, index)
    }
}

func (rsm *RSM) killed() bool {
	z := atomic.LoadInt32(&rsm.dead)
	return z == 1
}

func (rsm *RSM) read() {
	for {
		msg, ok := <-rsm.applyCh
		DPrintf("RSM %d receive apply msg: %v\n", rsm.me, msg)
		if !ok {
			DPrintf("RSM %d killed\n", rsm.me)
			rsm.Kill()
			return
		}
		rsm.mu.Lock()
		if msg.CommandValid {
			res := rsm.sm.DoOp(msg.Command.(Op).Req)
			if reqChan, ok := rsm.waitingReq[msg.CommandIndex]; ok {
				DPrintf("RSM %d, find waiting req at index %d: %d\n", rsm.me, msg.CommandIndex, reqChan.id)
				term, isLeader := rsm.rf.GetState()
				if reqChan.term != term || !isLeader {
					DPrintf("RSM %d, req %d at index %d term %d no longer valid, current term %d isLeader %v\n", rsm.me, reqChan.id, msg.CommandIndex, reqChan.term, term, isLeader)
				} else {
					DPrintf("RSM %d, req %d at index %d term %d is valid, applied %v\n", rsm.me, reqChan.id, msg.CommandIndex, reqChan.term, msg.Command)
					reqChan.ch <- res
				}
			} else {
				DPrintf("RSM %d, no waiting req at index %d, applied %v\n", rsm.me, msg.CommandIndex, msg.Command)	
			}
			// TODO: snapshot truncate
		} // else if msg.SnapshotValid {
			// rsm.sm.Restore(msg.Snapshot)
		// }
		rsm.mu.Unlock()
	}
}

func (rsm *RSM) checkRequestValid(cancel context.CancelFunc, index int, term int, id int) {
	defer cancel()
	for {
		if rsm.killed() {
			// fmt.Printf("RSM %d canceled task %d[%d] due to shutdown\n", rsm.me, index, term)
			return
		}
		rsm.mu.Lock()
		// check if still leader
		curTerm, isLeader := rsm.rf.GetState()
		if !isLeader || curTerm != term {
			rsm.mu.Unlock()
			return
		}
		// check if request is still waiting
		if reqCh, ok := rsm.waitingReq[index]; ok && reqCh.id == id {
			if term != reqCh.term {
				rsm.mu.Unlock()
				return
			}
		}
		rsm.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	if rsm.killed() {
        return rpc.ErrWrongLeader, nil
    }
	// your code here
	rsm.mu.Lock()
	if rsm.killed() {
		return rpc.ErrWrongLeader, nil
	}
	op := Op{Me: rsm.me, Id: rand.Int(), Req: req}
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan any, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Second)
	go rsm.checkRequestValid(cancel, index, term, op.Id)
	DPrintf("RSM server %d, submit op %v at index %d term %d\n", rsm.me, op, index, term)
	if _, ok := rsm.waitingReq[index]; ok {
		return rpc.ErrWrongLeader, nil
	}

	rsm.waitingReq[index] = ReqChan{ch: ch, term: term, id: op.Id}
	rsm.mu.Unlock()

	select {
	case res, ok := <-ch:
		rsm.mu.Lock()
		defer rsm.mu.Unlock()
		if !ok || rsm.killed() {
            return rpc.ErrWrongLeader, nil
        }
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
	case <-ctx.Done():
		rsm.mu.Lock()
		defer rsm.mu.Unlock()
		delete(rsm.waitingReq, index)
		return rpc.ErrWrongLeader, nil
	}
}