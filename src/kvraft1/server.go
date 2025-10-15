package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu sync.Mutex
	lastAppliedIndex int
	kv map[string]string
	version map[string]rpc.Tversion
}

func (kv *KVServer) doGet(req *rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := rpc.GetReply{}
	DPrintf("KVServer %d Get request: %v\n", kv.me, req)
	if val, ok := kv.kv[req.Key]; ok {
		reply.Value = val
		reply.Version = kv.version[req.Key]
		reply.Err = rpc.OK
	} else {
		reply.Value = ""
		reply.Version = 0
		reply.Err = rpc.ErrNoKey
	}
	return reply
}

func (kv *KVServer) doPut(req *rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := rpc.PutReply{}
	DPrintf("KVServer %d Put request: %v\n", kv.me, req)
	curVer, ok := kv.version[req.Key]
	if !ok {
		DPrintf("KVServer %d Put new key %s\n", kv.me, req.Key)
		if req.Version != 0 {
			DPrintf("KVServer %d Put version mismatch for new key %s: req.Version=%d\n", kv.me, req.Key, req.Version)
			reply.Err = rpc.ErrVersion
			return reply
		}
		DPrintf("KVServer %d Put new key %s success\n", kv.me, req.Key)
		kv.kv[req.Key] = req.Value
		kv.version[req.Key] = 1
		reply.Err = rpc.OK
	} else {
		DPrintf("KVServer %d Put existing key %s current version %d\n", kv.me, req.Key, curVer)
		if req.Version != curVer {
			DPrintf("KVServer %d Put version mismatch for key %s: req.Version=%d curVer=%d\n", kv.me, req.Key, req.Version, curVer)
			reply.Err = rpc.ErrVersion
			return reply
		}
		DPrintf("KVServer %d Put existing key %s success\n", kv.me, req.Key)
		kv.kv[req.Key] = req.Value
		kv.version[req.Key]++
		reply.Err = rpc.OK
	}
	return reply
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch req := req.(type) {
	case *rpc.GetArgs:
		return kv.doGet(req)
	case rpc.GetArgs:
		return kv.doGet(&req)
	case *rpc.PutArgs:
		return kv.doPut(req)
	case rpc.PutArgs:
		return kv.doPut(&req)
	default:
		panic(fmt.Sprintf("unknown request type: %v", req))
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, rep := kv.rsm.Submit(args)
	reply.Err = err
	if err == rpc.OK {
		getRep := rep.(rpc.GetReply)
		reply.Err = getRep.Err
		reply.Value = getRep.Value
		reply.Version = getRep.Version
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	err, rep := kv.rsm.Submit(args)
	if err != rpc.OK {
        reply.Err = err
        return
    }
    reply.Err = rep.(rpc.PutReply).Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}
	kv.kv = make(map[string]string)
	kv.version = make(map[string]rpc.Tversion)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
