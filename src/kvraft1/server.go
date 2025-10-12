package kvraft

import (
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
	kv map[string]string
	version map[string]rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch req := req.(type) {
	case *rpc.GetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply := rpc.GetReply{}
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
	case *rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply := rpc.PutReply{}
		if curVer, ok := kv.version[req.Key]; ok {
			if req.Version != curVer {
				reply.Err = rpc.ErrVersion
				return reply
			}
			// versions match, update the value and increment version
			kv.kv[req.Key] = req.Value
			kv.version[req.Key] = curVer + 1
			reply.Err = rpc.OK
			return reply
		} else {
			// key doesn't exist
			if req.Version != 0 {
				reply.Err = rpc.ErrNoKey
				return reply
			}
			// install the value and set version to 1
			kv.kv[req.Key] = req.Value
			kv.version[req.Key] = 1
			reply.Err = rpc.OK
			return reply
		}
	}
	return nil
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
	err, rep := kv.rsm.Submit(args)
	reply.Err = err
	if err == rpc.OK {
		getRep := rep.(rpc.GetReply)
		reply.Value = getRep.Value
		reply.Version = getRep.Version
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(args)
	reply.Err = err
	if err == rpc.OK {
		reply.Err = rep.(rpc.PutReply).Err
	}
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
