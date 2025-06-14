package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVPair struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu    sync.Mutex        // 用于保护 store 的并发访问
	store map[string]KVPair // key -> (value, version)
	locks map[string]*sync.RWMutex
}

// 获取特定键的锁，如果不存在则创建
func (kv *KVServer) getLock(key string) *sync.RWMutex {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exists := kv.locks[key]; !exists {
		kv.locks[key] = &sync.RWMutex{}
	}
	return kv.locks[key]
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	// 根据请求类型确定处理函数
	switch r := req.(type) {
	case *rpc.PutArgs:
		return kv.doPut(r)
	case rpc.PutArgs:
		return kv.doPut(&r)
	case *rpc.GetArgs:
		return kv.doGet(r)
	case rpc.GetArgs:
		return kv.doGet(&r)
	default:
		panic("unexpected request type")
	}
}

func (kv *KVServer) doPut(args *rpc.PutArgs) *rpc.PutReply {
	if kv.killed() {
		return &rpc.PutReply{Err: rpc.ErrWrongLeader} // 如果服务器已被杀死，返回 ErrWrongLeader
	}

	keyLock := kv.getLock(args.Key)
	keyLock.Lock()
	defer keyLock.Unlock()

	// 检查args.Key是否存在
	if pair, ok := kv.store[args.Key]; ok {
		// 如果存在，检查版本号
		if args.Version == pair.Version {
			// 版本号匹配，更新值和版本
			kv.store[args.Key] = KVPair{Value: args.Value, Version: pair.Version + 1}
			return &rpc.PutReply{Err: rpc.OK}
		} else {
			// 版本号不匹配
			return &rpc.PutReply{Err: rpc.ErrVersion}
		}
	} else {
		// 如果不存在，检查版本号
		if args.Version == 0 {
			// 如果版本号为0，安装新键值对
			kv.store[args.Key] = KVPair{Value: args.Value, Version: 1}
			return &rpc.PutReply{Err: rpc.OK}
		} else {
			// 如果版本号不为0，返回 ErrNoKey
			return &rpc.PutReply{Err: rpc.ErrNoKey}
		}
	}
}

func (kv *KVServer) doGet(args *rpc.GetArgs) *rpc.GetReply {
	if kv.killed() {
		return &rpc.GetReply{Err: rpc.ErrWrongLeader} // 如果服务器已被杀死，返回 ErrWrongLeader
	}

	keyLock := kv.getLock(args.Key)
	keyLock.RLock()
	defer keyLock.RUnlock()

	// 检查args.Key是否存在
	if pair, ok := kv.store[args.Key]; ok {
		// 如果存在，返回值和版本
		return &rpc.GetReply{Value: pair.Value, Version: pair.Version, Err: rpc.OK}
	} else {
		// 如果不存在，返回 ErrNoKey
		return &rpc.GetReply{Err: rpc.ErrNoKey}
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock() // 确保在函数结束时解锁
	if kv.killed() {
		return nil // 如果服务器已被杀死，返回 nil
	}

	// 创建一个新的 labgob 编码器
	W := new(bytes.Buffer)
	e := labgob.NewEncoder(W)
	e.Encode(kv.store) // 编码当前的 store
	return W.Bytes()   // 返回编码后的字节切片
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock() // 确保在函数结束时解锁

	if kv.killed() {
		return // 如果服务器已被杀死，直接返回
	}

	if kv.store == nil {
		kv.store = make(map[string]KVPair) // 如果 store 为空，初始化它
	}

	if len(data) == 0 {
		kv.store = make(map[string]KVPair) // 如果没有数据，清空 store
		return                             // 如果没有数据，直接返回
	}

	// 创建一个新的 labgob 解码器
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var store map[string]KVPair
	if d.Decode(&store) != nil {
		// 解码失败，
		panic("Failed to decode KVServer state from snapshot data")
	}
	kv.store = store
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
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	getReply, ok := rep.(*rpc.GetReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = *getReply
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
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	putReply, ok := rep.(*rpc.PutReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = *putReply
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
	labgob.Register(KVPair{})
	labgob.Register(map[string]KVPair{})

	kv := &KVServer{me: me, mu: sync.Mutex{},
		store: make(map[string]KVPair),
		locks: make(map[string]*sync.RWMutex),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
