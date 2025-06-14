package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 每个键（key）对应一个 (value, version) 元组
type KVPair struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]KVPair // key -> (value, version)
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		store: make(map[string]KVPair),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 检查 args.Key 是否存在
	if pair, ok := kv.store[args.Key]; ok {
		reply.Value = pair.value
		reply.Version = pair.version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 检查 args.Key 是否存在
	if pair, ok := kv.store[args.Key]; ok {
		// 如果存在，检查版本号
		if args.Version == pair.version {
			// 版本匹配，更新值和版本号
			kv.store[args.Key] = KVPair{value: args.Value, version: pair.version + 1}
			reply.Err = rpc.OK
		} else {
			// 版本不匹配
			reply.Err = rpc.ErrVersion
		}
	} else {
		// 如果不存在，检查版本号
		if args.Version == 0 {
			// 安装新值
			kv.store[args.Key] = KVPair{value: args.Value, version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
