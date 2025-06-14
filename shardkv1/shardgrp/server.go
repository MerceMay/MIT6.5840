package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type KVPair struct {
	Value   string
	Version rpc.Tversion
}

// 每个分片的信息
type ShardInfo struct {
	ConfigNum shardcfg.Tnum // 分片配置号
	Frozen    bool          // 是否冻结
	Owned     bool          // 是否拥有
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu        sync.RWMutex                 // 保护shardInfo locks
	shardInfo [shardcfg.NShards]*ShardInfo // 分片信息
	store     map[string]KVPair            // 存储键值对
	locks     map[string]*sync.RWMutex     // 键值对锁
}

func (kv *KVServer) shardOwned(shard shardcfg.Tshid) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if shard < 0 || shard >= shardcfg.NShards {
		return false
	}
	return kv.shardInfo[shard].Owned
}

func (kv *KVServer) shardFrozen(shard shardcfg.Tshid) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if shard < 0 || shard >= shardcfg.NShards {
		return false
	}
	return kv.shardInfo[shard].Frozen
}

func (kv *KVServer) getLock(key string) *sync.RWMutex {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if lock, ok := kv.locks[key]; ok {
		return lock
	}
	lock := &sync.RWMutex{}
	kv.locks[key] = lock
	return lock
}

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
	case *shardrpc.FreezeShardArgs:
		return kv.doFreezeShard(r)
	case shardrpc.FreezeShardArgs:
		return kv.doFreezeShard(&r)
	case *shardrpc.InstallShardArgs:
		return kv.doInstallShard(r)
	case shardrpc.InstallShardArgs:
		return kv.doInstallShard(&r)
	case *shardrpc.DeleteShardArgs:
		return kv.doDeleteShard(r)
	case shardrpc.DeleteShardArgs:
		return kv.doDeleteShard(&r)
	default:
		panic("unexpected request type")
	}
}

func (kv *KVServer) doPut(args *rpc.PutArgs) *rpc.PutReply {
	if kv.killed() {
		return &rpc.PutReply{Err: rpc.ErrWrongLeader}
	}
	shard := shardcfg.Key2Shard(args.Key)
	if !kv.shardOwned(shard) {
		return &rpc.PutReply{Err: rpc.ErrWrongGroup}
	}
	if kv.shardFrozen(shard) {
		return &rpc.PutReply{Err: rpc.ErrWrongGroup}
	}

	keyLock := kv.getLock(args.Key)
	keyLock.Lock()
	defer keyLock.Unlock()

	if pair, ok := kv.store[args.Key]; ok {
		if args.Version == pair.Version {
			kv.store[args.Key] = KVPair{Value: args.Value, Version: args.Version + 1}
			return &rpc.PutReply{Err: rpc.OK}
		} else {
			return &rpc.PutReply{Err: rpc.ErrVersion}
		}
	} else {
		if args.Version == 0 {
			kv.store[args.Key] = KVPair{Value: args.Value, Version: 1}
			return &rpc.PutReply{Err: rpc.OK}
		} else {
			return &rpc.PutReply{Err: rpc.ErrVersion}
		}
	}
}

func (kv *KVServer) doGet(args *rpc.GetArgs) *rpc.GetReply {
	if kv.killed() {
		return &rpc.GetReply{Err: rpc.ErrWrongLeader}
	}
	shard := shardcfg.Key2Shard(args.Key)
	if !kv.shardOwned(shard) {
		return &rpc.GetReply{Err: rpc.ErrWrongGroup}
	}
	if kv.shardFrozen(shard) {
		return &rpc.GetReply{Err: rpc.ErrWrongGroup}
	}

	keyLock := kv.getLock(args.Key)
	keyLock.RLock()
	defer keyLock.RUnlock()

	if pair, ok := kv.store[args.Key]; ok {
		return &rpc.GetReply{Value: pair.Value, Version: pair.Version, Err: rpc.OK}
	} else {
		return &rpc.GetReply{Err: rpc.ErrNoKey}
	}
}

func (kv *KVServer) serializeShardData(shard shardcfg.Tshid) []byte {
	shardData := make(map[string]KVPair)
	for key, pair := range kv.store {
		if shardcfg.Key2Shard(key) == shard {
			keyLock := kv.getLock(key)
			keyLock.RLock()
			shardData[key] = pair
			keyLock.RUnlock()
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shardData)
	return w.Bytes()
}

func (kv *KVServer) deserializeShardData(data []byte) map[string]KVPair {
	if len(data) == 0 {
		return make(map[string]KVPair)
	}
	r := bytes.NewBuffer(data)
	e := labgob.NewDecoder(r)
	var shardData map[string]KVPair
	if e.Decode(&shardData) != nil {
		panic("failed to decode shard data")
	}
	return shardData
}

func (kv *KVServer) doFreezeShard(args *shardrpc.FreezeShardArgs) *shardrpc.FreezeShardReply {
	if kv.killed() {
		return &shardrpc.FreezeShardReply{Err: rpc.ErrWrongLeader}
	}
	shard := args.Shard
	kv.mu.Lock()
	configNum := kv.shardInfo[shard].ConfigNum
	if configNum > args.Num {
		// 过期的冻结请求，返回OK和配置号
		kv.mu.Unlock()
		return &shardrpc.FreezeShardReply{Err: rpc.ErrWrongGroup}
	}
	if configNum == args.Num && kv.shardInfo[shard].Frozen {
		// 重复的冻结请求，幂等性操作，直接返回当前状态
		kv.mu.Unlock()
		shardData := kv.serializeShardData(shard)
		return &shardrpc.FreezeShardReply{Num: configNum, State: shardData, Err: rpc.OK}
	}
	kv.shardInfo[shard].ConfigNum = args.Num
	kv.shardInfo[shard].Frozen = true
	kv.mu.Unlock()
	// currentShardInfo.Owned 保持不变，它只在 InstallShard 和 DeleteShard 中改变
	shardData := kv.serializeShardData(shard)
	return &shardrpc.FreezeShardReply{Num: configNum, State: shardData, Err: rpc.OK}
}

func (kv *KVServer) doInstallShard(args *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {
	if kv.killed() {
		return &shardrpc.InstallShardReply{Err: rpc.ErrWrongLeader}
	}
	shard := args.Shard
	kv.mu.Lock()
	configNum := kv.shardInfo[shard].ConfigNum
	if configNum > args.Num {
		// 过期的安装请求，返回OK和配置号
		kv.mu.Unlock()
		return &shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
	}
	installData := kv.deserializeShardData(args.State)
	kv.shardInfo[shard].ConfigNum = args.Num
	kv.shardInfo[shard].Frozen = false
	kv.shardInfo[shard].Owned = true
	kv.mu.Unlock()
	for key, pair := range installData {
		keyLock := kv.getLock(key)
		keyLock.Lock()
		// if existingPair, exists := kv.store[key]; exists {
		// 	if pair.Version > existingPair.Version {
		// 		kv.store[key] = pair // 只在版本更高时更新
		// 	}
		// } else {
		// 	kv.store[key] = pair // 新键值对直接添加
		// }
		kv.store[key] = pair
		keyLock.Unlock()
	}
	return &shardrpc.InstallShardReply{Err: rpc.OK}
}

func (kv *KVServer) doDeleteShard(args *shardrpc.DeleteShardArgs) *shardrpc.DeleteShardReply {
	if kv.killed() {
		return &shardrpc.DeleteShardReply{Err: rpc.ErrWrongLeader}
	}
	shard := args.Shard
	kv.mu.Lock()
	configNum := kv.shardInfo[shard].ConfigNum
	if configNum > args.Num {
		// 过期的删除请求，返回OK和配置号
		kv.mu.Unlock()
		return &shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
	}
	kv.shardInfo[shard].ConfigNum = args.Num
	kv.shardInfo[shard].Frozen = false
	kv.shardInfo[shard].Owned = false
	kv.mu.Unlock()
	// 删除分片数据
	keysToDelete := []string{}
	for key := range kv.store {
		if shardcfg.Key2Shard(key) == shard {
			keysToDelete = append(keysToDelete, key)
		}
	}
	for _, key := range keysToDelete {
		keyLock := kv.getLock(key)
		keyLock.Lock()
		delete(kv.store, key)
		keyLock.Unlock()
		// delete(kv.locks, key) // 删除锁
	}
	return &shardrpc.DeleteShardReply{Err: rpc.OK}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.killed() {
		return nil
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shardInfo)
	e.Encode(kv.store)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.killed() {
		return
	}
	if len(data) == 0 {
		kv.shardInfo = [shardcfg.NShards]*ShardInfo{}
		kv.store = make(map[string]KVPair)
		return
	}
	r := bytes.NewBuffer(data)
	e := labgob.NewDecoder(r)
	var shardInfo [shardcfg.NShards]*ShardInfo
	var store map[string]KVPair
	if e.Decode(&shardInfo) != nil || e.Decode(&store) != nil {
		panic("failed to decode snapshot data")
	}
	kv.shardInfo = shardInfo
	kv.store = store
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
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
	// Your code here
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, rep := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	freezeReply, ok := rep.(*shardrpc.FreezeShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = *freezeReply
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, rep := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	installReply, ok := rep.(*shardrpc.InstallShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = *installReply
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, rep := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	deleteReply, ok := rep.(*shardrpc.DeleteShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = *deleteReply
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:       gid,
		me:        me,
		store:     make(map[string]KVPair),
		locks:     make(map[string]*sync.RWMutex),
		shardInfo: [shardcfg.NShards]*ShardInfo{},
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	if gid == shardcfg.Gid1 {
		for i := range shardcfg.NShards {
			kv.shardInfo[i] = &ShardInfo{
				ConfigNum: shardcfg.NumFirst,
				Frozen:    false,
				Owned:     true,
			}
		}
	} else {
		for i := range shardcfg.NShards {
			kv.shardInfo[i] = &ShardInfo{
				ConfigNum: shardcfg.NumFirst - 1, // 表示没有配置
				Frozen:    false,
				Owned:     false,
			}
		}
	}
	return []tester.IService{kv, kv.rsm.Raft()}
}
