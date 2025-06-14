package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
	config shardcfg.ShardConfig
	mu     sync.Mutex
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:   clnt,
		sck:    sck,
		config: shardcfg.ShardConfig{},
	}
	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		groupClerk := ck.getGroupClerk(key)
		value, version, err := groupClerk.Get(key)
		if err == rpc.ErrWrongGroup {
			ck.updateConfig() // 更新配置
			continue          // 重试获取
		}
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	for {
		groupClerk := ck.getGroupClerk(key)
		err := groupClerk.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			ck.updateConfig() // 更新配置
			continue          // 重试放置
		}
		return err
	}
}

func (ck *Clerk) updateConfig() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	newConfig := ck.sck.Query()
	if newConfig == nil {
		return
	}
	if newConfig.Num > ck.config.Num {
		ck.config = *newConfig
	}
}

func (ck *Clerk) getGroupClerk(key string) *shardgrp.Clerk {
	shard := shardcfg.Key2Shard(key)
	ck.mu.Lock()
	gid := ck.config.Shards[shard]
	servers := ck.config.Groups[gid]
	ck.mu.Unlock()
	return shardgrp.MakeClerk(ck.clnt, servers)
}
