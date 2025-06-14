package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	lastLeaderId int // 记录上次成功的服务器 id
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, lastLeaderId: 0}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	getArgs := &rpc.GetArgs{Key: key}
	serverId := ck.lastLeaderId // 从上次成功的服务器开始尝试
	timeout := time.After(1 * time.Second)
	for {
		select {
		case <-timeout:
			return "", 0, rpc.ErrWrongGroup // 超时返回错误
		default:
			for i := 0; i < len(ck.servers); i++ {
				server := ck.servers[serverId]
				var getReply rpc.GetReply
				if ok := ck.clnt.Call(server, "KVServer.Get", getArgs, &getReply); ok {
					if getReply.Err == rpc.OK {
						ck.lastLeaderId = serverId
						return getReply.Value, getReply.Version, rpc.OK // 成功获取值
					} else if getReply.Err == rpc.ErrNoKey {
						ck.lastLeaderId = serverId // 更新上次成功的服务器 id
						return "", 0, rpc.ErrNoKey // 键不存在
					} else if getReply.Err == rpc.ErrWrongGroup {
						return "", 0, rpc.ErrWrongGroup // 键不属于当前组，让调用者更新配置
					} else if getReply.Err == rpc.ErrWrongLeader {
						// 如果是 ErrWrongLeader，继续尝试下一个服务器
					}
				}
				// 执行到这说明，RPC调用失败或者返回了 ErrWrongLeader
				serverId = (serverId + 1) % len(ck.servers) // 循环尝试下一个服务器
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	putArgs := &rpc.PutArgs{Key: key, Value: value, Version: version}
	firstAttempt := true        // 标记是否是第一次尝试
	serverId := ck.lastLeaderId // 从上次成功的服务器开始尝试
	timeout := time.After(1 * time.Second)
	for {
		select {
		case <-timeout:
			return rpc.ErrWrongGroup // 超时返回错误
		default:
			for i := 0; i < len(ck.servers); i++ {
				server := ck.servers[serverId]
				var putReply rpc.PutReply
				if ok := ck.clnt.Call(server, "KVServer.Put", putArgs, &putReply); ok {
					if putReply.Err == rpc.OK {
						ck.lastLeaderId = serverId
						return rpc.OK // 成功更新值
					} else if putReply.Err == rpc.ErrNoKey {
						ck.lastLeaderId = serverId // 更新上次成功的服务器 id
						return rpc.ErrNoKey        // 键不存在
					} else if putReply.Err == rpc.ErrVersion {
						if firstAttempt {
							return rpc.ErrVersion // 第一次尝试就返回 ErrVersion
						}
						return rpc.ErrMaybe // 重试时返回 ErrMaybe
					} else if putReply.Err == rpc.ErrWrongGroup {
						return rpc.ErrWrongGroup // 键不属于当前组
					} else if putReply.Err == rpc.ErrWrongLeader {
						// 如果是 ErrWrongLeader，继续尝试下一个服务器
					}
				}
				// 执行到这说明，RPC调用失败或者返回了 ErrWrongLeader
				firstAttempt = false                        // 标记为非第一次尝试
				serverId = (serverId + 1) % len(ck.servers) // 循环尝试下一个服务器
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	shardArgs := &shardrpc.FreezeShardArgs{Shard: s, Num: num}
	serverId := ck.lastLeaderId
	timeout := time.After(200 * time.Millisecond)
	for {
		select {
		case <-timeout:
			return nil, rpc.ErrWrongGroup // 超时返回错误
		default:
			for i := 0; i < len(ck.servers); i++ {
				server := ck.servers[serverId]
				var shardReply shardrpc.FreezeShardReply
				if ok := ck.clnt.Call(server, "KVServer.FreezeShard", shardArgs, &shardReply); ok {
					if shardReply.Err == rpc.OK {
						ck.lastLeaderId = serverId
						return shardReply.State, rpc.OK // 成功冻结分片
					} else if shardReply.Err == rpc.ErrWrongGroup {
						return nil, rpc.ErrWrongGroup // 分片不属于当前组
					} else if shardReply.Err == rpc.ErrWrongLeader {
						// 如果是 ErrWrongLeader，继续尝试下一个服务器
					}
				}
				// 执行到这说明，RPC调用失败或者返回了 ErrWrongLeader
				serverId = (serverId + 1) % len(ck.servers) // 循环尝试下一个服务器
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	installArgs := &shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	serverId := ck.lastLeaderId
	timeout := time.After(200 * time.Millisecond)
	for {
		select {
		case <-timeout:
			return rpc.ErrWrongGroup // 超时返回错误
		default:
			for i := 0; i < len(ck.servers); i++ {
				server := ck.servers[serverId]
				var installReply shardrpc.InstallShardReply
				if ok := ck.clnt.Call(server, "KVServer.InstallShard", installArgs, &installReply); ok {
					if installReply.Err == rpc.OK {
						ck.lastLeaderId = serverId
						return rpc.OK // 成功安装分片
					} else if installReply.Err == rpc.ErrWrongGroup {
						return rpc.ErrWrongGroup // 分片不属于当前组
					} else if installReply.Err == rpc.ErrWrongLeader {
						// 如果是 ErrWrongLeader，继续尝试下一个服务器
					}
				}
				// 执行到这说明，RPC调用失败或者返回了 ErrWrongLeader
				serverId = (serverId + 1) % len(ck.servers) // 循环尝试下一个服务器
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	deleteArgs := &shardrpc.DeleteShardArgs{Shard: s, Num: num}
	serverId := ck.lastLeaderId
	timeout := time.After(200 * time.Millisecond)
	for {
		select {
		case <-timeout:
			return rpc.ErrWrongGroup // 超时返回错误
		default:
			for i := 0; i < len(ck.servers); i++ {
				server := ck.servers[serverId]
				var deleteReply shardrpc.DeleteShardReply
				if ok := ck.clnt.Call(server, "KVServer.DeleteShard", deleteArgs, &deleteReply); ok {
					if deleteReply.Err == rpc.OK {
						ck.lastLeaderId = serverId
						return rpc.OK // 成功删除分片
					} else if deleteReply.Err == rpc.ErrWrongGroup {
						return rpc.ErrWrongGroup // 分片不属于当前组
					} else if deleteReply.Err == rpc.ErrWrongLeader {
						// 如果是 ErrWrongLeader，继续尝试下一个服务器
					}
				}
				// 执行到这说明，RPC调用失败或者返回了 ErrWrongLeader
				serverId = (serverId + 1) % len(ck.servers) // 循环尝试下一个服务器
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
