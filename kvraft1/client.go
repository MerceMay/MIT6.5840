package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	lastLeaderId int // 记录上次成功的服务器 id
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.lastLeaderId = 0
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	getArgs := &rpc.GetArgs{Key: key}
	serverId := ck.lastLeaderId // 从上次成功的服务器开始尝试
	for {
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
				} else if getReply.Err == rpc.ErrWrongLeader {
					// 如果是 ErrWrongLeader，继续尝试下一个服务器
				}
			}
			// 执行到这说明，RPC调用失败或者返回了 ErrWrongLeader
			serverId = (serverId + 1) % len(ck.servers) // 循环尝试下一个服务器
		}
		time.Sleep(500 * time.Millisecond) // 等待500ms再重试
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	putArgs := &rpc.PutArgs{Key: key, Value: value, Version: version}
	firstAttempt := true
	serverId := ck.lastLeaderId // 从上次成功的服务器开始尝试
	for {
		for i := 0; i < len(ck.servers); i++ {
			server := ck.servers[serverId]
			var putReply rpc.PutReply
			if ok := ck.clnt.Call(server, "KVServer.Put", putArgs, &putReply); ok {
				if putReply.Err == rpc.OK {
					ck.lastLeaderId = serverId
					return rpc.OK // 成功更新键值对
				} else if putReply.Err == rpc.ErrNoKey {
					ck.lastLeaderId = serverId
					return rpc.ErrNoKey // 键不存在
				} else if putReply.Err == rpc.ErrVersion {
					ck.lastLeaderId = serverId
					if firstAttempt {
						return rpc.ErrVersion // 第一次尝试收到 ErrVersion，直接返回
					} else {
						return rpc.ErrMaybe // 重试阶段收到 ErrVersion，返回 ErrMaybe
					}
				} else if putReply.Err == rpc.ErrWrongLeader {
					// 如果是 ErrWrongLeader，继续尝试下一个服务器
				}
			}
			firstAttempt = false // 之后都是重试阶段
			// 执行到这说明，RPC调用失败或者返回了 ErrWrongLeader
			serverId = (serverId + 1) % len(ck.servers) // 循环尝试下一个服务器
		}
		time.Sleep(500 * time.Millisecond) // 等待500ms再重试
	}
}
