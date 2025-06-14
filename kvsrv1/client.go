package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	getArgs := &rpc.GetArgs{Key: key}
	for {
		var getReply rpc.GetReply
		if ok := ck.clnt.Call(ck.server, "KVServer.Get", getArgs, &getReply); ok {
			if getReply.Err == rpc.OK {
				return getReply.Value, getReply.Version, getReply.Err
			} else if getReply.Err == rpc.ErrNoKey {
				return "", 0, getReply.Err
			}
		}
		time.Sleep(100 * time.Millisecond) // 等待100ms再重试
		// 如果是其他错误，继续重试
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
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	putArgs := &rpc.PutArgs{Key: key, Value: value, Version: version}
	firstAttempt := true
	var putReply rpc.PutReply
	for {
		if ok := ck.clnt.Call(ck.server, "KVServer.Put", putArgs, &putReply); ok {
			if putReply.Err == rpc.ErrVersion {
				if firstAttempt {
					// 如果是第一次尝试调用收到 ErrVersion，直接返回 ErrVersion
					return rpc.ErrVersion
				} else {
					// 如果是重试阶段收到 ErrVersion，返回 ErrMaybe
					return rpc.ErrMaybe
				}
			}
			return putReply.Err
		}
		firstAttempt = false               // 之后都是重试阶段
		time.Sleep(100 * time.Millisecond) // 等待100ms再重试
	}
}
