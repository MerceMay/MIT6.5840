package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// 似乎和测试有关，定时器的间隔如果没选好，会导致TestFigure8Unreliable3C有概率失败：failed to reach agreement
// 还有一个导致这个测试失败的原因是，心跳只能在三种情况下重置：https://thesquareplanet.com/blog/students-guide-to-raft/
// a) you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer);
// b) you are starting an election;
// or c) you grant a vote to another peer.(没有投票也不重置)
// 此外，一定要确保每次RPC后身份不变，任期不变，如果变化了，就丢弃这个RPC
const HeartbeatTimeout = 100

func RandomElectionTimeout() time.Duration {
	// 测试器要求你的 Raft 在旧 leader 失败后的 5 秒内选出一个新的 leader。
	return time.Duration(250+rand.Intn(400)) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	// 测试器要求 leader 每秒发送检测信号 RPC 不超过 10 次。
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}
