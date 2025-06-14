package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key      string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.key = l
	lk.clientID = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		// 先检查锁是否存在，且谁拥有它
		val, ver, err := lk.ck.Get(lk.key)

		if err == rpc.ErrNoKey {
			// 锁不存在，直接上锁
			err = lk.ck.Put(lk.key, lk.clientID, 0)
			if err == rpc.OK {
				// 上锁成功
				return
			}
			// 说明在get和put之间，锁被其他客户端上锁了
			// 或者返回的是rpc.ErrMaybe，此时自己已经上锁了，保险的起见，继续重试
			continue
		} else if err == rpc.OK {
			// 锁存在，检查是否是自己上锁的
			if val == lk.clientID {
				// 是自己上锁的，直接返回
				return
			} else if val == "" {
				// 锁存在但是没有被上锁
				err = lk.ck.Put(lk.key, lk.clientID, ver)
				if err == rpc.OK {
					// 上锁成功
					return
				}
			} else {
				// 锁被其他客户端上锁了，等待一段时间后重试
				time.Sleep(time.Millisecond * 10)
				continue
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		// 先检查锁是否存在，且谁拥有它
		val, ver, err := lk.ck.Get(lk.key)

		if err == rpc.ErrNoKey {
			// 锁不存在，直接返回
			return
		} else if err == rpc.OK {
			if val == lk.clientID {
				// 是自己上锁的，直接释放锁
				err = lk.ck.Put(lk.key, "", ver)
				if err == rpc.OK {
					// 释放锁成功
					return
				} else if err == rpc.ErrVersion {
					// 版本号不匹配，尝试重新更新版本号
					continue
				} else if err == rpc.ErrMaybe {
					// rpc.ErrMaybe:不确定是否释放成功，为了避免锁没有释放成功，再次尝试
					continue
				}
			} else {
				// 锁被其他客户端上锁了，直接返回
				return
			}
		}
	}
}
