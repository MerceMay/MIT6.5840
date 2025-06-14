package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	for {
		currentCfgStr, currentVer, _ := sck.IKVClerk.Get("currentCfg")
		current := shardcfg.FromString(currentCfgStr)
		nextCfgStr, nextVer, _ := sck.IKVClerk.Get("nextCfg")
		if nextCfgStr == "" {
			// 没有下一个配置，直接跳过
			return
		}
		next := shardcfg.FromString(nextCfgStr)
		if next.Num <= current.Num {
			err := sck.IKVClerk.Put("nextCfg", "", nextVer) // 清除下一个配置
			if err == rpc.ErrVersion {
				continue
			}
			return
		}
		// 下一个配置更新，更新
		if sck.migrateConfig(current, next) {
			// 成功迁移分片，更新 currentCfg
			err := sck.IKVClerk.Put("currentCfg", next.String(), currentVer)
			if err == rpc.ErrVersion {
				continue // 版本不匹配，说明其他控制器在配置更新时已经有了更高版本的配置，本轮更新重试
			}
			// 清除 nextCfg
			err = sck.IKVClerk.Put("nextCfg", "", nextVer)
			if err == rpc.ErrVersion {
				continue // 版本不匹配，重试
			}
			return
		}
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	for {
		err1 := sck.IKVClerk.Put("currentCfg", cfg.String(), 0)
		err2 := sck.IKVClerk.Put("nextCfg", "", 0) // 清除 nextCfg
		if (err1 == rpc.OK || err1 == rpc.ErrMaybe) && (err2 == rpc.OK || err2 == rpc.ErrMaybe) {
			break
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	for {
		currentCfgStr, currentVer, _ := sck.IKVClerk.Get("currentCfg")
		current := shardcfg.FromString(currentCfgStr)
		if new.Num <= current.Num {
			return
		}
		nextCfgStr, nextVersion, _ := sck.IKVClerk.Get("nextCfg")
		if nextCfgStr != "" {
			existingNext := shardcfg.FromString(nextCfgStr)
			if existingNext.Num >= new.Num {
				if sck.migrateConfig(current, existingNext) {
					// 成功迁移分片，更新 currentCfg
					err := sck.IKVClerk.Put("currentCfg", existingNext.String(), currentVer)
					if err == rpc.ErrVersion {
						continue // 版本不匹配，重试
					}
					// 清除 nextCfg
					err = sck.IKVClerk.Put("nextCfg", "", nextVersion)
					if err == rpc.ErrVersion {
						continue // 版本不匹配，重试
					}
					return // 成功迁移分片并更新配置，退出循环
				}
			}
		}
		err := sck.IKVClerk.Put("nextCfg", new.String(), nextVersion)
		// Put会返回OK, ErrVersion, ErrMaybe, ErrNoKey
		if err == rpc.ErrVersion {
			continue
		}

		if sck.migrateConfig(current, new) {
			// 成功迁移分片，更新 currentCfg
			err = sck.IKVClerk.Put("currentCfg", new.String(), currentVer)
			 if err == rpc.ErrVersion {
				continue // 版本不匹配，重试
			}
			// 清除 nextCfg
			err = sck.IKVClerk.Put("nextCfg", "", nextVersion)
			 if err == rpc.ErrVersion {
				continue // 版本不匹配，重试
			}
			return // 成功迁移分片并更新配置，退出循环
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	configStr, _, err := sck.IKVClerk.Get("currentCfg")
	if err != rpc.OK {
		return nil
	}
	return shardcfg.FromString(configStr)
}

func (sck *ShardCtrler) migrateConfig(startCfg, targetCfg *shardcfg.ShardConfig) bool {
	for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
		currentGid := startCfg.Shards[shard]
		nextGid := targetCfg.Shards[shard]
		if currentGid == nextGid {
			continue // 一个分片的分组没有变化，不需要迁移
		}

		fromServer, fromExists := startCfg.Groups[currentGid]
		toServer, toExists := targetCfg.Groups[nextGid]
		// 只有当分片从一个组迁移到另一个组时才执行迁移逻辑
		// 如果源组不存在，说明该分片是新加入的，无需冻结/删除
		// 如果目标组不存在，说明该分片被移除，只需要删除 (但这里只处理迁移)
		if !fromExists {
			continue // 源分组不存在，跳过迁移
		}
		if !toExists {
			continue // 目标分组不存在，跳过迁移
		}
		fromClerk := shardgrp.MakeClerk(sck.clnt, fromServer)
		toClerk := shardgrp.MakeClerk(sck.clnt, toServer)

		var state []byte
		var err rpc.Err
		for {
			state, err = fromClerk.FreezeShard(shard, targetCfg.Num)
			if err == rpc.OK {
				break // 成功冻结分片
			} else if err == rpc.ErrWrongGroup {
				return false // 分片不属于当前组，无法迁移
			}
		}
		for {
			err = toClerk.InstallShard(shard, state, targetCfg.Num)
			if err == rpc.OK {
				break // 成功安装分片
			} else if err == rpc.ErrWrongGroup {
				return false // 分片不属于当前组，无法迁移
			}
		}
		for {
			err = fromClerk.DeleteShard(shard, targetCfg.Num)
			if err == rpc.OK {
				break // 成功删除分片
			} else if err == rpc.ErrWrongGroup {
				return false // 分片不属于当前组，无法迁移
			}
		}
	}
	return true // 成功迁移所有分片
}
