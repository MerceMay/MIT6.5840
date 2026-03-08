# MIT 6.5840 分布式系统项目 — 技术亮点总结

> 本文档从源码层面深入分析项目中值得写进简历的技术亮点，涵盖 Raft 共识算法、容错 KV 服务、分片 KV 存储等核心模块。

---

## 项目概览

本项目完整实现了 MIT 6.5840（原 6.824）分布式系统课程的全部 Lab，基于 Go 语言，包含：
- **Lab 1**: MapReduce 分布式计算框架
- **Lab 2**: Raft 共识算法（Leader 选举、日志复制、持久化、日志压缩与快照）
- **Lab 3**: 基于 Raft 的容错 KV 服务（线性一致性、快照）
- **Lab 4**: 分片 KV 存储（分片迁移、动态配置变更、Freeze/Install/Delete 三阶段协议）

所有课程测试均已通过。

---

### 亮点 1：基于 Channel 的事件驱动定时器架构

**涉及文件**：`raft1/raft.go`（第 492-591 行）、`raft1/util.go`

**问题背景**：
Raft 算法需要两种定时器：选举超时定时器和心跳定时器。如果使用简单的 `time.Sleep` 加轮询方式实现，会导致定时器重置不及时、CPU 空转等问题，且无法优雅地处理定时器重置和节点关闭。

**实现细节**：
- 采用了 **四个独立的 channel**（`resetElectionTimerCh`、`sendHeartbeatAtOnceCh`、`electionCh`、`heartbeatCh`）配合 `shutdownCh` 构建事件驱动模型
- `electionTimer()` 和 `heartbeatTimer()` 各运行在独立的 goroutine 中，使用 `time.NewTimer` 配合 `select` 语句实现精确的定时和重置：
  ```go
  func (rf *Raft) electionTimer() {
      timer := time.NewTimer(RandomElectionTimeout())
      defer timer.Stop()
      for !rf.killed() {
          select {
          case <-timer.C:
              // 超时触发选举
              select { case rf.electionCh <- struct{}{}: default: }
              timer.Reset(RandomElectionTimeout())
          case <-rf.resetElectionTimerCh:
              if !timer.Stop() { <-timer.C }
              timer.Reset(RandomElectionTimeout())
          case <-rf.shutdownCh:
              return
          }
      }
  }
  ```
- `ticker()` 协程作为**中央事件调度器**，统一监听 `electionCh`、`heartbeatCh` 和 `shutdownCh`，根据当前角色决定是发起选举还是发送心跳
- 心跳定时器支持 **立即触发**（`sendHeartbeatAtOnce()`），当新日志被提交或成为 Leader 时立即发送 AppendEntries，而非等待下一次心跳周期
- 选举超时使用 **250~650ms 随机化**（`RandomElectionTimeout()`），心跳固定 100ms（`StableHeartbeatTimeout()`），满足测试器要求的每秒不超过 10 次心跳和 5 秒内选出新 Leader
- 定时器重置使用了**带缓冲的 channel**（容量为 1）和 `select/default` 模式来避免阻塞

**技术价值**：这种事件驱动架构将定时器管理与业务逻辑完全解耦，避免了锁竞争和阻塞问题，体现了对 Go 并发原语（channel、select、goroutine）的深入理解和对定时器竞态条件（timer drain）的正确处理。

---

### 亮点 2：AppendEntries 日志冲突快速回溯优化

**涉及文件**：`raft1/raft.go`（第 704-886 行）

**问题背景**：
Raft 论文中描述的基础日志冲突处理是每次只回退一个 index（`nextIndex--`），在网络分区恢复后可能需要大量 RPC 才能使 follower 的日志与 leader 同步。例如，一个 follower 落后 1000 条日志，基础方案需要 1000 次 RPC。

**实现细节**：
- 在 `AppendEntriesReply` 中增加了 `ConflictIndex` 和 `ConflictTerm` 两个字段，实现了**按任期跳跃回溯**：
  ```go
  type AppendEntriesReply struct {
      Term          int
      Success       bool
      ConflictIndex int  // 冲突的日志条目索引
      ConflictTerm  int  // 冲突的日志条目任期号
  }
  ```
- **Follower 端**（`AppendEntries` handler）：当日志不匹配时，返回冲突任期和该任期的第一个 index，使 Leader 能跳过整个冲突任期：
  ```go
  if args.PrevLogTerm != rf.getLogTerm(args.PrevLogIndex) {
      conflictTerm := rf.getLogTerm(args.PrevLogIndex)
      i := args.PrevLogIndex
      for i > rf.lastIncludedIndex && rf.getLogTerm(i-1) == conflictTerm {
          i--
      }
      reply.ConflictIndex = i
      reply.ConflictTerm = conflictTerm
  }
  ```
- **Leader 端**（`sendAppendEntries` 回调）：收到冲突回复后，在自己的日志中查找是否存在 `ConflictTerm`，如果存在则定位到该任期的最后一个 index + 1，否则直接跳到 `ConflictIndex`：
  ```go
  if reply.ConflictTerm == -1 {
      rf.nextIndex[i] = reply.ConflictIndex
  } else {
      conflictIndex := -1
      for i := rf.getLastLogIndex(); i >= rf.lastIncludedIndex; i-- {
          if rf.getLogTerm(i) == reply.ConflictTerm {
              conflictIndex = i
              break
          }
      }
      if conflictIndex != -1 {
          rf.nextIndex[i] = conflictIndex + 1
      } else {
          rf.nextIndex[i] = reply.ConflictIndex
      }
  }
  ```
- 还处理了 **PrevLogIndex 超出 follower 日志范围**的情况（`ConflictTerm = -1`, `ConflictIndex = lastLogIndex + 1`），直接跳到 follower 日志末尾

**技术价值**：该优化将日志同步从 O(N) 次 RPC 降低到 O(任期数) 次 RPC，在长时间分区恢复时效果显著。这是 Raft 论文中提到但未详细描述的优化方案，体现了对分布式系统性能调优的理解。

---

### 亮点 3：日志压缩与快照的完整实现（含 InstallSnapshot RPC）

**涉及文件**：`raft1/raft.go`（第 69-306 行）

**问题背景**：
Raft 日志会无限增长，不仅占用存储空间，还会导致崩溃恢复时重放时间过长。需要通过日志压缩（Snapshot）来截断已应用的日志条目。同时，当 Leader 发现某个 Follower 落后太多（需要的日志已被快照覆盖）时，需要通过 InstallSnapshot RPC 发送完整快照。

**实现细节**：
- **日志索引虚拟化**：引入 `lastIncludedIndex` 和 `lastIncludedTerm`，所有日志访问通过 `getLogTerm(index)`、`getLastLogIndex()` 等辅助方法实现物理索引到逻辑索引的转换：
  ```go
  func (rf *Raft) getLogTerm(index int) int {
      if index == rf.lastIncludedIndex { return rf.lastIncludedTerm }
      relativeIndex := index - rf.lastIncludedIndex
      return rf.log[relativeIndex].Term
  }
  ```
- **快照触发**：`Snapshot()` 方法由上层服务调用，在持有锁的情况下原子地截断日志、更新快照元数据并持久化：
  ```go
  func (rf *Raft) Snapshot(index int, snapshot []byte) {
      rf.log = append([]LogEntry{{Term: relativeTerm}}, rf.log[relativeIndex+1:]...)
      rf.lastIncludedIndex = index
      rf.persistWithSnapshot(snapshot)  // 原子持久化状态和快照
  }
  ```
- **InstallSnapshot RPC**：当 `nextIndex[i] <= lastIncludedIndex` 时，Leader 自动切换为发送快照而非日志条目；Follower 收到快照后正确处理日志截断（保留快照之后的日志或清空）
- **持久化完整性**：`persist()` 保存 5 个字段（currentTerm、votedFor、log、lastIncludedIndex、lastIncludedTerm），`persistWithSnapshot()` 原子保存 Raft 状态和快照数据
- **applier 协程中的快照处理**：当 `lastApplied < lastIncludedIndex` 时，优先通过 applyCh 发送快照而非逐条日志

**技术价值**：这是 Raft 论文 Section 7 的完整工程实现，涵盖了日志截断、索引映射、快照传输、持久化一致性等多个细节点，体现了对分布式系统存储管理和数据一致性的深入理解。

---

### 亮点 4：基于条件变量（Cond）的 Applier 协程与有序日志应用

**涉及文件**：`raft1/raft.go`（第 645-701 行）

**问题背景**：
Raft 提交的日志条目需要**严格按顺序**应用到状态机。如果使用轮询方式检查 commitIndex 变化，会浪费 CPU；如果使用 channel，在高吞吐量场景下可能导致缓冲区溢出或死锁。

**实现细节**：
- 使用 `sync.Cond` 条件变量（`applyCond`）实现高效的等待-通知机制：
  ```go
  func (rf *Raft) applier() {
      for !rf.killed() {
          rf.applyCond.L.Lock()
          for rf.commitIndex <= rf.lastApplied && !rf.killed() {
              rf.applyCond.Wait()  // 等待新条目可应用
          }
          // 逐条应用日志...
          for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
              rf.applyCond.L.Unlock()
              rf.applyCh <- msg  // 在不持锁的情况下发送到 applyCh
              rf.applyCond.L.Lock()
              rf.lastApplied = i
          }
          rf.applyCond.L.Unlock()
      }
  }
  ```
- **关键设计**：在发送 `applyCh <- msg` 时**释放锁**，避免了因 applyCh 消费者处理较慢时产生的死锁（applyCh 的消费者可能需要获取 Raft 锁）
- 在 `AppendEntries` handler 和 `sendAppendEntries` 回调中更新 `commitIndex` 后调用 `rf.applyCond.Signal()` 唤醒 applier
- 严格按照 `lastApplied + 1` 到 `commitIndex` 的顺序逐条应用，确保状态机一致性

**技术价值**：条件变量相比 channel 在此场景下更合适——它天然支持"等待条件满足"的语义，不会产生缓冲区积压问题。释放锁再发送 channel 的设计避免了隐蔽的死锁风险，体现了对并发编程细节的掌控能力。

---

### 亮点 5：RSM（Replicated State Machine）层的请求路由与操作匹配机制

**涉及文件**：`kvraft1/rsm/rsm.go`

**问题背景**：
基于 Raft 的 KV 服务需要解决一个核心问题：RPC handler 提交命令到 Raft 后，如何等待该命令被提交并获取执行结果？由于 Leader 可能变更，提交到某个 index 的命令可能最终被另一个命令覆盖。

**实现细节**：
- 设计了 `PendingOp` 结构，通过 `map[int]*PendingOp`（index -> 操作）管理等待中的请求：
  ```go
  type PendingOp struct {
      op     Op        // 包含 Me（服务器ID）+ Id（唯一操作ID）+ Req（请求内容）
      result any       // 操作结果
      done   chan bool  // 完成信号（true=成功，false=需要重试）
  }
  ```
- **Submit 流程**：为每个请求生成唯一 `opId`（原子递增），调用 `rf.Start()` 提交到 Raft，然后注册 PendingOp 并等待结果
- **reader 协程**：从 `applyCh` 读取已提交命令，执行状态机操作后，通过 index 匹配到 PendingOp，并使用 **Op.Id + Op.Me 双重验证**确保是同一个操作：
  ```go
  if pendingOp.op.Id == op.Id && pendingOp.op.Me == rsm.me {
      pendingOp.result = result
      pendingOp.done <- true   // 操作成功
  } else {
      pendingOp.done <- false  // 操作被覆盖/Leader 变更
  }
  ```
- **超时与 Leader 检测**：`waitForResult()` 使用 100ms 定时器周期性检查 Leader 状态和任期变化，避免无限等待：
  ```go
  func (rsm *RSM) waitForResult(pendingOp *PendingOp, initialTerm int) (rpc.Err, any) {
      timeout := time.NewTimer(100 * time.Millisecond)
      for {
          select {
          case <-timeout.C:
              currentTerm, isLeader := rsm.rf.GetState()
              if !isLeader || currentTerm != initialTerm {
                  return rpc.ErrWrongLeader, nil
              }
              timeout.Reset(100 * time.Millisecond)
          case res := <-pendingOp.done:
              if res { return rpc.OK, pendingOp.result }
              return rpc.ErrWrongLeader, nil
          }
      }
  }
  ```
- **优雅关闭**：`handleShutdown()` 通知所有等待中的操作返回 ErrWrongLeader，防止 goroutine 泄漏

**技术价值**：RSM 层是连接 Raft 共识层和上层 KV 服务的关键抽象层，它解决了"命令提交后如何异步获取结果"这一分布式系统中的经典问题。Op.Id + Op.Me 双重验证 + 超时检测的组合策略有效保证了正确性，体现了对分布式系统请求处理全生命周期的理解。

---

### 亮点 6：RSM 层的快照与状态恢复机制

**涉及文件**：`kvraft1/rsm/rsm.go`（第 257-300 行）、`kvraft1/server.go`（第 115-158 行）

**问题背景**：
RSM 层需要在 Raft 日志过大时触发快照，并在收到快照时正确恢复状态。快照需要包含两部分：RSM 自身的操作 ID（用于保证唯一性）和上层状态机的数据。

**实现细节**：
- **自动快照触发**：在每次应用命令后检查 Raft 持久化大小，当超过阈值的 90% 时异步创建快照：
  ```go
  if rsm.maxraftstate > 0 && rsm.rf.PersistBytes() > (rsm.maxraftstate*9)/10 {
      go rsm.createSnapshot(msg.CommandIndex)
  }
  ```
- **双层快照编码**：快照数据包含 `opId`（RSM 层操作计数器）和 `smSnapshot`（上层状态机快照），通过 `StateMachine` 接口实现解耦：
  ```go
  type StateMachine interface {
      DoOp(any) any
      Snapshot() []byte
      Restore([]byte)
  }
  ```
- **KVServer 的 Snapshot/Restore**：序列化/反序列化整个 `store map[string]KVPair`
- **快照应用时的 opId 处理**：仅在快照中的 opId 更大时更新当前 opId，避免快照回退导致 ID 冲突
- **PendingOps 清理**：应用快照时，通知所有 index ≤ snapshotIndex 的等待操作返回失败

**技术价值**：双层快照设计通过 `StateMachine` 接口实现了 RSM 层与具体业务逻辑的解耦，使得同一套 RSM 代码可以同时服务于 KVRaft 和 ShardKV 两种不同的上层应用，体现了良好的软件抽象能力。

---

### 亮点 7：MapReduce Coordinator 的 Channel 串行化消息处理与容错机制

**涉及文件**：`mr/coordinator.go`、`mr/worker.go`

**问题背景**：
MapReduce Coordinator 需要同时处理多个 Worker 的心跳请求和任务完成报告，还要处理 Worker 超时（10 秒未完成则重新分配任务）。并发访问共享状态容易引入竞态条件。

**实现细节**：
- **Channel 串行化**：通过 `heartbeatChan` 和 `reportChan` 将所有消息序列化到单一 `run()` goroutine 处理，从根本上避免了并发竞争：
  ```go
  func (c *Coordinator) run() {
      for {
          select {
          case hb := <-c.heartbeatChan:
              c.handleHeartbeat(hb)
          case rp := <-c.reportChan:
              c.handleReport(rp)
          case <-c.doneChan:
              return
          }
      }
  }
  ```
- 每个 RPC handler（`Heartbeat`/`Report`）使用**同步 channel**（`ok chan struct{}`）等待处理完成后再返回 RPC 响应
- **Worker 崩溃容错**：独立的 `checkTimeout()` 协程每秒检查运行中的任务，将超过 10 秒未完成的任务重置为 idle 状态
- **原子文件写入**：Worker 使用 `atomicWriteFile()` 通过临时文件 + `os.Rename` 实现原子写入，避免部分写入导致的数据损坏：
  ```go
  func atomicWriteFile(filename string, data []byte) error {
      tempFile, err := os.CreateTemp("", "temp_*")
      // 写入 -> Sync -> Close -> Remove旧文件 -> Rename
  }
  ```
- **阶段化设计**：Coordinator 维护明确的阶段（mapPhase → reducePhase → allDonePhase），在 map 全部完成后自动初始化 reduce 任务

**技术价值**：Channel 串行化处理模式是 Go 语言推荐的并发安全模式（"不要通过共享内存来通信，而要通过通信来共享内存"），消除了复杂的锁逻辑。原子文件写入、Worker 超时重试等设计体现了对分布式计算容错性的全面考虑。

---

### 亮点 8：分片迁移的三阶段协议（Freeze → Install → Delete）

**涉及文件**：`shardkv1/shardctrler/shardctrler.go`、`shardkv1/shardgrp/server.go`、`shardkv1/shardgrp/shardrpc/shardrpc.go`

**问题背景**：
在分片 KV 系统中，当配置变更导致分片需要在不同 Group 之间迁移时，需要保证迁移过程中数据的一致性和可用性。简单的直接传输方案可能导致数据丢失或重复。

**实现细节**：
- **三阶段协议**：
  1. **Freeze**：冻结源 Group 上的分片，拒绝后续对该分片的读写请求，并返回分片的完整数据
  2. **Install**：将分片数据安装到目标 Group，设置分片为 Owned 状态
  3. **Delete**：删除源 Group 上的旧分片数据，释放存储空间

- **每个分片独立管理状态**，通过 `ShardInfo` 跟踪配置号、冻结状态和所有权：
  ```go
  type ShardInfo struct {
      ConfigNum shardcfg.Tnum  // 分片配置号
      Frozen    bool           // 是否冻结
      Owned     bool           // 是否拥有
  }
  ```

- **配置号（ConfigNum）保证幂等性**：每个分片操作都携带目标配置号，如果当前配置号已超过请求中的配置号，则拒绝操作（返回 `ErrWrongGroup`），避免过期请求导致的状态回退

- **所有分片操作都通过 Raft 日志复制**：FreezeShard、InstallShard、DeleteShard 都经过 RSM 的 `Submit()` 方法提交到 Raft，保证 Group 内所有节点对分片状态变更达成共识

- **ShardCtrler 的故障恢复**：
  ```go
  func (sck *ShardCtrler) InitController() {
      // 检查是否有未完成的配置变更（nextCfg 不为空）
      // 如果有，继续执行迁移，保证配置变更的原子性
  }
  ```

- **ChangeConfigTo 的版本化更新**：使用 KV 存储保存当前配置和下一个配置，通过版本号（Tversion）的乐观锁机制保证并发控制器的安全协调

**技术价值**：三阶段迁移协议解决了分片迁移过程中的一致性问题——Freeze 保证迁移期间不会有新写入，Install 保证数据完整到达，Delete 实现垃圾回收。配置号的幂等性设计使得迁移操作可以安全重试，体现了对分布式事务和数据一致性的深入思考。

---

### 亮点 9：Raft RPC 的安全性守卫 — 任期与角色一致性校验

**涉及文件**：`raft1/raft.go`（选举、日志复制、快照相关的所有 RPC 回调）

**问题背景**：
在分布式环境中，RPC 可能因网络延迟而过时（stale RPC）。例如，一个 Leader 发送了 AppendEntries RPC 后自己已经变成了 Follower，但收到了延迟回复。如果不进行校验，可能基于过时信息做出错误决策。

**实现细节**：
- **每个 RPC 回调都包含三重校验**（以 `sendAppendEntries` 为例）：
  ```go
  // 发送前校验
  rf.mu.Lock()
  if rf.killed() || rf.state != Leader || rf.currentTerm != currentTerm {
      rf.mu.Unlock(); return
  }
  // ... 构造参数并发送 RPC ...
  // 收到回复后再次校验
  rf.mu.Lock()
  if rf.killed() || rf.state != Leader || rf.currentTerm != args.Term {
      return  // 丢弃过时回复
  }
  ```
- **选举 RPC 的四重校验**：
  ```go
  if rf.killed() || rf.currentTerm != args.Term || rf.state != Candidate || rf.votedFor != args.CandidateId {
      return  // 任期变了 / 不再是候选人 / 投票对象变了
  }
  ```
- **选举定时器重置的严格条件**（遵循 Students' Guide to Raft）：
  1. 收到**当前任期** Leader 的 AppendEntries（过期任期不重置）
  2. 自己发起选举时
  3. **授予投票**时（未投票不重置）
  
  代码中在 `RequestVote` handler 里只有 `reply.VoteGranted = true` 时才调用 `rf.resetElectionTimer()`

- **becomeFollower 时的统一状态重置**：
  ```go
  func (rf *Raft) becomeFollower(term int) {
      rf.state = Follower
      rf.currentTerm = term
      rf.votedFor = -1  // 重置投票
      rf.persist()      // 持久化
  }
  ```

**技术价值**：这些看似简单的校验是 Raft 正确性的基石。源码注释中提到了因为选举定时器重置条件不正确导致 `TestFigure8Unreliable3C` 概率性失败的调试经历，体现了对 Raft 协议细微之处（safety proof）的深入理解和实战调试能力。

---

### 亮点 10：分片 KV 的读写路径细粒度锁设计

**涉及文件**：`shardkv1/shardgrp/server.go`、`kvraft1/server.go`

**问题背景**：
KV 服务的读写操作需要并发安全，但如果使用全局锁，会严重限制并发性能。不同 key 之间的操作本质上是独立的，可以安全并发执行。

**实现细节**：
- **Key 级别读写锁**：每个 key 拥有独立的 `sync.RWMutex`，存储在 `locks map[string]*sync.RWMutex` 中，通过 `getLock(key)` 按需创建：
  ```go
  func (kv *KVServer) getLock(key string) *sync.RWMutex {
      kv.mu.Lock()
      defer kv.mu.Unlock()
      if _, exists := kv.locks[key]; !exists {
          kv.locks[key] = &sync.RWMutex{}
      }
      return kv.locks[key]
  }
  ```
- **Get 操作使用 RLock**，允许同一 key 的多个读并发执行
- **Put 操作使用 Lock**，保证同一 key 的写操作互斥
- **分片级别的所有权和冻结检查**使用 `mu sync.RWMutex`（只读操作用 RLock），与 key 级别的锁分层
- **版本号（Tversion）乐观并发控制**：Put 操作通过版本号检查实现 CAS（Compare-And-Swap）语义，进一步减少不必要的锁等待

**技术价值**：两层锁设计（shard 级 RWMutex + key 级 RWMutex）在保证线性一致性的同时最大化了读写并发度。结合版本号的乐观并发控制，体现了对高并发系统锁策略和性能优化的全面理解。

---

### 亮点 11：Goroutine 生命周期管理与优雅关闭

**涉及文件**：`raft1/raft.go`（第 481-516 行）、`kvraft1/rsm/rsm.go`（第 194-229 行）

**问题背景**：
Raft 实例会启动多个长期运行的 goroutine（applier、electionTimer、heartbeatTimer、ticker），测试框架在每个测试后不会主动终止这些 goroutine。如果不正确管理，会导致内存泄漏、CPU 浪费和后续测试干扰。

**实现细节**：
- **Raft 层**：使用 `shutdownCh` channel 配合 `atomic.StoreInt32(&rf.dead, 1)` 实现双重关闭信号：
  ```go
  func (rf *Raft) Kill() {
      atomic.StoreInt32(&rf.dead, 1)
      close(rf.shutdownCh)  // 关闭所有计时器
  }
  ```
  - 所有定时器 goroutine 都监听 `shutdownCh`，收到关闭信号后正确停止 timer 并退出
  - applier 协程通过 `rf.killed()` 检查退出循环

- **RSM 层**：使用 `atomic.Bool` 标志 + applyCh 关闭检测：
  ```go
  func (rsm *RSM) reader() {
      for {
          msg, ok := <-rsm.applyCh
          if !ok {
              rsm.handleShutdown()  // channel 关闭时清理所有 pending ops
              return
          }
          // ...
      }
  }
  ```
  - `handleShutdown()` 遍历所有 pendingOps 发送 false 信号，防止 Submit 调用者永久阻塞

- **KV 层**：使用 `atomic.StoreInt32/LoadInt32` 进行无锁的 killed 状态检查

**技术价值**：正确的 goroutine 生命周期管理是 Go 程序质量的重要标志。多层关闭机制（atomic flag + channel close + condition variable signal）保证了在各种并发场景下都能安全退出，体现了对 Go 并发模型和资源管理的深入理解。

---

### 亮点 12：ShardCtrler 配置变更的容错与幂等性设计

**涉及文件**：`shardkv1/shardctrler/shardctrler.go`

**问题背景**：
ShardCtrler 负责协调配置变更，但控制器本身可能在变更过程中崩溃并被新的控制器替代。需要保证配置变更的原子性——要么完全完成，要么可以安全重试。

**实现细节**：
- **两键状态机**：使用 KV 存储中的两个键 `currentCfg` 和 `nextCfg` 管理配置变更的状态：
  - `currentCfg`：当前生效的配置
  - `nextCfg`：正在迁移中的目标配置
- **乐观锁（版本号）保证并发安全**：所有配置更新都使用 `Put(key, value, version)` 的版本号机制，如果版本不匹配（其他控制器已经修改），则重试整个流程：
  ```go
  err := sck.IKVClerk.Put("currentCfg", new.String(), currentVer)
  if err == rpc.ErrVersion {
      continue  // 版本冲突，重新读取配置并重试
  }
  ```
- **InitController 恢复逻辑**：新控制器启动时检查 `nextCfg` 是否有未完成的迁移，如果有则继续执行，保证了**配置变更的最终一致性**
- **分片迁移的幂等性**：`FreezeShard` 检测重复冻结请求（同一 ConfigNum 且已冻结），直接返回当前数据而不重复操作

**技术价值**：这种"两阶段提交 + 幂等重试"的设计模式在分布式系统中广泛应用（如 Saga 模式），通过将中间状态持久化到可靠存储中，实现了跨故障的操作连续性，体现了对分布式事务和故障恢复的工程能力。

---

## 可改进之处

1. **读请求优化**：当前所有 Get 请求都通过 Raft 日志复制，保证了线性一致性但牺牲了读性能。可以考虑实现 ReadIndex 或 Lease Read 优化，减少读请求对 Raft 日志的依赖。

2. **日志批量应用**：当前 applier 逐条发送日志到 applyCh，在高吞吐量场景下可以考虑批量应用以减少 channel 通信开销。

3. **Key 级锁的内存管理**：`locks map[string]*sync.RWMutex` 只增不减，长时间运行后可能占用较多内存。可以考虑引入 LRU 或定期清理机制。

4. **分片迁移的并行化**：当前 `migrateConfig` 中分片逐个迁移，可以并行迁移不同的分片以提高配置变更速度。

5. **PreVote 机制**：当前实现未包含 PreVote（Raft 论文 Section 9.6），在网络分区场景下可能导致不必要的任期增长。

6. **日志冲突回溯可进一步优化**：当前 Follower 端查找冲突任期的第一个 index 使用线性扫描，可以使用二分查找进一步优化。
