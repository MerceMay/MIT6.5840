package rsm

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

// 每个服务器节点上上会运行：
// 一个Raft实例（负责共识协议）
// 一个RSM实例（负责应用操作到状态机）
// 一个KVServer实例（具体的键值存储服务）

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int   // 发起请求的服务器 id
	Id  int64 // 每次为一个请求生成一个唯一的 id
	Req any   // 请求内容
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// PendingOp 用于存储日志index对应的操作信息
type PendingOp struct {
	op     Op        // 操作
	result any       // 操作结果
	done   chan bool // 操作完成的信号通道
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	opId       int64              // 用于生成唯一操作 id
	pendingOps map[int]*PendingOp // 存储未完成的操作
	shutdown   atomic.Bool        // 用于标记是否已关闭
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		opId:         0,
		pendingOps:   make(map[int]*PendingOp),
	}
	rsm.shutdown.Store(false) // 初始化为未关闭
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if maxraftstate >= 0 {
		rsm.mu.Lock()
		defer rsm.mu.Unlock()
		snapshot := persister.ReadSnapshot() // 读取快照
		if len(snapshot) > 0 {
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			var opId int64
			var smSnapshot []byte
			if d.Decode(&opId) != nil || d.Decode(&smSnapshot) != nil {
				// 使用默认值
				panic("Failed to decode from snapshot")
			}
			atomic.StoreInt64(&rsm.opId, opId) // 恢复操作 ID
			rsm.sm.Restore(smSnapshot)         // 恢复状态机状态
		}
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	if rsm.shutdown.Load() {
		return rpc.ErrWrongLeader, nil // 如果已经关闭，返回错误
	}

	opId := atomic.AddInt64(&rsm.opId, 1) // 每次调用 Submit 时生成一个新的唯一操作 id
	op := Op{
		Me:  rsm.me,
		Id:  opId,
		Req: req,
	}

	index, term, isLeader := rsm.rf.Start(op) // 提交操作到 Raft
	if !isLeader {
		return rpc.ErrWrongLeader, nil // 不是领导者，返回错误
	}

	pendingOp := &PendingOp{
		op:   op,
		done: make(chan bool, 1),
	}
	rsm.mu.Lock()
	if pendingOp, exists := rsm.pendingOps[index]; exists {
		select {
		case pendingOp.done <- false: // 如果已经存在相同的操作，发送 false
		default:
		}
	}
	rsm.pendingOps[index] = pendingOp // 将操作添加到待处理操作列表

	rsm.mu.Unlock()

	err, result := rsm.waitForResult(pendingOp, term)

	rsm.mu.Lock()
	delete(rsm.pendingOps, index)
	rsm.mu.Unlock()

	return err, result // 返回操作结果或错误
}

// 等待操作结果
func (rsm *RSM) waitForResult(pendingOp *PendingOp, initialTerm int) (rpc.Err, any) {
	// 设置绝对超时
	timeout := time.NewTimer(100 * time.Millisecond)
	defer timeout.Stop()
	for {
		if rsm.shutdown.Load() {
			return rpc.ErrWrongLeader, nil // 如果已经关闭，返回错误
		}
		select {
		case <-timeout.C:
			currentTerm, isLeader := rsm.rf.GetState()
			if !isLeader || currentTerm != initialTerm {
				return rpc.ErrWrongLeader, nil
			}
			timeout.Reset(100 * time.Millisecond)
		case res := <-pendingOp.done:
			if res {
				return rpc.OK, pendingOp.result // 操作成功完成
			} else {
				return rpc.ErrWrongLeader, nil // 操作失败，领导者变更、或者操作被覆盖、或者applyCh被关闭
			}
		}
	}
}

func (rsm *RSM) reader() {
	for {
		msg, ok := <-rsm.applyCh
		if !ok {
			rsm.handleShutdown()
			return
		}
		if rsm.shutdown.Load() {
			return // 如果已经关闭，直接返回
		}
		if msg.CommandValid {
			rsm.applyCommand(msg)
		} else if msg.SnapshotValid {
			rsm.applySnapshot(msg)
		}
	}
}

// 处理系统关闭
func (rsm *RSM) handleShutdown() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	rsm.shutdown.Store(true) // 设置为已关闭状态

	// 通知所有等待操作
	for _, pendingOp := range rsm.pendingOps {
		select {
		case pendingOp.done <- false: // 发送 false 表示操作未完成
		default:
		}
	}

	// 清空待处理操作
	rsm.pendingOps = make(map[int]*PendingOp)
}

func (rsm *RSM) applyCommand(msg raftapi.ApplyMsg) {
	// 提取出操作
	op, ok := msg.Command.(Op) // 类型断言，确保 Command 是 Op 类型，但是不可能出现不是 Op 的情况，如果真出现了，panic
	if !ok {
		panic("Command is not of type Op")
	}
	rsm.mu.Lock()

	result := rsm.sm.DoOp(op.Req) // 执行状态机操作
	if pendingOp, exists := rsm.pendingOps[msg.CommandIndex]; exists {
		if pendingOp.op.Id == op.Id && pendingOp.op.Me == rsm.me {
			pendingOp.result = result // 设置操作结果
			select {
			case pendingOp.done <- true: // 发送 true 表示操作完成
			default:
			}
		} else {
			// 说明这是一个过期的操作，可能是因为领导者变更或者操作被覆盖
			select {
			case pendingOp.done <- false: // 发送 false 表示操作未完成
			default:
			}
		}
	}
	rsm.mu.Unlock()

	if rsm.maxraftstate > 0 && rsm.rf.PersistBytes() > (rsm.maxraftstate*9)/10 {
		go rsm.createSnapshot(msg.CommandIndex) // 定期创建快照
	}

}

func (rsm *RSM) createSnapshot(index int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	opId := atomic.LoadInt64(&rsm.opId) // 获取当前操作 ID
	smSnapshot := rsm.sm.Snapshot()     // 获取状态机快照
	e.Encode(opId)                      // 编码操作 ID
	e.Encode(smSnapshot)                // 将状态机快照写入缓冲区
	rsm.rf.Snapshot(index, w.Bytes())   // 提交快照到 Raft
}

func (rsm *RSM) applySnapshot(msg raftapi.ApplyMsg) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)
	var opId int64
	var smSnapshot []byte
	if d.Decode(&opId) != nil || d.Decode(&smSnapshot) != nil {
		panic("Failed to decode from snapshot")
	}
	currentOpId := atomic.LoadInt64(&rsm.opId)
	if opId > currentOpId {
		atomic.StoreInt64(&rsm.opId, opId) // 恢复操作 ID
	}
	rsm.sm.Restore(smSnapshot) // 恢复状态机状态

	for index, pendingOp := range rsm.pendingOps {
		if index <= msg.SnapshotIndex {
			select {
			case pendingOp.done <- false: // 发送 false 表示操作未完成
			default:
			}
		}
	}
}
