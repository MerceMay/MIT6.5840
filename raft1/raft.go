package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// LogEntry represents a single log entry in the Raft log.
type LogEntry struct {
	Command interface{} // 日志条目包含的命令
	Term    int         // 日志条目的任期号
}

// 定义一个 Raft 节点的状态
type PeerState int

const (
	Leader PeerState = iota
	Follower
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state	on all servers
	currentTerm int        // 服务器最后一次收到的任期号
	votedFor    int        // 服务器投票给的候选人 ID
	log         []LogEntry // 日志条目，包含命令和任期号
	// volatile state on all servers
	commitIndex int // 已被集群大多数节点提交的日志条目索引
	lastApplied int // 此节点已应用到状态机的日志条目索引
	// volatile state on leaders
	nextIndex  []int // 对于第i个服务器，领导者发送心跳的时候，从nextIndex[i]开始发送日志条目
	matchIndex []int // 对于第i个服务器，从[1, matchIndex[i]]的日志条目和leader的日志条目一致

	// 其他属性
	state                 PeerState             // 当前状态：Leader, Follower, Candidate
	resetElectionTimerCh  chan struct{}         // 重置选举定时器的通道
	sendHeartbeatAtOnceCh chan struct{}         // 立即发送心跳的通道
	electionCh            chan struct{}         // 选举定时器超时通知通道
	heartbeatCh           chan struct{}         // 心跳定时器超时通知通道
	shutdownCh            chan struct{}         // 当节点被杀死时，关闭所有通道
	applyCh               chan raftapi.ApplyMsg // 用于发送 ApplyMsg 的通道
	applyCond             *sync.Cond            // 用于通知 ApplyMsg 的条件变量
	// for 3D
	lastIncludedIndex int // 快照的最后一个日志条目索引 // 所有以rf.log为基础的索引都要减去这个值
	lastIncludedTerm  int // 快照的最后一个日志条目任期
}

func (rf *Raft) getLogTerm(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	relativeIndex := index - rf.lastIncludedIndex
	if relativeIndex < 0 || relativeIndex >= len(rf.log) {
		return -1
	}
	return rf.log[relativeIndex].Term
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	state := rf.encodeState()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(state, snapshot)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	state := rf.encodeState()
	rf.persister.Save(state, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("readPersist failed")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

type InstallSnapshotArgs struct {
	Term              int    // 领导者任期
	LeaderId          int    // 领导者 ID
	LastIncludedIndex int    // 快照的最后一个日志条目索引，包括在内
	LastIncludedTerm  int    // 快照的最后一个日志条目任期
	Data              []byte // 快照数据
}

type InstallSnapshotReply struct {
	Term int // 当前任期，让领导者更新自己的任期
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	reply.Term = rf.currentTerm

	rf.resetElectionTimer() // 论文中并没有说，学生指导中也没有明确说明，但按照逻辑，这个快照安装是由领导者发起的，领导者发起的都应该重置选举定时器

	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex < rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex > rf.getLastLogIndex() {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	} else {
		rf.log = append([]LogEntry{{Term: args.LastIncludedTerm, Command: nil}}, rf.log[args.LastIncludedIndex-rf.lastIncludedIndex+1:]...)
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	// 持久化快照
	rf.persistWithSnapshot(args.Data)

	// 应用快照到状态机
	snapshotMsg := raftapi.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- snapshotMsg
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}

	if index <= rf.lastIncludedIndex {
		return
	}

	if index > rf.getLastLogIndex() {
		return
	}

	relativeIndex := index - rf.lastIncludedIndex
	relativeTerm := rf.getLogTerm(index)
	rf.log = append([]LogEntry{{Term: relativeTerm, Command: nil}}, rf.log[relativeIndex+1:]...)

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = relativeTerm

	// 更新 commitIndex 和 lastApplied
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	rf.persistWithSnapshot(snapshot)

	// 如果是领导者，立即发送心跳
	if rf.state == Leader {
		rf.sendHeartbeatAtOnce()
	}

	// 通知 applier
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, peer *labrpc.ClientEnd) {
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := peer.Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.currentTerm != args.Term || rf.state != Leader {
		return
	}
	// fmt.Println("test")
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人的任期
	CandidateId  int // 候选人的 ID
	LastLogIndex int // 候选人的最后日志条目
	LastLogTerm  int // 候选人的最后日志条目的 任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期，让候选人更新自己的任期
	VoteGranted bool // 候选人是否获得投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	upToDate := func() bool {
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.getLastLogTerm()
		if args.LastLogTerm != lastLogTerm {
			return args.LastLogTerm > lastLogTerm
		}
		return args.LastLogIndex >= lastLogIndex
	}()

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendRequestVote() {
	rf.mu.Lock()
	if rf.killed() || rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.becomeCandidate()
	rf.resetElectionTimer()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	var votes int32 = 1 // 自己投票
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer *labrpc.ClientEnd) {
			reply := RequestVoteReply{}
			ok := peer.Call("Raft.RequestVote", &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.killed() || rf.currentTerm != args.Term || rf.state != Candidate || rf.votedFor != args.CandidateId {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				if atomic.AddInt32(&votes, 1) > int32(len(rf.peers)/2) && rf.state == Candidate && rf.currentTerm == args.Term {
					rf.becomeLeader()
					rf.sendHeartbeatAtOnce()
				}
			}
		}(peer)
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	lastLogIndex := rf.getLastLogIndex()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	// isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.killed() {
		return index, term, false
	}
	index = rf.getLastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()
	// 立马发送心跳
	rf.sendHeartbeatAtOnce()
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.shutdownCh) // 关闭所有计时器
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionCh:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if !isLeader {
				go rf.sendRequestVote()
			}
		case <-rf.heartbeatCh:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				go rf.sendAppendEntries()
			}
		case <-rf.shutdownCh:
			return
		}
	}
}

func (rf *Raft) electionTimer() {
	timer := time.NewTimer(RandomElectionTimeout())
	defer timer.Stop()

	for !rf.killed() {
		select {
		case <-timer.C:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if !isLeader {
				select {
				case rf.electionCh <- struct{}{}:
				default:
				}
			}
			timer.Reset(RandomElectionTimeout())
		case <-rf.resetElectionTimerCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(RandomElectionTimeout())
		case <-rf.shutdownCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (rf *Raft) heartbeatTimer() {
	timer := time.NewTimer(StableHeartbeatTimeout())
	defer timer.Stop()

	for !rf.killed() {
		select {
		case <-timer.C:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				select {
				case rf.heartbeatCh <- struct{}{}:
				default:
				}
			}
			timer.Reset(StableHeartbeatTimeout())
		case <-rf.sendHeartbeatAtOnceCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(0) // 立即发送心跳
		case <-rf.shutdownCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	select {
	case rf.resetElectionTimerCh <- struct{}{}:
	default:
	}
}
func (rf *Raft) sendHeartbeatAtOnce() {
	select {
	case rf.sendHeartbeatAtOnceCh <- struct{}{}:
	default:
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.mu = sync.Mutex{}
	atomic.StoreInt32(&rf.dead, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0, Command: nil}} // 初始化日志，包含一个 dummy log
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = Follower
	rf.resetElectionTimerCh = make(chan struct{}, 1)
	rf.sendHeartbeatAtOnceCh = make(chan struct{}, 1)
	rf.electionCh = make(chan struct{}, 1)
	rf.heartbeatCh = make(chan struct{}, 1)
	rf.shutdownCh = make(chan struct{}, 1)
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

	go rf.applier()
	go rf.electionTimer()
	go rf.heartbeatTimer()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.applyCond.L.Lock()
		// 等待有新条目可应用
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.applyCond.L.Unlock()
			return
		}

		// 发送快照
		if rf.lastApplied < rf.lastIncludedIndex {
			snapshotMsg := raftapi.ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.lastApplied = rf.lastIncludedIndex // 更新 lastApplied
			rf.applyCh <- snapshotMsg
			rf.applyCond.L.Unlock()
			continue
		}

		// 出现任何日志乱序都是因为，
		// You should have a loop somewhere that takes one client operation at the time (in the same order on all servers – this is where Raft comes in),
		// and applies **each one** to the state machine in order
		// 应该逐一应用日志条目，这一点非常重要
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex && !rf.killed(); i++ {
				if i <= rf.lastIncludedIndex {
					continue
				}
				relativeIndex := i - rf.lastIncludedIndex
				if relativeIndex < 0 || relativeIndex >= len(rf.log) {
					break
				}
				// 发送日志条目
				msg := raftapi.ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[relativeIndex].Command,
					CommandIndex:  i,
					SnapshotValid: false,
				}
				rf.applyCond.L.Unlock()
				rf.applyCh <- msg
				rf.applyCond.L.Lock()
				rf.lastApplied = i
			}
			rf.applyCond.L.Unlock()
		} else {
			rf.applyCond.L.Unlock()
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // 领导者任期
	LeaderId     int        // 领导者 ID：让跟随者知道领导者是谁
	PrevLogIndex int        // 领导者的nextIndex[i] - 1
	PrevLogTerm  int        // 领导者的log[prevLogIndex].Term
	Entries      []LogEntry // 日志条目，以便将新条目附加到日志，如果是新领导者，则为空
	LeaderCommit int        // 领导者已提交的日志条目索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期号，以便领导者更新自己的任期号
	Success bool // 成功附加日志条目到跟随者的日志
	// 下面两个字段用于处理冲突
	ConflictIndex int // 冲突的日志条目索引，nextIndex[i] = ConflictIndex
	ConflictTerm  int // 冲突的日志条目任期号
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	rf.resetElectionTimer()

	if args.PrevLogIndex < rf.lastIncludedIndex || args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogTerm != rf.getLogTerm(args.PrevLogIndex) {
		conflictTerm := rf.getLogTerm(args.PrevLogIndex)
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex && rf.getLogTerm(i-1) == conflictTerm {
			i--
		}

		reply.Success = false
		reply.ConflictIndex = i
		reply.ConflictTerm = conflictTerm
		return
	}

	index := args.PrevLogIndex + 1
	i := 0
	for i < len(args.Entries) {
		if index+i <= rf.getLastLogIndex() {
			if rf.getLogTerm(index+i) != args.Entries[i].Term {
				// 发现冲突，截断并追加剩余条目
				rf.log = rf.log[:index+i-rf.lastIncludedIndex]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			// 需要追加新条目
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
		i++
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	leaderId := rf.me
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, peer *labrpc.ClientEnd) {
			rf.mu.Lock()
			if rf.killed() || rf.state != Leader || rf.currentTerm != currentTerm {
				rf.mu.Unlock()
				return
			}
			nextIndex := rf.nextIndex[i]
			if nextIndex <= rf.lastIncludedIndex {
				rf.mu.Unlock()
				go rf.sendInstallSnapshot(i, peer)
				return
			}

			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.getLogTerm(prevLogIndex)

			entries := []LogEntry{}
			if nextIndex <= rf.getLastLogIndex() {
				entries = append(entries, rf.log[nextIndex-rf.lastIncludedIndex:]...)
			}
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			ok := peer.Call("Raft.AppendEntries", &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.killed() || rf.state != Leader || rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if reply.Success {
				rf.matchIndex[i] = prevLogIndex + len(args.Entries)
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
					if rf.getLogTerm(N) != rf.currentTerm {
						continue
					}
					count := 1
					for j := range rf.peers {
						if j != rf.me && rf.matchIndex[j] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = N
						rf.applyCond.Signal()
						break
					}
				}
			} else {
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
			}
		}(i, peer)
	}
}
