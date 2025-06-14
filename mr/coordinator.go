package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskStatus int

const (
	idle taskStatus = iota
	running
	completed
)

type task struct {
	mapFile    string
	reduceFile []string
	taskId     int
	startTime  time.Time
	taskStatus taskStatus
}

type heartbeatMsg struct {
	reply *Reply
	ok    chan struct{}
}

type reportMsg struct {
	args *Args
	ok   chan struct{}
}

type coordinatorPhase int

const (
	mapPhase coordinatorPhase = iota
	reducePhase
	allDonePhase
)

type Coordinator struct {
	inputFiles        []string          // 输入文件列表
	numReduce         int               // reduce任务数量（即中间文件数量）
	numMap            int               // map任务数量（即输入文件数量）
	intermediateFiles [][]string        // 每个worker在map结束后生成的中间文件列表（二维数组）
	currentPhase      coordinatorPhase  // 当前阶段（map或reduce）
	tasks             []task            // 任务列表
	heartbeatChan     chan heartbeatMsg // 心跳通道
	reportChan        chan reportMsg    // 报告通道
	doneChan          chan struct{}     // 完成通道（使用空结构体）
	lastSubmitTime    time.Time         // 最后一次提交时间
	lock              sync.Mutex        // 锁
	runStarted        bool              // 是否已经启动
}

func (c *Coordinator) server() {
	rpc.Register(c)

	rpc.HandleHTTP()

	os.Remove(coordinatorSock())
	l, e := net.Listen("unix", coordinatorSock())
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.currentPhase == allDonePhase
}

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

func (c *Coordinator) Heartbeat(args *Args, reply *Reply) error {
	c.lock.Lock()
	if !c.runStarted {
		c.runStarted = true
		go c.run()
	}
	c.lock.Unlock()

	ok := make(chan struct{})
	hb := heartbeatMsg{
		reply: reply,
		ok:    ok,
	}
	c.heartbeatChan <- hb
	<-ok
	return nil
}

func (c *Coordinator) Report(args *Args, reply *Reply) error {
	ok := make(chan struct{})
	rp := reportMsg{
		args: args,
		ok:   ok,
	}
	c.reportChan <- rp
	<-ok
	return nil
}

func (c *Coordinator) handleHeartbeat(hb heartbeatMsg) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.currentPhase == allDonePhase {
		hb.reply.WorkerStatus = CompletedTask
		hb.ok <- struct{}{}
		return
	}

	// 找到第一个空闲的任务
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].taskStatus == idle {
			c.tasks[i].taskStatus = running   // 设置任务状态为进行中
			c.tasks[i].startTime = time.Now() // 设置开始时间

			hb.reply.NumReduce = c.numReduce    // 设置reduce的数量
			hb.reply.TaskId = c.tasks[i].taskId // 设置回复响应的任务编号
			if c.currentPhase == mapPhase {     // 如果当前阶段是map任务
				hb.reply.WorkerStatus = MapTask // 设置任务类型为map
				hb.reply.MapFile = c.tasks[i].mapFile
			} else if c.currentPhase == reducePhase { // 如果当前阶段是reduce任务
				hb.reply.WorkerStatus = ReduceTask          // 设置任务类型为reduce
				hb.reply.ReduceFile = c.tasks[i].reduceFile // 设置reduce的文件列表
			}
			hb.ok <- struct{}{} // 通知Heartbeat函数，心跳消息处理完毕
			return
		}
	}

	hb.reply.WorkerStatus = WaitingTask // 设置任务类型为等待
	hb.ok <- struct{}{}
}

func (c *Coordinator) handleReport(rp reportMsg) {
	c.lock.Lock()
	defer c.lock.Unlock()

	taskId := rp.args.TaskId
	if taskId < 0 || taskId >= len(c.tasks) {
		log.Fatalf("Coordinator: invalid task id %d\n", taskId)
		rp.ok <- struct{}{} // 通知Report函数，报告消息处理完毕
		return
	}

	// worker发过来的任务完成报告，如果map或reduce任务成功完成
	if rp.args.Succeeded {
		c.tasks[taskId].taskStatus = completed  // 设置任务状态为完成
		c.tasks[taskId].startTime = time.Time{} // 清空开始时间
		if c.currentPhase == mapPhase {
			if taskId < len(c.intermediateFiles) {
				c.intermediateFiles[taskId] = rp.args.MapResFile
			} else {
				log.Printf("TaskId %d out of range for intermediateFiles (len %d)", taskId, len(c.intermediateFiles))
			}
		}
	} else {
		c.tasks[taskId].taskStatus = idle       // 设置任务状态为空闲，允许重新分配
		c.tasks[taskId].startTime = time.Time{} // 清空开始时间
	}

	// 检查所有任务是否完成
	allDone := true
	for _, task := range c.tasks {
		if task.taskStatus != completed {
			allDone = false
			break
		}
	}

	if allDone {
		if c.currentPhase == mapPhase {
			c.currentPhase = reducePhase // 切换到reduce阶段
			c.initReduceTasks()          // 初始化reduce任务列表
		} else {
			c.currentPhase = allDonePhase // 设置当前阶段为所有任务完成
			close(c.doneChan)             // 关闭完成通道，通知run函数退出
		}
	}
	c.lastSubmitTime = time.Now() // 更新最后一次提交时间
	rp.ok <- struct{}{}           // 通知Report函数，报告消息处理完毕
}

func (c *Coordinator) initReduceTasks() {

	c.tasks = make([]task, c.numReduce)
	for i := 0; i < c.numReduce; i++ {
		var reduceFiles []string
		for j := 0; j < c.numMap; j++ {
			reduceFiles = append(reduceFiles, c.intermediateFiles[j][i])
		}
		c.tasks[i] = task{
			reduceFile: reduceFiles,
			taskId:     i,
			startTime:  time.Time{},
			taskStatus: idle,
		}
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:        files,
		numReduce:         nReduce,
		numMap:            len(files),
		intermediateFiles: make([][]string, len(files)),
		currentPhase:      mapPhase,
		heartbeatChan:     make(chan heartbeatMsg),
		reportChan:        make(chan reportMsg),
		doneChan:          make(chan struct{}),
		lastSubmitTime:    time.Now(),
		runStarted:        false,
	}

	c.initMapTasks() // 初始化map任务列表
	c.server()
	go c.checkTimeout() // 启动超时检查协程
	return &c
}

func (c *Coordinator) initMapTasks() {
	c.tasks = make([]task, c.numMap)
	for i, file := range c.inputFiles {
		c.tasks[i] = task{
			mapFile:    file,
			taskId:     i,
			startTime:  time.Time{},
			taskStatus: idle,
		}
	}
}

func (c *Coordinator) checkTimeout() {
	const timeout = 10 * time.Second // 设置超时时间为10秒
	for {
		time.Sleep(time.Second)

		if c.lock.TryLock() {
			if c.currentPhase == allDonePhase {
				c.lock.Unlock()
				return
			}

			now := time.Now()
			for i := 0; i < len(c.tasks); i++ {
				if c.tasks[i].taskStatus == running && now.Sub(c.tasks[i].startTime) > timeout {
					c.tasks[i].taskStatus = idle // 设置任务状态为空闲
					c.tasks[i].startTime = time.Time{}
				}
			}
			c.lock.Unlock()
		}
	}
}
