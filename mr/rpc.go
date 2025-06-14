package mr

import (
	"os"
	"strconv"
)

type WorkerPhase int

const (
	MapTask       WorkerPhase = iota // map任务
	ReduceTask                       // reduce任务
	WaitingTask                      // 等待任务
	CompletedTask                    // 完成任务
)

type Reply struct {
	TaskId       int         // 本次任务编号，和worker的任务编号一致
	NumReduce    int         // reduce的数量（即中间文件多少份）
	WorkerStatus WorkerPhase // 本次worker的任务类型
	MapFile      string      //map任务的文件名
	ReduceFile   []string    // reduce任务的文件名
}

type Args struct {
	TaskId     int      // 本次任务编号，和worker的任务编号一致
	Succeeded  bool     // 本次任务是否成功
	MapResFile []string // worker在map结束后，生成的文件列表
}

// 返回一个唯一的socket文件名：/var/tmp/mr-<uid>
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Geteuid())
	return s
}
