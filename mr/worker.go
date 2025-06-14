package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// 键值对，string to string
type KeyValue struct {
	Key   string
	Value string
}

// 计算哈希值
func ihash(key string) int {
	// 使用fnv哈希函数
	// 将字符串转换为字节切片，并写入哈希函数
	// 取模，确保哈希值为正整数
	h := fnv.New32()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func sendHeartbeat() (bool, *Reply) {
	args := Args{} // 第一次连接，不需要发送任何数据过去
	reply := Reply{}
	ok := call("Coordinator.Heartbeat", &args, &reply)
	if !ok {
		log.Printf("Worker: failed to send heartbeat to Coordinator\n")
		return false, nil
	}
	return true, &reply
}

func doMapTask(mapFunction func(string, string) []KeyValue, reply *Reply) bool {
	args := &Args{
		TaskId: reply.TaskId,
	}

	// Read input file
	content, err := os.ReadFile(reply.MapFile)
	if err != nil {
		log.Printf("Worker: failed to read file %s: %v\n", reply.MapFile, err)
		return false
	}

	// Apply map function
	keyValues := mapFunction(reply.MapFile, string(content))

	// Partition key-value pairs
	partitions := make([][]KeyValue, reply.NumReduce)
	for _, kv := range keyValues {
		index := ihash(kv.Key) % reply.NumReduce
		partitions[index] = append(partitions[index], kv)
	}

	// Write each partition to a JSON file
	for i := 0; i < reply.NumReduce; i++ {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
		args.MapResFile = append(args.MapResFile, intermediateFilename)

		// Marshal the key-value pairs to JSON
		jsonData, err := json.Marshal(partitions[i])
		if err != nil {
			log.Printf("Worker: failed to marshal JSON for file %s: %v\n", intermediateFilename, err)
			return false
		}

		// Write JSON data to file atomically
		err = atomicWriteFile(intermediateFilename, jsonData)
		if err != nil {
			log.Printf("Worker: failed to write to file %s: %v\n", intermediateFilename, err)
			return false
		}
	}

	args.Succeeded = true
	ok := call("Coordinator.Report", args, &Reply{})
	if !ok {
		log.Printf("Worker: failed to report task completion to Coordinator\n")
		return false
	}
	return true
}

func atomicWriteFile(filename string, data []byte) error {
	tempFile, err := os.CreateTemp("", "temp_*")
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())

	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return err
	}

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	// First try to remove the old file
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return err
	}
	// Then rename the temp file to the target file
	return os.Rename(tempFile.Name(), filename)
}

func doReduceTask(reduceFunction func(string, []string) string, reply *Reply) bool {
	// Map to collect all values for each key
	tempMap := make(map[string][]string)

	// Process each intermediate file
	for _, filename := range reply.ReduceFile {
		// Read the file
		jsonData, err := os.ReadFile(filename)
		if err != nil {
			log.Printf("Worker: failed to read file %s: %v\n", filename, err)
			return false
		}

		// Unmarshal JSON data
		var keyValues []KeyValue
		if err := json.Unmarshal(jsonData, &keyValues); err != nil {
			log.Printf("Worker: failed to unmarshal JSON from file %s: %v\n", filename, err)
			return false
		}

		// Group values by key
		for _, kv := range keyValues {
			tempMap[kv.Key] = append(tempMap[kv.Key], kv.Value)
		}
	}

	// Sort the keys
	var keys []string
	for k := range tempMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Apply reduce function and create output
	outputFileName := fmt.Sprintf("mr-out-%d", reply.TaskId)
	var builder strings.Builder
	for _, key := range keys {
		reduceValue := reduceFunction(key, tempMap[key])
		builder.WriteString(fmt.Sprintf("%v %v\n", key, reduceValue))
	}

	// Write output file atomically
	err := atomicWriteFile(outputFileName, []byte(builder.String()))
	if err != nil {
		log.Printf("Worker: failed to write to file %s %v\n", outputFileName, err)
		return false
	}

	// Report task completion
	ok := call("Coordinator.Report", &Args{TaskId: reply.TaskId, Succeeded: true}, &Reply{})
	if !ok {
		log.Printf("Worker: failed to report task completion to Coordinator\n")
		return false
	}
	return true
}

func Worker(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {
	for {
		heartbeatSuccess, reply := sendHeartbeat()
		if !heartbeatSuccess {
			return
		}
		switch reply.WorkerStatus {
		case MapTask:
			if doMapTask(mapFunction, reply) {
				continue
			}
		case ReduceTask:
			if doReduceTask(reduceFunction, reply) {
				continue
			}
		case WaitingTask:
			time.Sleep(1 * time.Second)
			continue
		case CompletedTask:
			return
		default:
			log.Printf("Worker: unknown task type %d, exiting.\n", reply.WorkerStatus)
			return
		}
		// 如果任务执行失败，发送失败报告
		args := &Args{TaskId: reply.TaskId, Succeeded: false}
		call("Coordinator.Report", args, &Reply{})
		return
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
