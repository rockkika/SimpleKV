//go:build ignore

package mr_tmp_9iR0_DNj

import (
	"6.5840/mr"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string

func Worker(
	sockname string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	coordSockName = sockname

	for {
		task, ok := requestTask()
		if !ok {
			// 对照改进：call 不使用 log.Fatal。Coordinator 正常退出后，
			// 尚未收到 TaskExit 的 Worker 也能自行结束，而不是打印致命错误。
			return
		}

		switch task.Type {
		case mr.TaskMap:
			if err := executeMap(mapf, task); err != nil {
				// 对照改进：执行失败时不报告完成。Worker 退出后，
				// Coordinator 会在超时后把该任务重新分配给其他 Worker。
				log.Printf("map task %d failed: %v", task.TaskID, err)
				return
			}
			if !reportDone(task) {
				return
			}

		case mr.TaskReduce:
			if err := executeReduce(reducef, task); err != nil {
				log.Printf("reduce task %d failed: %v", task.TaskID, err)
				return
			}
			if !reportDone(task) {
				return
			}

		case mr.TaskWait:
			// 对照改进：等待后再轮询，避免没有空闲任务时形成 RPC 忙循环。
			time.Sleep(300 * time.Millisecond)

		case mr.TaskExit:
			return

		default:
			log.Printf("unknown task type %d", task.Type)
			return
		}
	}
}

func requestTask() (mr.TaskReply, bool) {
	args := mr.TaskRequest{}
	reply := mr.TaskReply{}
	ok := call("Coordinator.TaskAlloc", &args, &reply)
	return reply, ok
}

func reportDone(task mr.TaskReply) bool {
	args := TaskDoneArgs{
		TaskID:  task.TaskID,
		Attempt: task.Attempt,
		Type:    task.Type,
	}
	reply := TaskDoneReply{}
	return call("Coordinator.TaskComplete", &args, &reply)
}

func executeMap(
	mapf func(string, string) []KeyValue,
	task mr.TaskReply,
) error {
	if task.NReduce <= 0 {
		return fmt.Errorf("invalid reduce count %d", task.NReduce)
	}

	content, err := os.ReadFile(task.Filename)
	if err != nil {
		return fmt.Errorf("read %s: %w", task.Filename, err)
	}

	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range mapf(task.Filename, string(content)) {
		reduceID := ihash(kv.Key) % task.NReduce
		buckets[reduceID] = append(buckets[reduceID], kv)
	}

	for reduceID := range buckets {
		if err := writeIntermediateFile(
			task.TaskID,
			reduceID,
			buckets[reduceID],
		); err != nil {
			return err
		}
	}
	return nil
}

func writeIntermediateFile(mapID, reduceID int, values []KeyValue) error {
	// 对照改进：始终先写同目录临时文件，再 Rename 发布最终文件。
	// Reduce 因此只会看到完整文件，而不会读到 Map 写了一半的内容。
	tmp, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-*", mapID, reduceID))
	if err != nil {
		return fmt.Errorf("create map temp file: %w", err)
	}

	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}

	encoder := json.NewEncoder(tmp)
	for i := range values {
		if err := encoder.Encode(&values[i]); err != nil {
			cleanup()
			return fmt.Errorf("encode intermediate data: %w", err)
		}
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("close map temp file: %w", err)
	}

	finalName := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
	if err := os.Rename(tmp.Name(), finalName); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("publish %s: %w", finalName, err)
	}
	return nil
}

func executeReduce(
	reducef func(string, []string) string,
	task mr.TaskReply,
) error {
	// 对照改进：Reduce TaskID 本身就是列号，不再通过 taskID % nReduce
	// 间接推导，避免把“总任务数组下标”和“Reduce 分区号”混在一起。
	reduceID := task.TaskID

	var intermediate []KeyValue
	for mapID := 0; mapID < task.NMap; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
		values, err := readIntermediateFile(filename)
		if err != nil {
			return err
		}
		intermediate = append(intermediate, values...)
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	tmp, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-*", reduceID))
	if err != nil {
		return fmt.Errorf("create reduce temp file: %w", err)
	}

	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		if _, err := fmt.Fprintf(
			tmp,
			"%v %v\n",
			intermediate[i].Key,
			output,
		); err != nil {
			cleanup()
			return fmt.Errorf("write reduce output: %w", err)
		}

		i = j
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("close reduce temp file: %w", err)
	}

	finalName := fmt.Sprintf("mr-out-%d", reduceID)
	if err := os.Rename(tmp.Name(), finalName); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("publish %s: %w", finalName, err)
	}
	return nil
}

func readIntermediateFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", filename, err)
	}
	defer file.Close()

	var values []KeyValue
	decoder := json.NewDecoder(file)
	for {
		var kv KeyValue
		err := decoder.Decode(&kv)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", filename, err)
		}
		values = append(values, kv)
	}
	return values, nil
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return false
	}
	defer client.Close()

	if err := client.Call(rpcname, args, reply); err != nil {
		log.Printf("%d: RPC %s failed: %v", os.Getpid(), rpcname, err)
		return false
	}
	return true
}
