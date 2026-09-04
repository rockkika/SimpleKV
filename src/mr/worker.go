package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.

func executeMap(mapf func(string, string) []KeyValue, taskID int, nReduce int, filename string) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	kva := mapf(filename, string(content))
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		buckets[reduceID] = append(buckets[reduceID], kv)
	}
	for reduceID, bucket := range buckets {
		tmp, err := os.CreateTemp(
			".",
			fmt.Sprintf("mr-%d-%d-*", taskID, reduceID),
		)
		if err != nil {
			return err
		}

		encoder := json.NewEncoder(tmp)

		for _, kv := range bucket {
			if err := encoder.Encode(&kv); err != nil {
				tmp.Close()
				os.Remove(tmp.Name())
				return err
			}
		}

		if err := tmp.Close(); err != nil {
			os.Remove(tmp.Name())
			return err
		}

		finalName := fmt.Sprintf(
			"mr-%d-%d",
			taskID,
			reduceID,
		)

		if err := os.Rename(tmp.Name(), finalName); err != nil {
			os.Remove(tmp.Name())
			return err
		}
	}

	return nil
}

func executeReduce(reducef func(string, []string) string, taskID int, nMap int, nReduce int) error {
	reduceID := taskID % nReduce
	var intermediate []KeyValue
	for mapID := 0; mapID < nMap; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)

		file, err := os.Open(filename)
		if err != nil {
			return err
		}

		decoder := json.NewDecoder(file)

		for {
			var kv KeyValue

			err := decoder.Decode(&kv)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				file.Close()
				return err
			}

			intermediate = append(intermediate, kv)
		}

		if err := file.Close(); err != nil {
			return err
		}
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	tmp, err := os.CreateTemp(
		".",
		fmt.Sprintf("mr-out-%d-*", reduceID),
	)
	if err != nil {
		return err
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) &&
			intermediate[j].Key == intermediate[i].Key {
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
			tmp.Close()
			os.Remove(tmp.Name())
			return err
		}

		i = j
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	finalName := fmt.Sprintf("mr-out-%d", reduceID)

	if err := os.Rename(tmp.Name(), finalName); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	return nil

}

func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		taskRequestArgs := TaskRequest{}
		taskReply := TaskReply{}

		ok := call("Coordinator.TaskAlloc", &taskRequestArgs, &taskReply)
		if ok {
			if taskReply.Type == TaskMap {
				err := executeMap(mapf, taskReply.TaskId, taskReply.NReduce, taskReply.Filename)
				if err != nil {
					return
				}
				resultRequestArgs := ResultRequest{
					TaskId:  taskReply.TaskId,
					TaskIdx: taskReply.TaskIdx,
				}
				resultReply := ResultReply{}
				call("Coordinator.TaskComplete", &resultRequestArgs, &resultReply)
			} else if taskReply.Type == TaskReduce {
				err := executeReduce(reducef, taskReply.TaskId, taskReply.NMap, taskReply.NReduce)
				if err != nil {
					return
				}
				resultRequestArgs := ResultRequest{
					TaskId:  taskReply.TaskId,
					TaskIdx: taskReply.TaskIdx,
				}
				resultReply := ResultReply{}
				call("Coordinator.TaskComplete", &resultRequestArgs, &resultReply)

			} else if taskReply.Type == TaskWait {
				time.Sleep(1 * time.Second)
			} else if taskReply.Type == TaskExit {
				return
			}
		} else {
			time.Sleep(time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
