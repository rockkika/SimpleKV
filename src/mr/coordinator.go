package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	tasks           []Task
	N               int
	M               int
	mu              sync.Mutex
	mapCompleted    bool
	reduceCompleted bool
	index           int
	expire          time.Duration
	leftMapTask     int
	leftReduceTask  int
}

type TaskState int

const (
	IDLE TaskState = iota
	PROCESSING
	SUCCESS
)

type Role int

const (
	mapper Role = iota
	reducer
)

type Task struct {
	id         int
	startAt    time.Time
	state      TaskState
	file       string
	expectedId int
	role       Role
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) TaskAlloc(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var curRole Role
	if c.reduceCompleted {
		*reply = TaskReply{
			TaskId:   -1,
			TaskIdx:  -1,
			Filename: "",
			Type:     TaskExit,
			NReduce:  c.N,
			NMap:     c.M,
		}
		return nil
	}
	if c.mapCompleted {
		curRole = reducer
	} else {
		curRole = mapper
	}
	//alloc task
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].role != curRole {
			continue
		}
		if c.tasks[i].state == PROCESSING {
			if time.Now().Sub(c.tasks[i].startAt) > c.expire {
				c.tasks[i].state = IDLE
				c.tasks[i].expectedId = -1
			}
		}

		if c.tasks[i].state == IDLE {
			c.index++
			*reply = TaskReply{
				TaskIdx:  c.index,
				Filename: c.tasks[i].file,
				NReduce:  c.N,
				TaskId:   c.tasks[i].id,
				NMap:     c.M,
			}
			if curRole == mapper {
				reply.Type = TaskMap
			} else {
				reply.Type = TaskReduce
			}
			c.tasks[i].expectedId = c.index
			c.tasks[i].startAt = time.Now()
			c.tasks[i].state = PROCESSING
			return nil
		}
	}

	*reply = TaskReply{
		TaskId:   -1,
		TaskIdx:  -1,
		Filename: "",
		Type:     TaskWait,
		NReduce:  c.N,
		NMap:     c.M,
	}

	return nil
}

func (c *Coordinator) TaskComplete(args *ResultRequest, reply *ResultReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.mapCompleted {
		if c.tasks[args.TaskId].expectedId != args.TaskIdx {
			return nil
		}
		c.tasks[args.TaskId].state = SUCCESS
		c.leftMapTask--
		if c.leftMapTask == 0 {
			c.mapCompleted = true
		}
	} else {
		if c.tasks[args.TaskId].role == mapper {
			return nil
		}
		if c.tasks[args.TaskId].expectedId != args.TaskIdx {
			return nil
		}
		c.tasks[args.TaskId].state = SUCCESS
		c.leftReduceTask--
		if c.leftReduceTask == 0 {
			c.reduceCompleted = true
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reduceCompleted
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.N = nReduce
	c.M = len(files)
	c.leftReduceTask = nReduce
	c.leftMapTask = len(files)
	c.index = 0
	c.mapCompleted = false
	c.reduceCompleted = false
	c.expire = 10 * time.Second
	for i := 0; i < len(files); i++ {
		c.tasks = append(c.tasks, Task{
			id:    i,
			file:  files[i],
			role:  mapper,
			state: IDLE,
		})
	}
	for i := 0; i < nReduce; i++ {
		c.tasks = append(c.tasks, Task{
			id:    i + len(files),
			role:  reducer,
			state: IDLE,
		})

	}

	c.server(sockname)
	return &c
}
