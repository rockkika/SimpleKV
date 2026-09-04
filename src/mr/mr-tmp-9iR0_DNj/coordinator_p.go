//go:build ignore

package mr_tmp_9iR0_DNj

import (
	"6.5840/mr"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const taskTimeout = 10 * time.Second

type taskState int

const (
	taskIdle taskState = iota
	taskRunning
	taskFinished
)

type jobPhase int

const (
	mapPhase jobPhase = iota
	reducePhase
	donePhase
)

type task struct {
	id        int
	filename  string
	state     taskState
	attempt   int
	startedAt time.Time
}

type Coordinator struct {
	// 对照改进：一把锁保护整个调度状态是有意为之。RPC handler 在锁内
	// 只做短小的内存操作，不进行文件 I/O、网络调用或 Sleep，因此单锁
	// 更容易保证“任务状态 + 阶段切换”的原子性。
	mu sync.Mutex

	// 对照改进：显式阶段比 mapCompleted/reduceCompleted 两个布尔值更好，
	// 因为它不可能表达出互相矛盾的组合。
	phase jobPhase

	// 对照改进：Map 和 Reduce 任务分开保存。两类任务的 TaskID 都可以
	// 从 0 开始，也就不再混用“任务 ID”和“总 tasks 切片下标”。
	mapTasks    []task
	reduceTasks []task

	nMap    int
	nReduce int
}

func (c *Coordinator) TaskAlloc(_ *mr.TaskRequest, reply *mr.TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 对照改进：用循环在一次 RPC 中完成阶段推进。最后一个 Map 已完成时，
	// 请求任务的 Worker 可以立刻拿到 Reduce，而不必先收到一次 TaskWait。
	for {
		switch c.phase {
		case mapPhase:
			c.expireTasksLocked(c.mapTasks)

			if c.assignTaskLocked(c.mapTasks, mr.TaskMap, reply) {
				return nil
			}
			if c.allFinishedLocked(c.mapTasks) {
				c.phase = reducePhase
				continue
			}

			reply.Type = mr.TaskWait
			return nil

		case reducePhase:
			c.expireTasksLocked(c.reduceTasks)

			if c.assignTaskLocked(c.reduceTasks, mr.TaskReduce, reply) {
				return nil
			}
			if c.allFinishedLocked(c.reduceTasks) {
				c.phase = donePhase
				continue
			}

			reply.Type = mr.TaskWait
			return nil

		case donePhase:
			reply.Type = mr.TaskExit
			return nil
		}
	}
}

func (c *Coordinator) TaskComplete(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tasks := c.tasksForTypeLocked(args.Type)
	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return nil
	}

	current := &tasks[args.TaskID]

	// 对照改进：完成 RPC 是幂等的。若 Coordinator 已接受这次执行，
	// Worker 因回复丢失而重发时，仍会收到 Accepted=true，且不会重复计数。
	if current.state == taskFinished && current.attempt == args.Attempt {
		reply.Accepted = true
		return nil
	}

	// 对照改进：同时检查状态和 attempt。仅比较 attempt 仍可能让重复完成
	// 再次修改全局统计；仅检查状态则无法识别超时后迟到的旧 Worker。
	if current.state != taskRunning || current.attempt != args.Attempt {
		return nil
	}

	current.state = taskFinished
	reply.Accepted = true
	c.advancePhaseLocked()
	return nil
}

func (c *Coordinator) assignTaskLocked(tasks []task, typ mr.TaskType, reply *mr.TaskReply) bool {
	for i := range tasks {
		current := &tasks[i]
		if current.state != taskIdle {
			continue
		}

		current.state = taskRunning
		current.attempt++
		current.startedAt = time.Now()

		*reply = mr.TaskReply{
			TaskID:   current.id,
			Attempt:  current.attempt,
			Type:     typ,
			Filename: current.filename,
			NMap:     c.nMap,
			NReduce:  c.nReduce,
		}
		return true
	}
	return false
}

func (c *Coordinator) expireTasksLocked(tasks []task) {
	now := time.Now()
	for i := range tasks {
		current := &tasks[i]
		if current.state == taskRunning &&
			now.Sub(current.startedAt) > taskTimeout {
			// 对照改进：不需要把 attempt 重置为特殊值。下一次分配时递增
			// attempt，自然会让旧 Worker 的完成通知失效。
			current.state = taskIdle
		}
	}
}

func (c *Coordinator) allFinishedLocked(tasks []task) bool {
	for i := range tasks {
		if tasks[i].state != taskFinished {
			return false
		}
	}
	return true
}

func (c *Coordinator) tasksForTypeLocked(typ mr.TaskType) []task {
	switch typ {
	case mr.TaskMap:
		return c.mapTasks
	case mr.TaskReduce:
		return c.reduceTasks
	default:
		return nil
	}
}

func (c *Coordinator) advancePhaseLocked() {
	if c.phase == mapPhase && c.allFinishedLocked(c.mapTasks) {
		c.phase = reducePhase
	}
	if c.phase == reducePhase && c.allFinishedLocked(c.reduceTasks) {
		c.phase = donePhase
	}
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 对照改进：Done 和 RPC handlers 使用同一把锁，避免 race detector
	// 检测到 reduce/done 状态的并发读写。
	return c.phase == donePhase
}

func (c *Coordinator) server(sockname string) {
	// 对照改进：使用独立的 rpc.Server 和 ServeMux，避免修改 net/rpc 与
	// net/http 的全局注册表，也让同一测试进程创建多个 Coordinator 更安全。
	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(c); err != nil {
		log.Fatalf("register coordinator: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	if err := os.Remove(sockname); err != nil && !os.IsNotExist(err) {
		log.Fatalf("remove stale socket %s: %v", sockname, err)
	}

	listener, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatalf("listen error %s: %v", sockname, err)
	}

	go func() {
		if err := http.Serve(listener, mux); err != nil {
			log.Printf("coordinator RPC server stopped: %v", err)
		}
	}()
}

func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		phase:       mapPhase,
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]task, len(files)),
		reduceTasks: make([]task, nReduce),
	}

	for id, filename := range files {
		c.mapTasks[id] = task{
			id:       id,
			filename: filename,
			state:    taskIdle,
		}
	}
	for id := range c.reduceTasks {
		c.reduceTasks[id] = task{
			id:    id,
			state: taskIdle,
		}
	}

	// 对照改进：初始化时也推进空阶段，避免零输入或零 Reduce 时永远等待。
	c.advancePhaseLocked()
	c.server(sockname)
	return c
}
