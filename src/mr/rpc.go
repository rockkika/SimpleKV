package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	TaskMap    TaskType = iota
	TaskReduce TaskType = 1
	TaskWait   TaskType = 2
	TaskExit   TaskType = 3
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskRequest struct {
}

type TaskReply struct {
	TaskIdx  int
	Type     TaskType
	Filename string
	NReduce  int
	TaskId   int
	NMap     int
}

type ResultRequest struct {
	TaskId  int
	TaskIdx int
}

type ResultReply struct {
}

// Add your RPC definitions here.
