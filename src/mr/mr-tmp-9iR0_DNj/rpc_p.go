//go:build ignore

package mr_tmp_9iR0_DNj

// 这个文件是 rpc.go 的对照版本。
//
// 对照改进：使用 build tag 排除默认构建。这样可以把完整参考实现放在
// 同一目录中阅读，而不会和当前实现中的同名类型发生重复定义。

type TaskType int

const (
	// 对照改进：只在第一项声明类型和 iota。后续项目会继承表达式，
	// 既能保证类型一致，也避免插入新枚举值时手工修改所有数字。
	TaskMap TaskType = iota
	TaskReduce
	TaskWait
	TaskExit
)

type TaskRequest struct{}

type TaskReply struct {
	// 对照改进：TaskID 只表示逻辑任务编号。Map 和 Reduce 的编号都分别
	// 从 0 开始，因此 Reduce Worker 不需要通过取模推导分区编号。
	TaskID int

	// 对照改进：Attempt 表示同一任务的第几次分配。任务超时重派后，
	// Coordinator 可以拒绝旧 Worker 迟到的完成通知。
	Attempt int

	Type     TaskType
	Filename string
	NMap     int
	NReduce  int
}

type TaskDoneArgs struct {
	TaskID  int
	Attempt int
	Type    TaskType
}

type TaskDoneReply struct {
	// 对照改进：明确告诉 Worker 此次完成通知是否属于当前有效执行。
	// 当前实验不强制 Worker 使用它，但这个字段让 RPC 语义更完整。
	Accepted bool
}
