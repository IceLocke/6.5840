package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

//
// RPC definitions for the coordinator to communicate with workers.
// All RPCs are from workers to coordinator.
//

// Worker -> Coordinator, ask for a task
type AssignTaskArgs struct {
	WorkerID int
}

// Coordinator -> Worker, reply with a task.
// MapTaskID is given with a KV pair if the worker is assigned a map task,
// ReduceTaskID and MapTaskID are both given if the worker is assigned a reduce task.
type AssignTaskReply struct {
	TaskType      int // 0 for map task, 1 for reduce task
	NReduce       int // number of reduce tasks
	MapTaskID     int
	ReduceTaskID  int
	Intermediates []int // prefixes of intermediate files for reduce task, i.e. map task IDs
	Filename      string   // filename for map task
}

// Worker -> Coordinator, report that a task is complete
type CompleteTaskArgs struct {
	TaskType      int // 0 for map task, 1 for reduce task
	MapTaskID     int
	ReduceTaskID  int
}

// Coordinator -> Worker, reply to task completion
type CompleteTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
