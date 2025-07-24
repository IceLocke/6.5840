package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Task status.
type TaskStatus int8

const (
	Idle       TaskStatus = iota
	InProgress TaskStatus = 1
	Completed  TaskStatus = 2
)

type Coordinator struct {
	mu                   sync.Mutex
	NReduce              int // number of reduce tasks
	InputFiles           []string
	MapTasks             []TaskStatus
	MapTasksRemaining    int
	Intermediates        [][]int // dim: NReduce x len(MapTasks)
	ReduceTasks          []TaskStatus
	ReduceTasksRemaining int
}

// BuildMapTaskReply constructs a reply for a map task.
func (c *Coordinator) BuildMapTaskReply(reply *AssignTaskReply, mapTaskID int) {
	reply.TaskType = MapTask
	reply.NReduce = c.NReduce
	reply.MapTaskID = mapTaskID
	reply.ReduceTaskID = -1
	reply.Intermediates = nil
	reply.Filename = c.InputFiles[mapTaskID]
}

// BuildReduceTaskReply constructs a reply for a reduce task.
func (c *Coordinator) BuildReduceTaskReply(reply *AssignTaskReply, reduceTaskID int) {
	reply.TaskType = ReduceTask
	reply.NReduce = c.NReduce
	reply.MapTaskID = -1
	reply.ReduceTaskID = reduceTaskID
	reply.Intermediates = c.Intermediates[reduceTaskID]
	reply.Filename = ""
}

// Check whether a task is completed.
// It will update the task status if a worker is failed.
// i.e. no response after 10 seconds described in the lab page.
func (c *Coordinator) CheckTask(taskType int, mapTaskID int, reduceTaskID int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	switch taskType {
	case MapTask:
		if c.MapTasks[mapTaskID] == InProgress {
			c.MapTasks[mapTaskID] = Idle
		}
	case ReduceTask:
		if c.ReduceTasks[reduceTaskID] == InProgress {
			c.ReduceTasks[reduceTaskID] = Idle
		}
	}
}

//
// RPC handlers for the worker to call
//

// AssignTask assigns a task to a worker.
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.TaskType = UnknownTask
	if c.MapTasksRemaining > 0 {
		for i, status := range c.MapTasks {
			if status == Idle {
				c.BuildMapTaskReply(reply, i)
				c.MapTasks[i] = InProgress
				break
			}
		}
	} else if c.ReduceTasksRemaining > 0 {
		for i, status := range c.ReduceTasks {
			if status == Idle {
				c.BuildReduceTaskReply(reply, i)
				c.ReduceTasks[i] = InProgress
				break
			}
			//// Backup tasks, remaining <= 10% of NReduce
			// if c.ReduceTasksRemaining*10 <= c.NReduce && status == InProgress {
			// 	c.BuildMapTaskReply(reply, i)
			// 	break
			// }
		}
	} else if c.ReduceTasksRemaining == 0 && c.MapTasksRemaining == 0 {
		reply.TaskType = ExitTask
	}

	go c.CheckTask(reply.TaskType, reply.MapTaskID, reply.ReduceTaskID)
	return nil
}

// CompleteTask marks a task as completed.
// It updates the task status and remaining tasks.
func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if c.MapTasks[args.MapTaskID] == InProgress {
			c.MapTasks[args.MapTaskID] = Completed
			// Record the intermediate files for reduce tasks
			for _, reduceID := range args.Intermediates {
				c.Intermediates[reduceID] = append(c.Intermediates[reduceID], args.MapTaskID)
			}
			c.MapTasksRemaining--
		}
	case ReduceTask:
		if c.ReduceTasks[args.ReduceTaskID] == InProgress {
			c.ReduceTasks[args.ReduceTaskID] = Completed
			c.ReduceTasksRemaining--
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	done := c.MapTasksRemaining == 0 && c.ReduceTasksRemaining == 0
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.InputFiles = files
	c.NReduce = nReduce
	c.MapTasks = make([]TaskStatus, len(files))
	c.MapTasksRemaining = len(files)
	c.Intermediates = make([][]int, nReduce)
	for i := range c.Intermediates {
		c.Intermediates[i] = []int{}
	}
	c.ReduceTasks = make([]TaskStatus, nReduce)
	c.ReduceTasksRemaining = nReduce

	c.server()
	return &c
}
