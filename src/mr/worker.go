package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Task AssignTaskReply

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const (
	ExitTask    = -1
	MapTask     = 0
	ReduceTask  = 1
	UnknownTask = 999
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
WorkerLoop:
	for {
		switch task := CallAssignTask(); task.TaskType {
		case MapTask:
			intermediate := []KeyValue{}

			// Read and process the file for the map task
			file, err := os.Open(task.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", task.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Filename)
			}
			file.Close()
			kva := mapf(task.Filename, string(content))
			intermediate = append(intermediate, kva...)

			// Partition the intermediate key/value pairs into reduce tasks
			partitioned := make([][]KeyValue, task.NReduce)
			for i := range partitioned {
				partitioned[i] = []KeyValue{}
			}
			for _, kv := range intermediate {
				partitionID := ihash(kv.Key) % task.NReduce
				partitioned[partitionID] = append(partitioned[partitionID], kv)
			}
			for i := 0; i < task.NReduce; i++ {
				sort.Sort(ByKey(partitioned[i]))
			}

			// Write the partitioned intermediate files
			intermediates := []int{}
			for i := range partitioned {
				if len(partitioned[i]) > 0 {
					iname := fmt.Sprintf("mr-%d-%d", task.MapTaskID, i)
					ifile, _ := os.Create(iname)
					enc := json.NewEncoder(ifile)
					for _, kv := range partitioned[i] {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("cannot write to %v: %v", iname, err)
						}
					}
					ifile.Close()
					task.ReduceTaskID = i
					intermediates = append(intermediates, i)
				}
			}

			// Report completion of the map task
			task.Intermediates = intermediates
			CallCompleteTask(&task)
		case ReduceTask:
			kva := []KeyValue{}

			// Collect and sort all intermediate files for the reduce task
			for _, mapTaskID := range task.Intermediates {
				iname := fmt.Sprintf("mr-%d-%d", mapTaskID, task.ReduceTaskID)
				ifile, err := os.Open(iname)
				if err != nil {
					log.Fatalf("cannot open %v", iname)
				}
				dec := json.NewDecoder(ifile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))

			// oname is the temporarily output filename, needs to be set to the final one
			oname := fmt.Sprintf("mr-out-%d", task.ReduceTaskID)
			wd, err := os.Getwd()
			if err != nil {
				log.Fatalf("cannot get current working directory: %v", err)
			}
			ofile, err := os.CreateTemp(wd, oname)
			if err != nil {
				log.Fatalf("cannot create temporary file for %v: %v", oname, err)
			}

			// Reduce and output the collected key/value pairs
			for i := 0; i < len(kva); {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}

			// Rename the output filename
			ofile.Close()
			os.Rename(ofile.Name(), oname)
			CallCompleteTask(&task)
		case ExitTask:
			fmt.Printf("[Worker %d] All tasks completed, exit.\n", os.Getpid())
			break WorkerLoop
		case UnknownTask:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Call the coordinator to ask for a task.
func CallAssignTask() Task {
	args := AssignTaskArgs{os.Getpid()}
	reply := AssignTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("[Worker %d] Assigned task: TaskType=%d, MapTaskID=%d, ReduceTaskID=%d, Filename=%s\n",
			os.Getpid(), reply.TaskType, reply.MapTaskID, reply.ReduceTaskID, reply.Filename)
		return Task(reply)
	} else {
		log.Fatalf("CallAssignTask failed")
		return Task(AssignTaskReply{}) // unreachable, but keeps the compiler happy
	}
}

// Call the coordinator to report task completion.
func CallCompleteTask(task *Task) {
	args := CompleteTaskArgs{task.TaskType, task.MapTaskID, task.ReduceTaskID, task.Intermediates}
	reply := CompleteTaskReply{}

	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		fmt.Printf("[Worker %d] Completed and reported task: TaskType=%d, MapTaskID=%d, ReduceTaskID=%d\n",
			os.Getpid(), task.TaskType, task.MapTaskID, task.ReduceTaskID)
	} else {
		log.Fatalf("CallCompleteTask failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
