package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

var (
	nReduce    int
	shouldStop bool
)

const (
	FILENAME_FORMAT = "mr-%d-%d"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
// https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// One way to get started is to modify mr/worker.go's Worker()
	// to send an RPC to the coordinator asking for a task.
	// Then modify the coordinator to respond with the file name of an as-yet-unstarted map task.
	// Then modify the worker to read that file and call the application Map function, as in mrsequential.go.

	// CallExample()

	for !shouldStop {
		// Get a task from coordinator
		task := getTask()
		if nReduce != 0 && task.NReduce != nReduce {
			log.Printf("nReduce changed from %v to %v \n", nReduce, task.NReduce)
		}
		nReduce = task.NReduce

		mapTaskNum := task.TaskNum
		if task.ShouldStop {
			shouldStop = true
			break
		}

		if task.JobName == "map" {
			filename := task.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))

			outFilenames, encoders := getOutputEncoders(mapTaskNum, nReduce)
			for _, kv := range kva {
				keyHash := ihash(kv.Key)
				N := keyHash % nReduce
				err := encoders[N].Encode(&kv)
				if err != nil {
					log.Fatalf("Problem encode %v to file: %v\n", kv, filename)
				}
			}
			// Finished writing to file, close all files
			for _, outf := range outFilenames {
				outf.Close()
			}

			// Notify coordinator that map task is done

			args := Args{JobName: "map", TaskNum: mapTaskNum, Filename: filename, Status: MapTaskDone}

			notifyCoordinator(args)

		} else if task.JobName == "reduce" {
			// TODO:

		} else {
			fmt.Println("Unknown job name")
		}
		time.Sleep(time.Second)
	}

}

// TODO:
func getOutputEncoders(mapTaskNum, nReduce int) ([]*os.File, []*json.Encoder) {
	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf(FILENAME_FORMAT, mapTaskNum, i)
		file, err := os.Create(filename)

		if err != nil {
			log.Fatalf("cannot open %v\n", filename)
		}
		files[i] = file
		encoders[i] = json.NewEncoder(file)
	}
	return files, encoders
}

// Get one task from the coordinator
func getTask() Reply {
	args := Args{}
	reply := Reply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply
}

func notifyCoordinator(args Args) {
	reply := Reply{}
	call("Coordinator.TaskDone", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {
// 	// declare an argument structure.
// 	args := ExampleArgs{}
// 	// fill in the argument(s).
// 	args.X = 99
// 	// declare a reply structure.
// 	reply := ExampleReply{}
// 	// send the RPC request, wait for the reply.
// 	call("Coordinator.Example", &args, &reply)
// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args *Args, reply *Reply) bool {
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

	log.Println("[RPC]: ", err)
	return false
}
