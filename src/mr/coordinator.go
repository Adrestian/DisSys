package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mu              sync.Mutex
	nReduce         int
	files           []string        // input files
	mapTasks        map[string]bool // track if certain input file has been mapped or not
	mapTaskNumSoFar int             // number of map tasks generated so far
	mapTaskDone     int             // number of map tasks done so far
	nFiles          int             // total number of input files
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	reply.Filename = "test"
	reply.NReduce = c.nReduce
	reply.ShouldStop = false
	reply.JobName = "map"
	return nil
}

func (c *Coordinator) TaskDone(args *Args, reply *Reply) error {

	//jobName := args.JobName
	filenameDone := args.Filename
	//taskNum := args.TaskNum
	taskStatus := args.Status
	if taskStatus == MapTaskDone {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mapTasks[filenameDone] = true
		c.mapTaskDone++
	}

	return nil
}

//f
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Init Coordinator struct
	c := initCoordinator(files, nReduce)

	// Your code here.

	c.server()
	return c
}

func initCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:           files,
		nReduce:         nReduce,
		mapTasks:        make(map[string]bool),
		mapTaskNumSoFar: 0,
		mapTaskDone:     0,
	}

	for _, filenames := range files {
		c.mapTasks[filenames] = false
	}
	return &c
}
