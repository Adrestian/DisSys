package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	IDLE        string = "idle"
	IN_PROGRESS string = "in_progress"
	DONE        string = "done"
)

type Coordinator struct {
	mu        sync.Mutex
	nReduce   int
	filenames []string // input files

	mapTaskCount         int               // equal to the number of input files, should never change
	mapTasks             map[string]string // track if certain input file has been mapped or not
	mapTasksDone         bool
	mapTaskToNumber      map[string]int // track the number of map tasks for each input file, map filename to task number
	finishedMapTaskCount int            // number of map tasks that have finished

	reduceTaskCount int            // equal to the number of reduce tasks, which is nReduce
	reduceTasks     map[int]string // track the progress of reduce tasks
	reduceTaskDone  bool

	allTaskDone bool
}

func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	// Need to cleanup exisiting mess on the filesystem?
	// get a task from the task pool, and return to worker

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.allTaskDone || (c.mapTasksDone && c.reduceTaskDone) { // redundunt check, but safe
		// Tell workers to exit
		reply.ShouldStop = true
		return errors.New("All tasks are done")
	}
	if !c.mapTasksDone { // there are still map tasks to do
		for filename, status := range c.mapTasks {
			if status == IDLE { // give worker a task
				reply.TaskName = MAP_TASK
				reply.Filename = filename
				reply.NReduce = c.nReduce
				reply.TaskNum = c.mapTaskToNumber[filename]
				reply.ShouldStop = false

				c.mapTasks[filename] = IN_PROGRESS // update the task status
				return nil
			}
		}
	} else if !c.reduceTaskDone { // there are still reduce tasks to do
		// TODO:
	}

	return nil
}

func (c *Coordinator) TaskDone(args *Args, reply *Reply) error {
	log.Println(args)

	taskName := args.TaskName
	taskNumber := args.TaskNum
	filenameDone := args.Filename
	taskStatus := args.Status
	if taskStatus == MapTaskDone {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok := c.mapTasks[filenameDone]; ok {
			c.mapTasks[filenameDone] = DONE
			c.finishedMapTaskCount++
			if c.finishedMapTaskCount == c.mapTaskCount {
				c.mapTasksDone = true
			}
		} else {
			log.Printf("[TaskDone]: Worker Reply With A Non-Existent Filename. Task: %v, Filename: %v, Task#: %v, Status: %v \n", taskName, filenameDone, taskNumber, taskStatus)
		}

	} else if taskStatus == ReduceTaskDone {
		// TODO:
	} else {
		log.Printf("[TaskDone]: Unknown Task Status: %v\n", taskStatus)
	}

	if c.mapTasksDone && c.reduceTaskDone { // if all the tasks are done, tell workers to exit
		c.allTaskDone = true
		reply.ShouldStop = true
	}

	return nil
}

func (c *Coordinator) TaskError(args *Args, reply *Reply) error {
	errJobName := args.TaskName
	errFilename := args.Filename
	errTaskNum := args.TaskNum
	errMessage := args.Message
	log.Printf("[Worker Error] Task: %v, Filename: %v, Task#: %v, Error Message: %v \n",
		errJobName, errFilename, errTaskNum, errMessage)

	// IMPORTANT
	c.mu.Lock()
	defer c.mu.Unlock()

	if errJobName == MAP_TASK {
		// First make sure the error filename is actually in the map, otherwise there's something wring
		if _, ok := c.mapTasks[errFilename]; ok {
			c.mapTasks[errFilename] = IDLE // reset the task status
		} else {
			log.Printf("[Worker Error]: Worker Reply With A Non-Existent Filename. Task: %v, Filename: %v, Task#: %v, Error Message: %v \n", errJobName, errFilename, errTaskNum, errMessage)
		}
	} else if errJobName == REDUCE_TASK {
		// TODO:
	} else {
		log.Printf("[Worker Fatal Error]: Unknown Task Name: %v\n", errJobName)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.allTaskDone || (c.mapTasksDone && c.reduceTaskDone) {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(filenames []string, nReduce int) *Coordinator {
	// Init Coordinator struct
	c := initCoordinator(filenames, nReduce)

	// Your code here.

	c.server()
	return c
}

func initCoordinator(filenames []string, nReduce int) *Coordinator {
	c := Coordinator{
		filenames:       filenames,
		nReduce:         nReduce,
		mapTaskCount:    len(filenames),
		reduceTaskCount: nReduce,
		mapTasks:        make(map[string]string),
		mapTaskToNumber: make(map[string]int),
		mapTasksDone:    false,
		reduceTaskDone:  false,
		allTaskDone:     false,
	}

	// initialize mapTasks
	for i, filename := range filenames {
		c.mapTasks[filename] = IDLE
		c.mapTaskToNumber[filename] = i
	}

	// initialize reduceTasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = IDLE
	}

	return &c
}
