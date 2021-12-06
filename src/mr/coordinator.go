package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE        string        = "idle"
	IN_PROGRESS string        = "in_progress"
	DONE        string        = "done"
	TIMEOUT     time.Duration = time.Second * 10
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
	//reduceTaskToNumber map[int]int    // track the number of reduce tasks for each input file, map filename to task number
	reduceTaskDone          bool
	finishedReduceTaskCount int

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

				go func(mapTaskFilename string) {
					<-time.After(TIMEOUT)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.mapTasks[mapTaskFilename] == IN_PROGRESS {
						c.mapTasks[mapTaskFilename] = IDLE
					}
				}(filename)

				return nil
			}
		}
	} else if !c.reduceTaskDone { // there are still reduce tasks to do
		for taskNumber, status := range c.reduceTasks {
			if status == IDLE { // give worker a task
				reply.TaskName = REDUCE_TASK
				reply.NReduce = c.nReduce
				reply.TaskNum = taskNumber
				reply.ShouldStop = false
				reply.MapTaskCount = c.mapTaskCount

				c.reduceTasks[taskNumber] = IN_PROGRESS // update the task status

				// If worker doesn't reply in 10 seconds, assume worker's crashed anD reschedule the reduce task
				go func(reduceTaskNumber int) {
					<-time.After(TIMEOUT)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.reduceTasks[reduceTaskNumber] == IN_PROGRESS {
						c.reduceTasks[reduceTaskNumber] = IDLE
					}
				}(taskNumber)

				return nil
			}
		}
	}

	return nil
}

func (c *Coordinator) TaskDone(args *Args, reply *Reply) error {
	log.Println("[Task Done]: ", args)

	taskName := args.TaskName
	taskNumber := args.TaskNum
	filenameDone := args.Filename
	taskStatus := args.Status

	c.mu.Lock()
	defer c.mu.Unlock()

	if taskStatus == MapTaskDone {
		if status, ok := c.mapTasks[filenameDone]; status == IN_PROGRESS && ok {
			c.mapTasks[filenameDone] = DONE
			c.finishedMapTaskCount++
			if c.finishedMapTaskCount == c.mapTaskCount {
				c.mapTasksDone = true
			}
		} else {
			log.Printf("[TaskDone]: Worker Reply With A Non-Existent Filename. Task: %v, Filename: %v, Task#: %v, Status: %v \n", taskName, filenameDone, taskNumber, taskStatus)
		}

	} else if taskStatus == ReduceTaskDone {
		if status, ok := c.reduceTasks[taskNumber]; status == IN_PROGRESS && ok {
			c.reduceTasks[taskNumber] = DONE
			c.finishedReduceTaskCount++
			if c.finishedReduceTaskCount == c.nReduce {
				c.reduceTaskDone = true
			}
		} else {
			log.Printf("[TaskDone]: Worker Reply With A Non-Existent Filename. Task: %v, Filename: %v, Task#: %v, Status: %v \n", taskName, filenameDone, taskNumber, taskStatus)
		}
	} else {
		log.Printf("[TaskDone]: Unknown Task Status: %v\n", taskStatus)
		log.Println(args)
	}

	reply.ShouldStop = false

	if c.mapTasksDone && c.reduceTaskDone { // if all the tasks are done, tell workers to exit
		c.allTaskDone = true
		reply.ShouldStop = true
	}

	return nil
}

func (c *Coordinator) TaskError(args *Args, reply *Reply) error {
	errTaskName := args.TaskName
	errFilename := args.Filename
	errTaskNum := args.TaskNum
	errMessage := args.Message
	log.Printf("[Worker Error] Task: %v, Filename: %v, Task#: %v, Error Message: %v \n",
		errTaskName, errFilename, errTaskNum, errMessage)

	// IMPORTANT
	c.mu.Lock()
	defer c.mu.Unlock()

	if errTaskName == MAP_TASK {
		// First make sure the error filename is actually in the map, otherwise there's something wring
		if _, ok := c.mapTasks[errFilename]; ok {
			c.mapTasks[errFilename] = IDLE // reset the task status
		} else {
			log.Printf("[Worker Error]: Worker Reply With A Non-Existent Filename. Task: %v, Filename: %v, Task#: %v, Error Message: %v \n", errTaskName, errFilename, errTaskNum, errMessage)
		}
	} else if errTaskName == REDUCE_TASK {
		if _, ok := c.reduceTasks[errTaskNum]; ok {
			c.reduceTasks[errTaskNum] = IDLE // reset the task status
		} else {
			log.Printf("[Worker Error]: Worker Reply With A Non-Existent Filename. Task: %v, Filename: %v, Task#: %v, Error Message: %v \n", errTaskName, errFilename, errTaskNum, errMessage)
		}
	} else {
		log.Printf("[Worker Fatal Error]: Unknown Task Name: %v\n", errTaskName)
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
		filenames: filenames,
		nReduce:   nReduce,

		mapTaskCount:         len(filenames),
		mapTasks:             make(map[string]string),
		mapTaskToNumber:      make(map[string]int),
		mapTasksDone:         false,
		finishedMapTaskCount: 0,

		reduceTasks:             make(map[int]string),
		reduceTaskCount:         nReduce,
		reduceTaskDone:          false,
		allTaskDone:             false,
		finishedReduceTaskCount: 0,
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
