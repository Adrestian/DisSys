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

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

const (
	MapTaskDone    int = 0
	ReduceTaskDone int = 1
	TaskError      int = 2
)

const (
	MAP_TASK    string = "map"
	REDUCE_TASK string = "reduce"
)

type Args struct {
	TaskName string // "map" or "reduce"
	Filename string // for Map
	TaskNum  int
	Status   int
	Message  string
}

type Reply struct {
	TaskName     string
	Filename     string
	NReduce      int
	TaskNum      int
	ShouldStop   bool
	MapTaskCount int
}
