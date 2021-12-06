package mr

import (
	"encoding/json"
	"errors"
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
	FILENAME_FORMAT     string = "mr-%d-%d"
	TMP_FILENAME_FORMAT string = "mr-%d-%d-tmp"
	OUTPUT_FILE_FORMAT  string = "mr-out-%d"
)

// for sorting by key. Stolen from mrsequential.go
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
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
		task, ok := getTask() // task: Reply, the reply from the coordinator
		if !ok {
			os.Exit(0)
		}

		shouldExit := checkTask(&task)
		if shouldExit { // Coordinator told worker to exit
			os.Exit(0)
		}

		if task.TaskName == MAP_TASK { // Should do "map"
			taskNumber := task.TaskNum
			filename := task.Filename
			file, err := os.Open(filename)

			if err != nil { // worker encountered error
				notifyCoordinatorOnError(task.TaskName, filename, err.Error(), taskNumber)
				checkError(err, "cannot open file")
			}
			content, err := ioutil.ReadAll(file)
			if err != nil { // worker encountered error
				notifyCoordinatorOnError(task.TaskName, filename, err.Error(), taskNumber)
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))

			// Should we write to a tmp file and then rename it?
			finalFilenames := getFinalFilenames(taskNumber, nReduce)
			tmpFiles, err := getTmpFiles(nReduce)
			if err != nil {
				notifyCoordinatorOnError(task.TaskName, filename, err.Error(), taskNumber)
				checkError(err, "Cannot get tmp files for map")
			}

			encoders := getOutputEncoders(tmpFiles)

			err = encodeAll(kva, encoders)
			if err != nil {
				notifyCoordinatorOnError(task.TaskName, filename, err.Error(), taskNumber)
				log.Fatalf("Problem encode file: %v\n", filename)
			}
			// Finished writing to file, close all files
			closeAllFiles(tmpFiles)

			// Atomically rename all files, don't care about errors
			renameFiles(finalFilenames, tmpFiles)

			// Finally Notify coordinator that map task is done
			args := Args{
				TaskName: "map",
				TaskNum:  taskNumber,
				Filename: filename,
				Status:   MapTaskDone,
				Message:  "Map Task Done",
			}
			reply := notifyCoordinatorOnSuccess(args)
			if reply.ShouldStop {
				shouldStop = true
			}

		} else if task.TaskName == REDUCE_TASK { // Should do "reduce"
			// TODO:

		} else {
			log.Fatalf("Unknown job name: %v\n", task.TaskName)
		}
		time.Sleep(time.Second)
	}

}

func closeAllFiles(files []*os.File) {
	for _, file := range files {
		file.Close()
	}
}

func encodeAll(kva []KeyValue, encoders []*json.Encoder) error {
	if len(encoders) == 0 {
		return errors.New("No encoders")
	}
	for _, kv := range kva {
		keyHash := ihash(kv.Key)
		N := keyHash % nReduce
		err := encoders[N].Encode(&kv)
		if err != nil {
			return err
		}
	}
	return nil
}

// return true if the worker should exit
func checkTask(task *Reply) bool {
	// Something is up if nReduce changes
	if nReduce != 0 && task.NReduce != nReduce { // nReduce is a global variable
		log.Printf("nReduce changed from %v to %v \n", nReduce, task.NReduce)
	}
	nReduce = task.NReduce

	if task.ShouldStop {
		return true
	}
	return false
}

func getFinalFilenames(taskNumber int, nReduce int) []string {
	finalFilenames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		finalFilenames[i] = fmt.Sprintf(FILENAME_FORMAT, taskNumber, i)
	}
	return finalFilenames
}

func getTmpFiles(nReduce int) ([]*os.File, error) {
	tmpFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		tmpFile, err := ioutil.TempFile("", "tmp****")
		if err != nil {
			return tmpFiles, err
		}
		tmpFiles[i] = tmpFile
	}
	return tmpFiles, nil
}

// I know, not ideal, but it works
func getOutputEncoders(files []*os.File) []*json.Encoder {
	nFiles := len(files)
	encoders := make([]*json.Encoder, nFiles)

	for i := 0; i < nFiles; i++ {
		encoders[i] = json.NewEncoder(files[i])
	}
	return encoders
}

// Get one task from the coordinator
func getTask() (Reply, bool) {
	args := Args{}
	reply := Reply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		shouldStop = true // set the global variable
		return reply, ok
	}
	return reply, ok
}

// rename all the tmp files to the final files with the correct name in {@code finalFilenames}
func renameFiles(finalFilenames []string, tmpFiles []*os.File) bool {
	if len(finalFilenames) < len(tmpFiles) {
		log.Println("[ERROR]: Cannot rename files, finalFilenames and tmpFiles are not the same length")
		return false
	}

	var encounteredError bool = false
	for i, tmpFile := range tmpFiles {
		tmpFilename := tmpFile.Name()
		err := os.Rename(tmpFilename, finalFilenames[i])
		if err != nil {
			log.Printf("[ERROR]: Cannot rename files, %v\n", err.Error())
			encounteredError = true
		}
	}
	return !encounteredError
}

func checkError(err error, msg string) {
	if err != nil {
		log.Fatalf("[ERROR] %v: %v\n", msg, err.Error())
	}
	shouldStop = true
	return
}

func notifyCoordinatorOnSuccess(args Args) Reply {
	reply := Reply{}
	call("Coordinator.TaskDone", &args, &reply)
	return reply
}

//TODO
func notifyCoordinatorOnError(taskName, filename, errMessage string, taskNum int) {
	reply := Reply{}
	args := Args{
		TaskName: taskName,
		Filename: filename,
		Status:   TaskError,
		Message:  errMessage,
		TaskNum:  taskNum,
	}
	call("Coordinator.TaskError", &args, &reply)
}

// call(...) return false if the rpc returns an error
//           return true if the rpc succeeded
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
