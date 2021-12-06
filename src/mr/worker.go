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

			// Write to a tmp file and then rename it
			intermediateFilenames := getMapIntermediateFilenames(taskNumber, nReduce)
			log.Println("finalFilenames: ", intermediateFilenames)

			tmpFiles, err := getTmpFiles(nReduce)
			if err != nil {
				notifyCoordinatorOnError(task.TaskName, filename, err.Error(), taskNumber)
				checkError(err, "Cannot get tmp files for map")
			}

			encoders := getEncoders(tmpFiles)

			err = encodeAll(kva, encoders)
			if err != nil {
				notifyCoordinatorOnError(task.TaskName, filename, err.Error(), taskNumber)
				log.Fatalf("Problem encode file: %v\n", filename)
			}
			// Finished writing to file, close all files
			closeAllFiles(tmpFiles)

			// Atomically rename all files, don't care about errors
			renameFiles(intermediateFilenames, tmpFiles)

			// Finally Notify coordinator that map task is done
			args := Args{
				TaskName: MAP_TASK,
				TaskNum:  taskNumber,
				Filename: filename,
				Status:   MapTaskDone,
				Message:  "Map Task Done",
			}
			reply := notifyCoordinatorOnSuccess(args)
			if reply.ShouldStop {
				shouldStop = true
				os.Exit(0)
			}
		} else if task.TaskName == REDUCE_TASK { // Should do "reduce"
			taskNumber := task.TaskNum
			totalMapTaskCount := task.MapTaskCount

			inputFiles := getInputFiles(taskNumber, totalMapTaskCount)
			defer closeAllFiles(inputFiles)

			outFilename := getOutputFilename(taskNumber) // final output file name
			tmpFile, err := ioutil.TempFile("", "tmpReduce*")
			if err != nil { // worker encountered error when creating tmp file
				notifyCoordinatorOnError(task.TaskName, outFilename, err.Error(), taskNumber)
				checkError(err, "Cannot create tmp file")
			}

			decoders := getDecoders(inputFiles)
			mem := []KeyValue{} // in memory buffer to reduce
			// Read everything from input files
			for _, decoder := range decoders {
				for {
					var kv KeyValue
					// decode them into memory, if there is no more data, break
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					mem = append(mem, kv)
				}
			}
			sort.Sort(ByKey(mem)) // sort by key

			// apply user's reduce function, stolen from mrsequential.go
			i := 0
			for i < len(mem) {
				j := i + 1
				for j < len(mem) && mem[j].Key == mem[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, mem[k].Value)
				}
				output := reducef(mem[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpFile, "%v %v\n", mem[i].Key, output)
				i = j
			}

			tmpFile.Close() // write it out to tmpFile

			os.Rename(tmpFile.Name(), outFilename) // atomically rename tmpFile to outFilename
			// Notify the coordinator
			args := Args{
				TaskName: REDUCE_TASK,
				TaskNum:  taskNumber,
				Filename: outFilename,
				Status:   ReduceTaskDone,
				Message:  "Reduce Task Done",
			}
			// Can share reply and handling stopping for both if branches, but no time to refactor
			reply := notifyCoordinatorOnSuccess(args)
			if reply.ShouldStop {
				shouldStop = true
				os.Exit(0)
			}
		} else {
			log.Fatalf("Unknown job name: %v\n", task.TaskName)
		}
		time.Sleep(time.Second)
	}

}

func getOutputFilename(taskNumber int) string {
	return fmt.Sprintf(OUTPUT_FILE_FORMAT, taskNumber)
}

func getInputFiles(taskNumber int, mapTaskCount int) []*os.File {
	inputFiles := make([]*os.File, nReduce)
	for i := 0; i < mapTaskCount; i++ {
		filename := fmt.Sprintf(FILENAME_FORMAT, i, taskNumber)
		file, err := os.Open(filename)
		if err != nil {
			notifyCoordinatorOnError(REDUCE_TASK, filename, err.Error(), taskNumber)
			checkError(err, "Cannot open all the files required for reduce")
		}
		inputFiles[i] = file
	}
	return inputFiles
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
		err := encoders[N].Encode(kv)
		if err != nil {
			return err
		}
	}
	return nil
}

// return true if the worker should exit
func checkTask(task *Reply) bool {
	if task.ShouldStop {
		shouldStop = true
		return true
	}
	// Something is up if nReduce changes
	if nReduce != 0 && task.NReduce != nReduce { // nReduce is a global variable
		log.Printf("nReduce changed from %v to %v \n", nReduce, task.NReduce)
	}
	nReduce = task.NReduce

	return false
}

func getMapIntermediateFilenames(taskNumber int, nReduce int) []string {
	finalFilenames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		finalFilenames[i] = fmt.Sprintf(FILENAME_FORMAT, taskNumber, i)
	}
	return finalFilenames
}

func getReduceIntermediateFilenames(taskNumber int, mapTaskCount int) []string {
	filenames := make([]string, nReduce)
	for i := 0; i < mapTaskCount; i++ {
		filename := fmt.Sprintf(FILENAME_FORMAT, i, taskNumber)
		filenames[i] = filename
	}

	return filenames
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

func getEncoders(files []*os.File) []*json.Encoder {
	nFiles := len(files)
	encoders := make([]*json.Encoder, nFiles)

	for i := 0; i < nFiles; i++ {
		encoders[i] = json.NewEncoder(files[i])
	}
	return encoders
}

func getDecoders(files []*os.File) []*json.Decoder {
	nFiles := len(files)
	decoders := make([]*json.Decoder, nFiles)

	for i := 0; i < nFiles; i++ {
		decoders[i] = json.NewDecoder(files[i])
	}
	return decoders
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
