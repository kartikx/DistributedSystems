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
	"strconv"
	"sync"
	"time"
)

const (
	StubFileName          = "mr"
	ErrNoMapTasks         = "No Map Tasks left"
	ErrNoReduceTasks      = "No Reduce Tasks left"
	ErrMapNotFinished     = "Map Tasks are still left"
	ErrMapAlreadyFinished = "Map Tasks have already finished"
)

// Synchchronizes between the various Map GoRoutines invoked
// From a single worker.
var wg sync.WaitGroup

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Functions to allow Sorting Intermediate Output Slices.
type KeyValueArray []KeyValue

func (kva KeyValueArray) Len() int {
	return len(kva)
}

func (kva KeyValueArray) Swap(i, j int) {
	kva[i], kva[j] = kva[j], kva[i]
}

func (kva KeyValueArray) Less(i, j int) bool {
	return kva[i].Key < kva[j].Key
}

// ihash(key) % NReduce allows Mapping each intermediate
// Key-Value pair to a unique Reduce Worker.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// fmt.Println("Worker begin")

	for {
		mr := CallMapTask()

		// An Empty (but Successful) Response indicates
		// That all Map Tasks have been Allocated.
		// TODO Replace with a response.Ok == false
		if mr.FileName == "" {
			// fmt.Printf("M[%s]Breaking\n", mr.WorkerID)
			break
		}

		// fmt.Printf("M[%s]Not Breaking\n", mr.WorkerID)
		// fmt.Println("Allocating Map Task")

		// ? Whenever you use a GoRoutine, you need to ensure that enclosing
		// ? Function doesn't exit too soon. Waitgroups are your friend.
		// ? Dont call add in a GoRoutine. Leads to races.
		wg.Add(1)
		go func(mr *MapResponse, mapf func(string, string) []KeyValue) {
			MapWorker(mr, mapf)
		}(mr, mapf)
	}

	wg.Wait()

	for {
		// fmt.Println("Map Tasks are over")
		// Reduce Workers
		rr := CallReduceTask()

		// fmt.Println("Allocating Reduce Task", rr.FileNum)

		if rr.FileNum == "" {
			// fmt.Printf("R[%d]Breaking\n", rr.WorkerID)
			break
		}

		// fmt.Printf("R[%d]Not Breaking\n", rr.WorkerID)
		// fmt.Println("Allocating Reduce Task")
		wg.Add(1)
		go func(rr *ReduceResponse, reducef func(string, []string) string) {
			ReduceWorker(rr, reducef)
			CallJobComplete()
		}(rr, reducef)
	}
	wg.Wait()

	// fmt.Println("Received Invalid Reduce Response")
	return
}

func MapWorker(mr *MapResponse, mapf func(string, string) []KeyValue) {
	defer wg.Done()

	// Now you may start working on your file.
	workerFileName := StubFileName + "-" + mr.WorkerID // Of the form mr-0

	// The input files are in the main/ directory.
	// file, err := os.Open("../main/" + mr.FileName)
	file, err := os.Open(mr.FileName)
	if err != nil {
		log.Fatalln("[Map Worker unable to open file]", err)
	}

	intermedFiles := make([]*os.File, mr.NReduce)

	// Create intermediate files. 0 based indexing of the form mr-0-0
	for i := 0; i < mr.NReduce; i++ {
		intermedFiles[i], err = os.Create(workerFileName + "-" + strconv.Itoa(i) + ".txt")
		if err != nil {
			log.Fatalln("[Map Worker unable to create files]", err)
		}
	}

	// File opened successfully
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalln("[Map Worker unable to read file content]", err)
	}
	file.Close()

	// Call Map Function
	kva := mapf(mr.FileName, string(contents))

	// iterate on Kva  and for each key find the hash value
	// and write to intermediate file output.

	for _, kv := range kva {
		writeIndex := ihash(kv.Key) % mr.NReduce

		// In the intermediate file, you're supposed to write JSON.
		enc := json.NewEncoder(intermedFiles[writeIndex])
		err := enc.Encode(kv)
		if err != nil {
			log.Fatalln("[Unable to encode JSON]", err)
		}
	}

	// Close the intermediate files.
	for _, f := range intermedFiles {
		f.Close()
	}
}

func ReduceWorker(rr *ReduceResponse, reducef func(string, []string) string) {
	defer wg.Done()

	intermedOut := []KeyValue{}

	// TODO Not a very clean approach.
	// TODO a better way would be to use rr.NWorkers.
	for i := 0; ; i++ {
		fileName := fmt.Sprintf("%s-%d-%s.txt", "mr", i, rr.FileNum)
		// fmt.Println("Trying to open:", fileName)
		file, err := os.Open(fileName)

		if err != nil {
			fmt.Println("Failed at: ", fileName)
			break
		}

		dec := json.NewDecoder(file)
		kv := KeyValue{}
		for {
			if err = dec.Decode(&kv); err != nil {
				break
			}
			intermedOut = append(intermedOut, kv)
		}
	}

	// fmt.Println("Going to sort")
	sort.Sort(KeyValueArray(intermedOut))

	outFileName := fmt.Sprintf("mr-out-%s.txt", rr.FileNum)
	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatalln("[Unable to create Reduce Output file]", err)
	}

	for i, j := 0, 0; i < len(intermedOut); {
		keySlice := []string{}

		for j < len(intermedOut) && intermedOut[i].Key == intermedOut[j].Key {
			keySlice = append(keySlice, intermedOut[j].Key)
			j += 1
		}

		count := reducef(intermedOut[i].Key, keySlice)

		fmt.Fprintf(outFile, "%s %s\n", intermedOut[i].Key, count)

		i = j
	}

	return
}

// ? Asks the Master for a Map Task()
func CallMapTask() *MapResponse {
	var ok bool

	mreq := &MapRequest{}
	mresp := &MapResponse{}

	for {
		ok = call("Master.MapTask", mreq, mresp)

		if ok {
			// fmt.Println("Return Map Response")
			return mresp
		}
		time.Sleep(1 * time.Second)
		// fmt.Println("Try CallMapTask again")
	}
}

func CallReduceTask() *ReduceResponse {
	// fmt.Println("Calling Reduce Task")

	rreq := &ReduceRequest{}
	rresp := &ReduceResponse{}

	for {
		ok := call("Master.ReduceTask", rreq, rresp)

		if ok {
			return rresp
		}

		time.Sleep(1 * time.Second)
	}
}

// Called by each reduce when it's task is Finished.
func CallJobComplete() {
	var req, resp struct{}

	call("Master.JobComplete", &req, &resp)
}

// Send an RPC request to the master, wait for the response.
// Returns false if something goes wrong.

// TODO understand this (again)
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil || err.Error() == ErrMapAlreadyFinished || err.Error() == ErrNoReduceTasks {
		return true
	}

	fmt.Println(err)
	return false
}
