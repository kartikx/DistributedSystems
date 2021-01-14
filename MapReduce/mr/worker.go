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
	"time"
)

const (
	StubFileName     = "mr"
	ErrNoMapTasks    = "No Map Tasks left"
	ErrNoReduceTasks = "No Reduce Tasks left"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
// ? So as a worker traverses it's entire input file, it calls this method
// ? for each keyvalue pair. So you are NOT creating the entire output, and then
// ? splitting it up into various files.
// ? You don't need to worry about concurrent write issues, because two goRoutines
// ? can't simult. write to a file say mr-2-10. Only second worker writes to mr-2-*.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// ? Here the worker should ask the Master for some task,
	// ? Master should return a File and the worker number.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	mr := CallMapTask()

	// Valid response
	if mr.FileName != "" {
		fmt.Println("Allocating Map Task")
		MapWorker(mr, mapf)
		return
	}

	// fmt.Println("Map Tasks are over")
	// Reduce Workers
	rr := CallReduceTask()

	if rr.FileNum != "" {
		fmt.Println("Allocating Reduce Task")
		ReduceWorker(rr, reducef)
		return
	}

	fmt.Println("Received Invalid Reduce Response")
	return
}

func MapWorker(mr *MapResponse, mapf func(string, string) []KeyValue) {
	// Now you may start working on your file.
	workerFileName := StubFileName + "-" + mr.WorkerID // Of the form mr-0

	// The input files are in the main/ directory.
	file, err := os.Open("../main/" + mr.FileName)
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
	// Reponse contains the column number to scan across.
	// Scan all, store in slice and sort.
	// Then call reduce. Read up on how you should sort.

	intermedOut := []KeyValue{}

	// TODO a better way would be to use rr.NWorkers.
	for i := 0; ; i++ {
		fileName := fmt.Sprintf("%s-%d-%s.txt", "mr", i, rr.FileNum)
		// fmt.Println("Trying to open:", fileName)
		file, err := os.Open(fileName)

		if err != nil {
			// fmt.Println("Failed at: ", fileName)
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
			return mresp
		}
		time.Sleep(1 * time.Second)
	}
}

func CallReduceTask() *ReduceResponse {
	fmt.Println("Calling Reduce Task")

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

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//

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
	if err == nil || err.Error() == ErrNoMapTasks || err.Error() == ErrNoReduceTasks {
		return true
	}

	fmt.Println(err)
	return false
}
