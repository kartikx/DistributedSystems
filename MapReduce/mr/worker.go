package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

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

	CallMapTask()

	CallReduceTask()
}

// ? Asks the Master for a Map Task()
func CallMapTask() {
	var ok bool

	mreq := &MapRequest{}
	mresp := &MapResponse{}

	for {
		ok = call("Master.MapTask", mreq, mresp)

		if !ok {
			time.Sleep(1 * time.Second)
		}
	}
}

func CallReduceTask() {
	var err error
	for {
		//

		if err != nil {
			time.Sleep(1 * time.Second)
		}
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
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
