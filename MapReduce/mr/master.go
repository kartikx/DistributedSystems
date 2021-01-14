package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) MapTask(req *MapRequest, resp *MapResponse) error {

}

func (m *Master) ReduceTask(req *ReduceRequest, resp *ReduceResponse) error {

}

//
// start a thread that listens for RPCs from worker.go
//

// ? Let it work as is for now, eventually understand rpc.HandleHTTP() http.Serve() pattern.
func (m *Master) server() {
	rpc.Register(m)

	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")

	sockname := masterSock()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

// ? Each of the Files corresponds to one split, and serves as an input
// ? to a Map Worker task.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	// ? What all is the master supposed to do?
	// * Initialize data structures. Make list of files to give.

	m.server()
	return &m
}
