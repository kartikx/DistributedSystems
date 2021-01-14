package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type JobInfo struct {
	file     string
	start    time.Time
	finished bool
}

type MapTask struct {
	file      string
	status    bool
	workerID  int
	startTime time.Time
}

type RedTask struct {
	file      string
	status    bool
	workerID  int
	startTime time.Time
}

type Master struct {
	// Your definitions here.
	nReduce     int8
	mapTasks    []MapTask
	redTasks    []RedTask
	workerCount int8
	// jobStatus []JobInfo
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

func (m *Master) getMapTask(workerID int) (string, error) {
	for i := range m.mapTasks {
		task := &m.mapTasks[i]
		if !task.status {
			task.workerID = workerID
			task.status = true
			task.startTime = time.Now()
			return task.file, nil
		}
	}

	return "", errors.New("No Tasks left")
}

func (m *Master) MapTask(req *MapRequest, resp *MapResponse) error {
	// Worker ID is the 0 based index.
	workerID := int(m.workerCount)
	m.workerCount += 1

	// TODO Optimize how to find the next available task.
	nextTaskFile, err := m.getMapTask(workerID)
	if err != nil {
		return err
	}

	// ? I don't think a separate structure is needed.
	// jobInfo := JobInfo{file: nextTaskFile, start: time.Now(), finished: false}
	// m.jobStatus = append(m.jobStatus, jobInfo)

	fmt.Printf("Allocating Worker %d, File: %s\n", workerID, nextTaskFile)
	resp.FileName = nextTaskFile
	resp.NReduce = int(m.nReduce)
	resp.WorkerID = strconv.Itoa(workerID)

	return nil
}

func (m *Master) ReduceTask(req *ReduceRequest, resp *ReduceResponse) error {
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//

// TODO Let it work as is for now, eventually understand rpc.HandleHTTP() http.Serve() pattern.
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

	// ? What all is the master supposed to do?
	// * Initialize data structures. Make list of files to give.
	m.nReduce = int8(nReduce)

	for _, f := range files {
		fmt.Println(f)
		m.mapTasks = append(m.mapTasks, MapTask{file: f})
		m.redTasks = append(m.redTasks, RedTask{file: f})
	}

	m.server()
	return &m
}
