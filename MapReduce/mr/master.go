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
	"sync"
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

type ReduceTask struct {
	file      string // This is the File Number. 0 .. 9
	status    bool
	workerID  int
	startTime time.Time
}

type Master struct {
	nReduce     int8
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	workerCount int8
	mut         sync.Mutex
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

// This function modifies shared Master data.
// Hence, use Mutex for safety.
func (m *Master) getNextMapTask(workerID int) (string, error) {
	m.mut.Lock()
	for i := range m.mapTasks {
		task := &m.mapTasks[i]
		if !task.status {
			task.workerID = workerID
			task.status = true
			task.startTime = time.Now()
			m.workerCount += 1

			m.mut.Unlock()
			return task.file, nil
		}
	}

	// To allow Reduce Workers count to start from 0.
	m.workerCount = 0
	m.mut.Unlock()
	return "", errors.New(ErrNoMapTasks)
}

func (m *Master) MapTask(req *MapRequest, resp *MapResponse) error {
	// Worker ID is the 0 based index.
	workerID := int(m.workerCount)

	// TODO Optimize how to find the next available task.
	nextTaskFile, err := m.getNextMapTask(workerID)
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

func (m *Master) getNextReduceTask(workerID int) (string, error) {
	m.mut.Lock()
	for i := range m.reduceTasks {
		task := &m.reduceTasks[i]
		if !task.status {
			task.workerID = workerID
			task.status = true
			task.startTime = time.Now()
			m.workerCount += 1

			m.mut.Unlock()
			return task.file, nil
		}
	}

	m.mut.Unlock()

	// TODO can use this as an indication that all tasks have finished.
	return "", errors.New(ErrNoReduceTasks)
}

func (m *Master) ReduceTask(req *ReduceRequest, resp *ReduceResponse) error {
	workerID := int(m.workerCount)

	nextTaskFile, err := m.getNextReduceTask(workerID)
	if err != nil {
		return err
	}

	resp.FileNum = nextTaskFile
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

// TODO understand where to call the master goroutine that repeat
// TODO checks whether workers are performing their job.

// ? Each of the Files corresponds to one split, and serves as an input
// ? to a Map Worker task.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = int8(nReduce)

	for _, f := range files {
		m.mapTasks = append(m.mapTasks, MapTask{file: f})
	}
	fmt.Println("Map Tasks initialized")

	for i := 0; i < nReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, ReduceTask{file: strconv.Itoa(i)})
	}
	fmt.Println("Reduce Tasks initialized")

	m.server()
	return &m
}
