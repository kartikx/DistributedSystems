package mr

import (
	"errors"
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
	nReduce         uint8
	mapTasks        []MapTask    // List of Map Tasks
	reduceTasks     []ReduceTask // List of Reduce Tasks
	workerCount     uint8        //ID
	mut             sync.Mutex
	doneMut         sync.Mutex
	mapFinished     bool
	reduceFinished  bool
	completedJobs   uint8
	allJobsComplete bool
	// jobStatus []JobInfo
}

func (m *Master) getNextMapTask(workerID int) (string, error) {
	// fmt.Prinln("GetNextMapTask")
	if m.mapFinished {
		// fmt.Println("Map Tasks Finished Already")
		return "", errors.New(ErrMapAlreadyFinished)
	}

	// TODO currently, only one goroutine can be in this loop.
	// TODO maybe i can optimize that.
	// TODO A solution could be to have mutex only on the lines
	// TODO that modify shared data.
	for i := range m.mapTasks {
		task := &m.mapTasks[i]
		if !task.status {
			task.workerID = workerID
			task.status = true
			task.startTime = time.Now()
			m.workerCount += 1
			// fmt.Println(m.workerCount)
			return task.file, nil
		}
	}

	// fmt.Println("Setting m.MapFinished")
	m.workerCount = 0
	m.mapFinished = true

	return "", errors.New(ErrMapAlreadyFinished)
}

func (m *Master) MapTask(req *MapRequest, resp *MapResponse) error {
	m.mut.Lock()

	// Worker ID is the 0 based index.
	workerID := int(m.workerCount)

	// TODO Optimize how to find the next available task.
	// TODO Could use a different Data Structure.
	nextTaskFile, err := m.getNextMapTask(workerID)
	m.mut.Unlock()

	if err != nil {
		return err
	}

	resp.FileName = nextTaskFile
	resp.NReduce = int(m.nReduce)
	resp.WorkerID = strconv.Itoa(workerID)

	return nil
}

func (m *Master) getNextReduceTask(workerID int) (string, error) {
	// fmt.Println("GetNextReduceTask")
	if !m.mapFinished {
		return "", errors.New(ErrMapNotFinished)
	}

	m.doneMut.Lock()
	isFinished := m.reduceFinished
	m.doneMut.Unlock()
	if isFinished {
		// fmt.Println("Reduce has already finished")
		return "", errors.New(ErrNoReduceTasks)
	}

	for i := range m.reduceTasks {
		task := &m.reduceTasks[i]
		if !task.status {
			task.workerID = workerID
			task.status = true
			task.startTime = time.Now()
			m.workerCount += 1

			return task.file, nil
		}
	}

	m.doneMut.Lock()

	// Indicates that all Reduce Jobs have been allocated to a worker.
	m.reduceFinished = true
	m.doneMut.Unlock()

	return "", errors.New(ErrNoReduceTasks)
}

func (m *Master) ReduceTask(req *ReduceRequest, resp *ReduceResponse) error {
	m.mut.Lock()

	workerID := int(m.workerCount)

	nextTaskFile, err := m.getNextReduceTask(workerID)
	m.mut.Unlock()

	if err != nil {
		return err
	}

	resp.FileNum = nextTaskFile
	resp.WorkerID = workerID
	return nil
}

func (m *Master) JobComplete(req, resp *struct{}) error {
	m.doneMut.Lock()
	// fmt.Println("One Reduce Job finished")

	m.completedJobs += 1

	// ? Currently both cases return a nil error
	// ? because I don't use the return value.
	if m.completedJobs == m.nReduce {
		m.allJobsComplete = true
	}

	m.doneMut.Unlock()
	return nil
}

// TODO Understand how Http.Serve() functions in this context.
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

// Done is called repeatedly by Mrmaster, to find out
// Whether all Tasks have been completed.
// This should return true, only when All Reduces have finished.
// NOT when all Reduces have been allocated.
func (m *Master) Done() bool {
	m.doneMut.Lock()
	ret := m.allJobsComplete
	m.doneMut.Unlock()
	return ret
}

// Master initialization function
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = uint8(nReduce)

	for _, f := range files {
		m.mapTasks = append(m.mapTasks, MapTask{file: f})
	}
	// fmt.Println("Map Tasks initialized", len(m.mapTasks))

	for i := 0; i < nReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, ReduceTask{file: strconv.Itoa(i)})
	}
	// fmt.Println("Reduce Tasks initialized")

	m.server()
	return &m
}
