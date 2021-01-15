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

type MapRequest struct {
}

type MapResponse struct {
	FileName string
	WorkerID string
	NReduce  int
}

type ReduceRequest struct {
}

type ReduceResponse struct {
	FileNum  string
	NWorkers int
	WorkerID int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
