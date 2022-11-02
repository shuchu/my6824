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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Define the arguments and reply for the Map-Reducer
type MrArgs struct {
	JobId     int32
	JobStatus int8
	FilePath  string
}

type MrReply struct {
	JobId      int32
	JobType    int8 // map(0), reduce (1)
	JobStatus  int8
	NumReducer int
	FilePath   string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
