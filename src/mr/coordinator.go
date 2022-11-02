package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	sync.RWMutex
	jobType      int8 // map (0), reducer (1)
	jobStatus    int8 // idel (0), in-progress (1), done (2)
	lastUpdateTs int64
	filePath     string
}

type Coordinator struct {
	tasks    []Task
	nReducer int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// Assign a job
func (c *Coordinator) Assign(args *MrArgs, reply *MrReply) error {
	// Check all tasks, and return an idel one.
	for i := int32(0); i < int32(len(c.tasks)); i++ {
		t := &c.tasks[i]
		if t.jobStatus == 0 {
			t.Lock()
			reply.FilePath = t.filePath
			reply.JobId = i
			reply.JobType = t.jobType
			reply.NumReducer = c.nReducer
			reply.JobStatus = t.jobStatus
			t.jobStatus = 1 // now in progress
			t.Unlock()
			break
		}
	}
	return nil
}

func (c *Coordinator) Update(args *MrArgs, reply *MrReply) error {
	// Update the state of One task.
	if 0 <= args.JobId && args.JobId < int32(len(c.tasks)) {
		t := &c.tasks[args.JobId]
		if args.JobStatus != t.jobStatus {
			t.Lock()
			t.jobStatus = args.JobStatus
			t.Unlock()

			// reply to the caller with latest job status
			reply.JobStatus = t.jobStatus
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReducer = nReduce
	if c.nReducer < 1 {
		c.nReducer = 1 // the minimum number of reducer
	}

	// create Tasks with length equals to possible total number of tasks
	c.tasks = make([]Task, len(files)+nReduce)

	// Your code here
	for idx, filename := range files {
		t := &c.tasks[idx]
		t.jobType = 0
		t.jobStatus = 0
		t.lastUpdateTs = time.Now().Unix()
		t.filePath = filename
	}

	c.server()

	//debug
	go showTasks(&c)

	return &c
}

func showTasks(c *Coordinator) {
	for {
		time.Sleep(3 * time.Second)
		for i := 0; i < len(c.tasks); i++ {
			t := &c.tasks[i]
			fmt.Printf("type: %v, status: %v, ts: %d, fpath: %v\n", t.jobType, t.jobStatus, t.lastUpdateTs,
				t.filePath)
		}
	}
}
