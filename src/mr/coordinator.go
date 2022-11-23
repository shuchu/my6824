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
	jobType      int8 // map (0), reducer (1), quit(2)
	jobStatus    int8 // idel (0), in-progress (1), done (2)
	lastUpdateTs int64
	filePath     string
}

type Coordinator struct {
	tasks       []Task
	nReducer    int
	mapJobsDone bool
	rdJobsDone  bool
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
	if c.mapJobsDone && c.rdJobsDone {
		reply.JobType = 2
		return nil
	}

	// Check all tasks, and return an idel one.
	for i := int32(0); i < int32(len(c.tasks)); i++ {
		t := &c.tasks[i]
		if t.jobStatus == 0 && t.jobType == 0 {
			// map jobs
			t.Lock()
			reply.FilePath = t.filePath
			reply.JobId = i
			reply.JobType = t.jobType
			reply.NumReducer = c.nReducer
			t.jobStatus = 1               // now in progress
			reply.JobStatus = t.jobStatus // this should after set t.jobStatus
			t.Unlock()
			break
		} else if t.jobStatus == 0 && t.jobType == 1 && c.mapJobsDone {
			// reduce jobs
			// now assign reduce jobs
			t.Lock()
			reply.FilePath = t.filePath
			reply.JobId = i
			reply.JobType = t.jobType
			reply.NumReducer = c.nReducer
			t.jobStatus = 1 // now in progress
			reply.JobStatus = t.jobStatus
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
			t.lastUpdateTs = time.Now().Unix()
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
	if c.mapJobsDone && c.rdJobsDone {
		ret = true
	}

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
	c.mapJobsDone = false
	c.rdJobsDone = false

	// Your code here
	// Create map tasks
	for idx, filename := range files {
		t := &c.tasks[idx]
		t.jobType = 0
		t.jobStatus = 0
		t.lastUpdateTs = time.Now().Unix()
		t.filePath = filename
	}

	// create reduce tasks
	for i := 0; i < nReduce; i++ {
		t := &c.tasks[i+len(files)]
		t.jobType = 1
		t.jobStatus = 0
		t.lastUpdateTs = time.Now().Unix()
		t.filePath = fmt.Sprintf("mr-*-%d", i)
	}

	c.server()

	// Evaluate tasks. Check the progress of Map tasks and Reduce Tasks.
	go evalTasks(&c)

	return &c
}

func evalTasks(c *Coordinator) {
	for {
		time.Sleep(10 * time.Second)
		mapTasksDone, reduceTasksDone := true, true

		currUnixTs := time.Now().Unix()
		for i := 0; i < len(c.tasks); i++ {
			t := &c.tasks[i]
			fmt.Printf("type: %v, status: %v, ts: %d, fpath: %v\n",
				t.jobType, t.jobStatus, t.lastUpdateTs, t.filePath)

			if t.jobType == 0 && t.jobStatus != 2 {
				mapTasksDone = false
			}

			if t.jobType == 1 && t.jobStatus != 2 {
				reduceTasksDone = false
			}

			// Check the status of each task, if the Last update TS
			// (lastUpdateTs) of task is 10sec ago, reset the jobStatus to 0,
			// which means New.

			if t.jobType == 1 && (currUnixTs-t.lastUpdateTs > 10) {
				t.Lock()
				t.jobStatus = 0
				t.Unlock()
				fmt.Println("Job %d status is reset to 0.", i)
			}

		}

		c.mapJobsDone = mapTasksDone
		c.rdJobsDone = reduceTasksDone
	}
}
