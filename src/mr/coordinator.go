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
	id           int32
	jobType      int8 // map (0), reducer (1)
	jobStatus    int8 // idel (0), in-progress (1), done (2)
	lastUpdateTs int64
	filePath     string
}

type Coordinator struct {
	tasks []Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// create Tasks with length equals to possible total number of tasks
	c.tasks = make([]Task, len(files)+nReduce)

	// Your code here
	var _task_id int32 = 1
	for _, filename := range files {
		t := Task{}
		t.id = _task_id
		t.jobType = 0
		t.jobStatus = 0
		t.lastUpdateTs = time.Now().Unix()
		t.filePath = filename
		c.tasks = append(c.tasks, t)
		_task_id += 1
	}

	c.server()

	//debug
	go showTasks(&c)

	return &c
}

func showTasks(c *Coordinator) {
	for {
		time.Sleep(time.Second)
		for _, t := range c.tasks {
			fmt.Println(t.id, t.jobType, t.jobStatus, t.lastUpdateTs,
				t.filePath)
		}
	}
}
