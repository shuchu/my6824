package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		time.Sleep(2 * time.Second)

		// request a job
		args := MrArgs{}
		reply := MrReply{}

		ok := call("Coordinator.Assign", &args, &reply)
		if ok {
			fmt.Printf("Got a job man! id: %d, type: %d, filePath %v\n",
				reply.JobId, reply.JobType, reply.FilePath)
		} else {
			fmt.Println("call failed!")
		}

		// map or reduce
		if reply.JobType == 0 && reply.FilePath != "" {
			var kva []KeyValue = mapWorker(reply.FilePath, mapf)
			// Now store the data to file
			for y := 0; y < reply.NumReducer; y++ {
				kva_y := []KeyValue{}
				oname := fmt.Sprintf("mr-%d-%d", reply.JobId, y)
				ofile, _ := os.Create(oname)
				for i := 0; i < len(kva); i++ {
					if ihash(kva[i].Key)%reply.NumReducer == y {
						kva_y = append(kva_y, kva[i])
					}
				}
				enc := json.NewEncoder(ofile)
				err := enc.Encode(&kva_y)
				if err != nil {
					log.Fatalf("cannot write %v", oname)
				}
				ofile.Close()
			}

			// call rpc to update the job status
			args.JobId = reply.JobId
			args.JobStatus = 2 // means Done!
			ok := call("Coordinator.Update", &args, &reply)
			if ok {
				fmt.Printf("Updated a job man! id: %d, type: %d, status: %d\n",
					reply.JobId, reply.JobType, reply.JobStatus)
				// if the update fail...
				if reply.JobStatus != args.JobStatus {
					fmt.Println("Failed to update job status!")
				}
			} else {
				fmt.Println("call failed!")
			}

		} else if reply.JobType == 1 {
			fmt.Printf("Its a reduce job")
		}
	}
}

func mapWorker(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return mapf(filename, string(content))
}

func reduceWorker(filename string, reducef func(string, string) []KeyValue) {

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
*/

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
