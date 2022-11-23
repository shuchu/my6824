package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			break // we let it die if the server does not reply.
		}

		// update the args values
		args.JobStatus = reply.JobStatus
		args.JobId = reply.JobId

		// send heart beat
		go sendHeartBeat(&args, &reply)

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
			args.JobStatus = 2 // means Done!
			ok := call("Coordinator.Update", &args, &reply)
			if ok {
				fmt.Printf("Updated the status of job  id: %d, type: %d, status: %d\n",
					reply.JobId, reply.JobType, reply.JobStatus)
				// if the update fail...
				if reply.JobStatus != args.JobStatus {
					fmt.Println("Failed to update job status!")
				}
			} else {
				fmt.Println("call failed!")
			}

		} else if reply.JobType == 1 {
			//Its a reduce job
			//Find all files that have map result
			matches, err := filepath.Glob(reply.FilePath)
			if err != nil {
				fmt.Println(err)
			}

			// load all map results to intermediate buffer
			intermediate := []KeyValue{}
			for _, filename := range matches {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv...)
				}
			}

			sort.Sort(ByKey(intermediate))

			oname := reply.FilePath
			oname = strings.Replace(oname, "*", "out", 1)
			ofile, _ := os.Create(oname)

			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
			// call rpc to update the job status
			args.JobStatus = 2 // means Done!
			ok := call("Coordinator.Update", &args, &reply)
			if ok {
				fmt.Printf("Updated the status of job  id: %d, type: %d, status: %d\n",
					reply.JobId, reply.JobType, reply.JobStatus)
				// if the update fail...
				if reply.JobStatus != args.JobStatus {
					fmt.Println("Failed to update job status!")
				}
			} else {
				fmt.Println("call failed!")
			}

		} else if reply.JobType == 2 {
			fmt.Printf("I was asked to quit the job since no more tasks.")
			break
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

//func reduceWorker(filename string, reducef func(string, string) []KeyValue) {
//}

func sendHeartBeat(args *MrArgs, reply *MrReply) {
	if reply.JobStatus == 1 {
		time.Sleep(5 * time.Second)
		ok := call("Coordinator.Update", args, reply)

		if ok {
			fmt.Println("sent heart beat to server for job: %d", args.JobId)
		} else {
			fmt.Println("failed sent heart beat to server for job %d", args.JobId)
		}
	}
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
