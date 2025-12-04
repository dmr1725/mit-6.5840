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
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		task := RequestTaskFromCoordinator()
		time.Sleep(time.Second)

		switch task.Type {
		case MapTask:
			log.Printf("Worker task: %v", task)
			file, err := os.Open(task.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", task.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.FileName)
			}
			file.Close()
			partitions := make([][]KeyValue, task.NReduce)
			kva := mapf(task.FileName, string(content))
			for _, keyValueObject := range kva {
				word := keyValueObject.Key
				partition := ihash(word) % task.NReduce
				partitions[partition] = append(partitions[partition], keyValueObject)
			}
			
			// writing each partition to a file
			for i:= 0; i < task.NReduce; i++ {
				finalFile := fmt.Sprintf("mr-%d-%d", task.ID, i)
				tempFile := fmt.Sprintf("mr-%d-%d-%d-temp", task.ID, i, os.Getpid())
				file, err := os.Create(tempFile)
				if err != nil {
					log.Fatalf("cannon create %v", tempFile)
				}
				enc := json.NewEncoder(file)
				
				for _, kv := range partitions[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode kv pair")
					}
				}
				file.Close()
				os.Rename(tempFile, finalFile)
			}

			MarkTaskDone(task)
		case ReduceTask:
			var kva []KeyValue
			for i:=0; i < task.NMap; i++ {
				// read all files of partition i
				filename := fmt.Sprintf("mr-%d-%d", i, task.ID)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Could not read %v", filename)
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break // EOF
					}
					kva = append(kva, kv)
				}
				file.Close()
			}

			sort.Sort(ByKey(kva))

			outFile, _ := os.Create(fmt.Sprintf("mr-out-%d", task.ID))

			// Group by key and apply reduce
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string 
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
				i = j
			}

			outFile.Close()
			MarkTaskDone(task)

		case Wait:
			time.Sleep(500 * time.Millisecond)
		case Exit:
			return
		}
	}

}

func RequestTaskFromCoordinator() TaskReply {
	// declare an argument structure.
	args := TaskArgs{}

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.AssignTask" tells the
	// receiving server that we'd like to call
	// the AssignTask() method of struct Coordinator.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
		return TaskReply{Type: Wait}
	}
}

func MarkTaskDone(task TaskReply) {

	// send the RPC request, wait for the reply.
	// the "Coordinator.MarkTaskDone" tells the
	// receiving server that we'd like to call
	// the MarkTaskDone() method of struct Coordinator.
	if task.Type == MapTask {
		// declare an argument structure.
		args := TaskArgs{
			Type: task.Type,
			Filename: task.FileName,
		}

		// declare a reply structure.
		reply := TaskReply{}
		
		ok := call("Coordinator.MarkTaskDone", &args, &reply)
		if ok {
			return
		} else {
			fmt.Printf("call failed!\n")
			return
		}
	} else if task.Type == ReduceTask {
		// declare an argument structure.
		args := TaskArgs{
			Type: task.Type,
			ID: task.ID,
		}

		// declare a reply structure.
		reply := TaskReply{}
		
		ok := call("Coordinator.MarkTaskDone", &args, &reply)
		if ok {
			return
		} else {
			fmt.Printf("call failed!\n")
			return
		}
	}
}


//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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