package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.
type TaskType int 
const (
	MapTask TaskType = iota
	ReduceTask
	Wait
	Exit
)

type TaskArgs struct {
	Type TaskType
	Filename string // for map tasks
	ID int // for reduce tasks
}

type TaskReply struct {
	Type TaskType
	FileName string // id for map task
	NReduce int // number of reduce tasks
	ID int // id for reduce task
	NMap int // number of map tasks
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}