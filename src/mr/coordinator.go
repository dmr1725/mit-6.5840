package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

/*
Type definitions. Coordinator will store a map of files and their status.
*/
type Status int 

const (
	// this assigns unAssigned=0, InProgress=1, Done=2
	unAssigned Status = iota
	InProgress 
	Done
)

type Phase int 
const (
	mapPhase Phase = iota
	reducePhase
)

type TaskInfo struct {
	ID int 
	Status Status
	startTime time.Time
}


type Coordinator struct {
	// Your definitions here.
	mapTasks map[string]TaskInfo
	reduceTasks map[string]TaskInfo
	nReduce int
	mu sync.Mutex
	phase Phase
}

// Your code here -- RPC handlers for the worker to call.
// yo escribí ese nombre AssignTask (no es parte del lab)
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error{
	// Multiple works can call AssignTask concurrently
	// without locking, two workers could be assigned the same task.
	c.mu.Lock()
	defer c.mu.Unlock()

	// send Exit signal
	if c.Done() {
		reply.Type = Exit
		return nil
	}

	if c.phase == mapPhase {
		for file, task_info := range c.mapTasks {
			if task_info.Status == unAssigned {
				task_info.Status = InProgress
				task_info.startTime = time.Now()
				reply.FileName = file
				reply.NReduce = c.nReduce
				reply.Type = MapTask
				reply.ID = task_info.ID

				// actually change the map
				c.mapTasks[file] = task_info

				log.Printf("[MAP] AssignTask: reply=%+v, mapTask=%+v", reply, c.mapTasks[file])
				return nil
			}
		}

		reply.Type = Wait
		return nil
	} else if c.phase == reducePhase {
		for id, task_info := range c.reduceTasks {
			if task_info.Status == unAssigned {
				task_info.Status = InProgress
				task_info.startTime = time.Now()

				reply.Type = ReduceTask
				reply.ID = task_info.ID
				reply.NMap = len(c.mapTasks)

				c.reduceTasks[id] = task_info
				log.Printf("[REDUCE] AssignTask: reply=%+v, reduceTask=%+v", reply, c.reduceTasks[id])
				return nil
			}
		}
		reply.Type = Wait
		return nil
	}

	return nil
}

func (c *Coordinator) MarkTaskDone(args *TaskArgs, reply *TaskReply) error {
	// Multiple works can call MarkMapTaskDone concurrently
	// without locking, two workers could be assigned the same task.
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == MapTask {
		for file, task_info := range c.mapTasks {
			if file == args.Filename && task_info.Status != Done {
				task_info.Status = Done

				// actually change the map
				c.mapTasks[file] = task_info
				log.Printf("[MAP] task done: file=%v task_info=%+v", file, task_info)
				return nil
			}
		}
	} else if args.Type == ReduceTask {
		for id, task_info := range c.reduceTasks {
			if id == strconv.Itoa(args.ID) && task_info.Status != Done {
				task_info.Status = Done 

				c.reduceTasks[id] = task_info
				log.Printf("[REDUCE] task done: id=%v task_info=%+v", id, task_info)
				return nil
			}
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	if c.phase != reducePhase {
		return false
	}

	if c.phase == reducePhase {
		for _, task_info := range c.reduceTasks {
			if task_info.Status != Done {
				return false
			}
		}
	} 

	return true
}

// This function should be called from a function that is using c.mu and locking
func (c *Coordinator) CheckAllMapTasksDone() bool {
	for _, task_info := range c.mapTasks {
		if task_info.Status == InProgress || task_info.Status == unAssigned {
			return false
		}
	}

	return true
}

func (c *Coordinator) ManagePhaseTranstion(nReduce int) {
	for {
		c.mu.Lock()
		if c.phase == mapPhase && c.CheckAllMapTasksDone() {
			log.Printf("Changing to reduce phase")
			c.phase = reducePhase

			// initialize reduceTasks
			for i := 0; i < nReduce; i++ {
				c.reduceTasks[strconv.Itoa(i)] = TaskInfo{
					ID: i,
					Status: unAssigned,
				}
			}

			log.Printf("Set reduce tasks successfully: %v", c.reduceTasks)
			c.mu.Unlock()
			break
		}
		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) CheckIdleTasks() {
	for {
		if c.phase == mapPhase {
			c.mu.Lock()
			for file, task_info := range c.mapTasks {
				// stale task that has taken more than 10 seconds
				if task_info.Status == InProgress && time.Since(task_info.startTime) > 10*time.Second {
					log.Printf("[MAP] Mark task to unassign due to being 10 seconds idle: %s, %v", file, task_info)
					task_info.Status = unAssigned

					c.mapTasks[file] = task_info
				}
			}
			c.mu.Unlock()
			time.Sleep(time.Second)
		} else if c.phase == reducePhase {
			c.mu.Lock()
			for id, task_info := range c.reduceTasks {
				// stale task that has taken more than 10 seconds
				if task_info.Status == InProgress && time.Since(task_info.startTime) > 10*time.Second {
					log.Printf("[REDUCE] Mark task to unassign due to being 10 seconds idle: %s, %v", id, task_info)
					task_info.Status = unAssigned

					c.reduceTasks[id] = task_info
				}
			}
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks: make(map[string]TaskInfo),
		reduceTasks: make(map[string]TaskInfo),
		nReduce: nReduce,
		phase: mapPhase,
	}

	// Your code here.
	log.Printf("Setting map tasks..")
	for idx, file := range files {
		c.mapTasks[file] = TaskInfo{
			ID: idx, 
			Status: unAssigned,
		}
	}
	log.Printf("Set map tasks successfully: %v", c.mapTasks)


	// these threads will run in the background/asynchronously
	go c.ManagePhaseTranstion(nReduce)
	go c.CheckIdleTasks()


	c.server()
	return &c
}