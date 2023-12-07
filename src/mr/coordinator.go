package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// X              int
	// Y              int
	InputFileNames []string
	nReduce        int
	MapDone        bool
	ReduceDone     bool
	MapJobs        SyncJobs
	ReduceJobs     SyncJobs
	sync.Mutex
}

type SyncJobs struct {
	Jobs  []int
	chans []chan int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkType(args *EmptyArgs, reply *WorkType) error {
	c.Lock()
	workFlag := 0
	if c.MapDone && !c.ReduceDone {
		workFlag = 1
	} else if c.MapDone && c.ReduceDone {
		workFlag = 2
	}
	c.Unlock()

	if workFlag == 0 {
		// reduces can't start until the last map has finished
		// give a Map work

		c.Lock()
		flag := true
		for i, stat := range c.MapJobs.Jobs {
			if stat == 0 {
				reply.X = i
				c.MapJobs.Jobs[i] = 1
				flag = false
				break
			}
		}
		c.Unlock()
		// fmt.Println(reply.X)
		if flag {
			reply.WorkType = "Wait"
		} else {
			reply.WorkType = "Map"
			reply.Filename = c.InputFileNames[reply.X]
			reply.Y = c.nReduce

			go c.Timer("Map", reply.X)

		}
	} else if workFlag == 1 {
		c.Lock()
		flag := true
		for i, stat := range c.ReduceJobs.Jobs {
			if stat == 0 {
				reply.Y = i
				c.ReduceJobs.Jobs[i] = 1
				flag = false
				break
			}
		}
		c.Unlock()

		if flag {
			reply.WorkType = "Wait"
		} else {
			reply.WorkType = "Reduce"
			reply.X = len(c.InputFileNames)

			go c.Timer("Reduce", reply.Y)
		}
	} else if workFlag == 2 {
		reply.WorkType = "Exit"
	} else {
		reply.WorkType = "Wait"
	}
	return nil
}

func (c *Coordinator) FinishWork(args *Finished, reply *EmptyReply) error {
	// fmt.Println(args.Index)
	if args.WorkType == "Map" {
		c.MapJobs.chans[args.Index] <- 1
	} else if args.WorkType == "Reduce" {
		c.ReduceJobs.chans[args.Index] <- 1
	}
	return nil
}

func (c *Coordinator) Timer(work string, index int) error {
	if work == "Map" {
		select {
		case <-c.MapJobs.chans[index]:
			// fmt.Println(index)
			c.Lock()
			c.MapJobs.Jobs[index] = 2
			for _, stat := range c.MapJobs.Jobs {
				if stat != 2 {
					c.Unlock()
					return nil
				}
			}
			c.MapDone = true
			c.Unlock()
		case <-time.After(10 * time.Second):
			// fmt.Println("Over")
			c.Lock()
			c.MapJobs.Jobs[index] = 0
			c.Unlock()
		}
	} else if work == "Reduce" {
		select {
		case <-c.ReduceJobs.chans[index]:
			// fmt.Println(index)
			c.Lock()
			c.ReduceJobs.Jobs[index] = 2
			for _, stat := range c.ReduceJobs.Jobs {
				if stat != 2 {
					c.Unlock()
					return nil
				}
			}
			c.ReduceDone = true
			c.Unlock()
		case <-time.After(10 * time.Second):
			c.Lock()
			c.ReduceJobs.Jobs[index] = 0
			c.Unlock()
		}
	}
	return nil
}

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
	c.Lock()
	if c.MapDone && c.ReduceDone {
		ret = true
	}
	c.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		// X:              0,
		// Y:              0,
		InputFileNames: files,
		nReduce:        nReduce,
		MapDone:        false,
		ReduceDone:     false,
	}

	c.MapJobs.Jobs = make([]int, len(files))
	for i := 0; i < len(files); i++ {
		nch := make(chan int)
		c.MapJobs.chans = append(c.MapJobs.chans, nch)
	}
	c.ReduceJobs.Jobs = make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		nch := make(chan int)
		c.ReduceJobs.chans = append(c.ReduceJobs.chans, nch)
	}

	// Your code here.

	c.server()
	return &c
}
