package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType int
type State int
type TaskState int

const (
	MAP TaskType = iota
	REDUCE
	KEEPWAITING
	NONE
)
const (
	MAPPING State = iota
	REDUCING
	FINISHED
)
const (
	WAITING TaskState = iota
	DOING
	DONE
)

type Task struct {
	Id        int       //任务编号
	Filename  string    //任务名，reduce任务没有这个
	State     TaskState //是否完成
	TaskType  TaskType  //任务类型 map or reduce
	StartTime time.Time //开始时间
}

//计数器-用于产生全局唯一任务编号
type Counter struct {
	lock  sync.Mutex
	value int
}

func (c *Counter) addValue() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value++
}

func (c *Counter) getValue() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.value
}

func (c *Counter) resetValue() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value = 1
}

var count = Counter{value: 1}

type Coordinator struct {
	// Your definitions here.
	mMap        int        //map任务数量
	nReduce     int        //reduce任务数量
	mDone       int        //map已完成数量
	nDone       int        //reduce已完成数量
	mapTasks    []*Task    //map任务列表
	reduceTasks []*Task    //reduce任务列表
	lock        sync.Mutex //锁
	state       State      //当前状态 map或reduce阶段
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	//1.加锁
	c.lock.Lock()
	defer c.lock.Unlock()
	//2.判断当前State MAPPING/REDUCING

	var task *Task
	if c.state == MAPPING {
		//3.选出未被分配的任务 设置任务开始时间
		for _, mapTask := range c.mapTasks {
			if mapTask.State == WAITING {
				mapTask.StartTime = time.Now()
				mapTask.State = DOING
				task = mapTask
				//fmt.Println("coordinator has assigned a map task!")
				break
			}
		}
		//3.1没有任务了，返回nil,worker接到nil wait
		if task != nil {
			reply.TaskID = task.Id
			reply.TaskType = task.TaskType
			reply.Filename = task.Filename
		} else {
			reply.TaskType = KEEPWAITING
		}

	} else if c.state == REDUCING {
		//3.选出未被分配的任务 设置任务开始时间
		for _, reduceTask := range c.reduceTasks {
			if reduceTask.State == WAITING {
				reduceTask.StartTime = time.Now()
				reduceTask.State = DOING
				task = reduceTask
				//fmt.Println("coordinator has assigned a reduce task!")
				break
			}
		}
		//3.1没有任务了，返回nil,worker接到nil后退出
		if task != nil {
			reply.TaskID = task.Id
			reply.TaskType = task.TaskType
		} else {
			reply.TaskType = KEEPWAITING
		}
	} else if c.state == FINISHED {
		reply.TaskType = NONE
	}
	reply.MMap = c.mMap
	reply.NReduce = c.nReduce
	//no use
	if task == nil {
		//fmt.Println("There are no tasks to be assigned!")
	}
	//fmt.Printf("send task:%p\n", task)
	//4.返回给worker
	return nil
	//5.解锁
}
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	//1.加锁
	c.lock.Lock()
	defer c.lock.Unlock()
	//2.设置任务已完成 设置mDone / nDone

	taskID := args.TaskID
	taskType := args.TaskType
	//fmt.Printf("receive &task:%p\n", task)
	//fmt.Println("receive task:", task)
	//3.检查一下是否全部mDone已完成，是-改变State
	if taskType == MAP && c.state == MAPPING {
		fmt.Printf("mapTask(taskid:%d) has done\n", taskID)
		c.mapTasks[taskID].State = DONE
		c.mDone++
		if c.mDone == c.mMap {
			c.state = REDUCING
			fmt.Println("------------Reduce------------")
		}
	} else if taskType == REDUCE && c.state == REDUCING {
		fmt.Printf("reduceTask(taskid:%d) has done\n", taskID)
		c.reduceTasks[taskID].State = DONE
		c.nDone++
		if c.nDone == c.nReduce {
			c.state = FINISHED
			fmt.Println("------------Finished------------")
		}
	}
	return nil
	//4.返回给worker
	//5.解锁
}

// check if any tasks are overdue
func (c *Coordinator) checkTasks() {
	for {
		c.lock.Lock()
		currentTime := time.Now()
		tenSecondsAgo := currentTime.Add(-10 * time.Second)
		if c.state == FINISHED {
			c.lock.Unlock()
			break
		} else if c.state == MAPPING {
			for _, mapTask := range c.mapTasks {
				if mapTask.State == DOING && mapTask.StartTime.Before(tenSecondsAgo) {
					fmt.Printf("mapTask(taskid:%d) has restarted\n", mapTask.Id)
					mapTask.State = WAITING
				}
			}
		} else if c.state == REDUCING {
			for _, reduceTask := range c.reduceTasks {
				if reduceTask.State == DOING && reduceTask.StartTime.Before(tenSecondsAgo) {
					fmt.Printf("reduceTask(taskid:%d) has restarted\n", reduceTask.Id)
					reduceTask.State = WAITING
				}
			}
		}
		c.lock.Unlock()
		time.Sleep(2 * time.Second)
	}
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
	ret := false

	// Your code here.
	c.lock.Lock()
	if c.state != FINISHED {
		c.lock.Unlock()
		return ret
	}

	fmt.Println("All tasks have done, coordinator return true!")
	ret = true
	c.lock.Unlock()
	//make sure all worker has exited
	time.Sleep(time.Second)
	fmt.Println("All worker has exited!")
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.lock.Lock()
	defer c.lock.Unlock()
	// Your code here.
	//Init coordinator and tasks.
	c.mMap = len(files)
	c.nReduce = nReduce
	c.state = MAPPING
	c.mapTasks = make([]*Task, c.mMap)
	c.reduceTasks = make([]*Task, c.nReduce)
	for i := 0; i < c.mMap; i++ {
		newTask := Task{Id: i, Filename: files[i], State: WAITING, TaskType: MAP}
		c.mapTasks[i] = &newTask
	}
	for i := 0; i < c.nReduce; i++ {
		newTask := Task{Id: i, State: WAITING, TaskType: REDUCE}
		c.reduceTasks[i] = &newTask
	}
	//fmt.Printf("start .task:%p\n", c.mapTasks[0])
	go c.checkTasks()
	c.server()
	return &c
}
