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
//AskTask
type AskTaskArgs struct {
	//似乎不用
}

type AskTaskReply struct {
	TaskID   int
	TaskType TaskType
	Filename string
	MMap     int
	NReduce  int
}

//AskTask
type TaskDoneArgs struct {
	TaskID   int
	TaskType TaskType
}

type TaskDoneReply struct {
	//似乎不用
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
