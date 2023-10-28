package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		//如果没有任务了，Worker如何退出？
		//下述代码应该循环执行
		//1.rpc请求任务-获取到任务Task信息
		askTaskArgs := AskTaskArgs{}
		askTaskReply := AskTaskReply{}
		//CallAskTask()
		askTaskRPC := call("Coordinator.AskTask", &askTaskArgs, &askTaskReply)
		if askTaskRPC {
			//fmt.Printf("call AskTask success!\n")
		} else {
			//fmt.Printf("call AskTask failed!\n")
			continue
		}
		var taskType = askTaskReply.TaskType
		//var mMap = askTaskReply.MMap
		//var nReduce = askTaskReply.NReduce
		//fmt.Printf("work &task:%p\n", task)
		//fmt.Println("work task:", task)
		//coordinator return nil,means all tasks have been done!
		//2.map/reduce
		if taskType == MAP {
			taskId := askTaskReply.TaskID
			taskFilename := askTaskReply.Filename
			nReduce := askTaskReply.NReduce
			//2.1map
			//1.打开文件(任务编号-X)
			ok := doMapTask(taskId, taskFilename, nReduce, mapf)
			//ok := true
			//2.调用mapf
			//3.排序kv(感觉这里不用排序) 需要调用ihash(k)%nReduce=Y
			//4.存储至中间文件mr-X-Y
			//5.原子重命名
			//fmt.Println(task)
			taskDoneArgs := TaskDoneArgs{}
			taskDoneReply := TaskDoneReply{}
			taskDoneArgs.TaskID = taskId
			taskDoneArgs.TaskType = taskType

			var doneTaskRPC = false
			if ok {
				doneTaskRPC = call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
			}

			if doneTaskRPC {
				//fmt.Println("Task Done, coordinator has received!")
			}

		} else if taskType == REDUCE {
			taskId := askTaskReply.TaskID
			//2.2reduce
			//1.读取文件(任务编号-Y)
			//2.调用reducef
			//3.排序kv,目的是让相同的word {"word","1"}排到一起
			//4.调用reduce函数
			//5.写入mr-out-Y文件，原子重命名
			//3.rpc任务完成
			//fmt.Println(task)
			taskDoneArgs := TaskDoneArgs{}
			taskDoneReply := TaskDoneReply{}
			taskDoneArgs.TaskID = taskId
			taskDoneArgs.TaskType = taskType
			var doneTaskRPC = false
			ok := doReduceTask(taskId, reducef)
			//ok := true
			if ok {
				doneTaskRPC = call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
			}
			if doneTaskRPC {
				//fmt.Println("Task Done, coordinator has received!")
			}
		} else if taskType == KEEPWAITING {
			time.Sleep(100 * time.Millisecond)
		} else if taskType == NONE {
			break
		}
		//CallTaskDone()
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// do map task,if no err: return true else: return false
func doMapTask(taskID int, filename string, nReduce int, mapf func(string, string) []KeyValue) bool {
	//1.打开文件(任务编号-X)
	fmt.Println(filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	//2.调用mapf
	kva := mapf(filename, string(content))
	//3.排序kv(感觉这里不用排序) 需要调用ihash(k)%nReduce=Y
	kvmap := make(map[int][]KeyValue)
	for _, kv := range kva {
		var key = kv.Key
		var hashY = ihash(key) % nReduce
		if _, exists := kvmap[hashY]; !exists {
			kvmap[hashY] = make([]KeyValue, 0)
		}
		kvmap[hashY] = append(kvmap[hashY], kv)
	}
	//4.存储至中间文件mr-X-Y
	for hashY, kvs := range kvmap {
		//fmt.Println(kvs)
		newFileName := fmt.Sprintf("mr-tmp-%d-%d", taskID, hashY)
		// create temp file
		tempFile, err := ioutil.TempFile("./", "temp-"+newFileName)

		if err != nil {
			log.Fatalf("cannot create %v", newFileName)
			return false
		}
		// 创建JSON编码器
		encoder := json.NewEncoder(tempFile)
		// 使用编码器将数据写入文件
		err = encoder.Encode(kvs)
		if err != nil {
			log.Fatalf("error：%v  when writing %v", err, newFileName)
			tempFile.Close()
			return false
		}
		//5.原子重命名
		err = os.Rename(tempFile.Name(), newFileName)
		if err != nil {
			log.Fatalf("error：%v when rename tempfile: %v", err, newFileName)
			tempFile.Close()
			return false
		}
		tempFile.Close()
	}
	return true
}

// do reduce task,if no err: return true else: return false
func doReduceTask(taskID int, reducef func(string, []string) string) bool {
	//1.读取文件(任务编号-Y)
	fmt.Println(taskID)
	var hashY = taskID
	intermediate := []KeyValue{}
	directory := "./"
	// 使用 filepath.Glob 查找匹配的文件
	filenameReg := fmt.Sprintf("mr-tmp-*-%d", hashY)
	files, err := filepath.Glob(filepath.Join(directory, filenameReg))
	if err != nil {
		log.Fatalf("error：%v when find files with Y: %v", err, hashY)
		return false
	}
	// 2.存储kv
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			file.Close()
			log.Fatalf("cannot open %v", filename)
			return false
		}
		decoder := json.NewDecoder(file)
		var kvs []KeyValue
		if err := decoder.Decode(&kvs); err != nil {
			file.Close()
			log.Fatalf("error：%v when read mr files with Y: %v", err, hashY)
			return false
		}
		intermediate = append(intermediate, kvs...)
		file.Close()
	}
	//3.排序kv,目的是让相同的word {"word","1"}排到一起
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", hashY)
	tempFile, err := ioutil.TempFile("./", "temp-"+oname)
	//4.调用reducef
	//
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
		_, err := fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			tempFile.Close()
			log.Fatalf("error :%v when writing to %v", oname)
			return false
		}
		i = j
	}
	//5.写入mr-out-Y文件，原子重命名
	err = os.Rename(tempFile.Name(), oname)
	if err != nil {
		log.Fatalf("error：%v when rename tempfile: %v", err, oname)
		tempFile.Close()
		return false
	}
	tempFile.Close()
	return true

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
