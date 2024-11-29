package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Task struct {
	TaskType  int    // 任务状态：Map、Reduce,0-Map,1-Reduce
	FileName  string // 文件切片
	TaskId    int    // 任务ID，生成中间文件要用
	ReduceNum int    // Reduce的数量
}

type Coordinator struct {
	// Your definitions here.
	State      int        // Map Reduce阶段,0-Map,1-Reduce
	MapChan    chan *Task // Map任务channel
	ReduceChan chan *Task // Reduce任务channel
	ReduceNum  int        // Reduce的数量
	Files      []string   // 文件
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) PullTask(args *ExampleArgs, reply *Task) error {
	*reply = *<-c.MapChan
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

// 初始化，生成任务并将生成的任务放入map管道
func (c *Coordinator) MakeMapTasks(files []string) {
	for id, v := range files {
		// 生成任务
		task := Task{TaskType: 0, // Map任务
			FileName:  v,
			TaskId:    id,
			ReduceNum: c.ReduceNum}
		c.MapChan <- &task // 写入通道

		fmt.Println(v, "写入成功！")
	}
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
	//c := Coordinator{} 改了

	// Your code here.
	c := Coordinator{State: 0,
		MapChan:    make(chan *Task, len(files)),
		ReduceChan: make(chan *Task, nReduce),
		ReduceNum:  nReduce,
		Files:      files}

	// 制造Map任务
	c.MakeMapTasks(files)

	c.server()
	return &c
}
