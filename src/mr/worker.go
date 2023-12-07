package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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
	// args:
	// 	mapf: Map Function
	//	reducef: Reduce Function

	for true {
		args_empty := EmptyArgs{}
		reply_empty := EmptyReply{}
		reply_type := WorkType{}
		ok := call("Coordinator.WorkType", &args_empty, &reply_type)

		if ok {
			if reply_type.WorkType == "Map" {
				// fmt.Printf("Map: %v.\n", reply_type.Filename)
				Mapper(&reply_type, mapf)
				finish := Finished{
					WorkType: "Map",
					Index:    reply_type.X,
				}
				ok = call("Coordinator.FinishWork", &finish, &reply_empty)

			} else if reply_type.WorkType == "Reduce" {
				//fmt.Printf("Reduce: %v.\n", reply_type.Filename)
				Reducer(&reply_type, reducef)
				finish := Finished{
					WorkType: "Reduce",
					Index:    reply_type.Y,
				}
				ok = call("Coordinator.FinishWork", &finish, &reply_empty)
				// if ok {
				// 	time.Sleep(1 * time.Second)
				// }
			} else if reply_type.WorkType == "Exit" {
				return
			} else if reply_type.WorkType == "Wait" {
				time.Sleep(1 * time.Second)
			}
		} else {
			fmt.Printf("call failed.\n")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Mapper(reply *WorkType, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	kva := mapf(reply.Filename, string(content))

	nReduce := reply.Y
	x := reply.X
	dir, _ := os.Getwd()
	tempFiles := make([]*os.File, nReduce)

	for i := 0; i < nReduce; i++ {
		fname := "mr-" + strconv.Itoa(x) + "-" + strconv.Itoa(i)
		inter, err := os.CreateTemp(dir, fname+"-*")
		if err != nil {
			log.Fatalf("cannot create %v temp file", fname)
		}
		tempFiles[i] = inter
		defer inter.Close()
		defer os.Rename(inter.Name(), fname)
	}

	for _, kv := range kva {
		y := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(tempFiles[y])
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", tempFiles[y].Name())
		}
	}
}

func Reducer(reply *WorkType, reducef func(string, []string) string) {
	//fmt.Printf("%v %v\n", reply.Filename, reply.X)
	X := reply.X
	Y := reply.Y
	kva := []KeyValue{}
	for i := 0; i < X; i++ {
		fname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(Y)
		file, err := os.Open(fname)
		if err != nil {
			log.Fatalf("cannot open %v", fname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(Y)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}
	defer ofile.Close()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
