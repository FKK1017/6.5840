# Lab1 MapReduce

## Introduction

## Implementation

整个分布式`MapReduce`系统被分为两个子系统：负责分配工作的`coordinator`和负责执行的`worker`，两种系统之间使用`RPC`进行通信。（只存在`worker`和`coordinator`之间的通信）

显然整个任务被分为两个阶段：
1. `Map`阶段，应该将每个`Input files`划分为`nReduce`个化简任务
    1. Each `mapper` should create `nReduce` intermediate files for consumption by the reduce tasks.
    2. 使用传入的`mapf()`执行`Map`任务：`kva := mapf(reply.Filename, string(content))`
    3. 为了保证`Map`任务的输出不会被提前观测到（实际上这里没有检测存不存在中间文件），需要使用临时文件，处理完后再改名
    4. `mapf()`的输出是一个`KeyValue`的数组，通过`ihash()`按`key`进行散列，具有一致哈希值的`key`被写入同一个`json`文件
2. `Map`阶段之后，一共应该输出$X*nReduce$个中间文件`mr-X-Y`，$X$是输入文件的数量
   1. 需要确保`Map`全部执行完后才执行`Reduce`，因此需要实现一个远程的同步操作
   2. 使用`go`的管道实现这个同步（对于检测`Reduce`）的完成也是一样的
   3. 每次分配一个任务的同时，`coordinator`会启动一个新线程，尝试从对应任务的管道（例如：`case <-c.MapJobs.chans[index]:`）和定时器（`case <-time.After(10 * time.Second):`）同时读取
   4. `worker`在完成一个工作后，会主动通过`RPC`调用`coordinator.FinishWork`报告自己完成的工作及其状态，`coordinator`收到后会在对应的管道写入，没有超时的情况下，完成了同步
   5. `coordinator`会在每次从管道中读取任务状态后，遍历所有任务的状态，如果所有任务都已完成，则将对应的`Done`属性置为`true`，这是提供给`Done()`方法判断是否完成的标志
3. `Reduce`阶段的输出应该是`nReduce`个`mr-out-Y`文件
   1. 每个`worker`输出一个`mr-out-Y`，因此要读取所有与`Y`一致的中间文件`mr-X-Y`，存储到一个中间变量`kva`
   2. 之后对`kva`按`Key`排序
   3. 剩余的部分和`mrsequential.go`一致
   4. 同步的做法与`Map`阶段一致
4. `RPC`设计
   1. 一个空结构体，用于请求任务的`args`和报告任务完成的`reply`
   2. `WorkType`，用于请求任务的`reply`
      1. ```go
            type WorkType struct {
                WorkType string
                Filename string
                X        int
                Y        int
            }
            ```
      2. `X`在`Map`阶段是对应输入文件的下标，在`Reduce`阶段是输入文件的个数（用于读取所有的`Y`）
      3. `Y`在`Map`阶段是`nReduce`，在`Reduce`阶段是对应输出文件的下标
   3. `Finished`，用于报告工作完成
      1. ```go
            type Finished struct {
            WorkType string
            Index    int
        }
            ```
      2. `Index`在`Map`阶段对应`X`，`Reduce`阶段对应`Y`
5. 踩坑
   1. `defer`不知道为什么没有先返回再运行，使用`go`开一个新线程解决了
   2. 初始化由管道组成的数组时，需要用一个循环，先`make(chan Type)`再`append`
   3. `select`从无缓存管道读可以用来做同步/超时
   4. 竞争的处理，现在是访问边缘变量就上锁
   