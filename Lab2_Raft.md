# Lab2 Raft

## Part 2A: leader election

### Introduction

### Implementation

先写一下思路：
Raft Server存在三个状态：
1. `Follower`
2. `Candidate`
3. `Leader`

三个状态可以互相转换，具体参考图4，下面写一下对实现的理解

1. 一开始Server均处于`Follower`状态，在触发计时器超时后，进入`Candidate`状态
2. 对于`Candidate`，同样会触发计时器超时，之后再次进入`Candidate`
3. 在`Candidate`状态会向别的`Server`发送`RequestVote`，获得多数选票后进入`Leader`状态
4. 所有状态都可能收到新的`RequestVote`(`args.Term >= rf.currentTerm`)，分类
   1. `Follower`: 如果严格大于，为其投票，如果等于，则先到先得
   2. `Candidate`: 如果严格大于，为其投票，并转换为`Follower`
   3. `Leader`: 如果严格大于，为其投票，并转换为`Follower`
5. 选出来之后，`Leader`定期发心跳包


##


t 0 1 2
8 1 1 ?
9 0   0