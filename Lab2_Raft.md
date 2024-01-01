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




   
   
1. H. Liu, Y. Wang, J. Yang and Y. Chen, "Fast and practical secret key extraction by exploiting channel response," 2013 Proceedings IEEE INFOCOM, Turin, Italy, 2013, pp. 3048-3056, doi: 10.1109/INFCOM.2013.6567117.
2. Wei Xi et al., "KEEP: Fast secret key extraction protocol for D2D communication," 2014 IEEE 22nd International Symposium of Quality of Service (IWQoS), Hong Kong, 2014, pp. 350-359, doi: 10.1109/IWQoS.2014.6914340.
3. N. Aldaghri and H. Mahdavifar, "Physical Layer Secret Key Generation in Static Environments," in IEEE Transactions on Information Forensics and Security, vol. 15, pp. 2692-2705, 2020, doi: 10.1109/TIFS.2020.2974621.
基于WiFi CSI的无线信道物理层密钥生成，从早期到近年的工作
   
Lu, Youjing, et al. "FREE: A fast and robust key extraction mechanism via inaudible acoustic signal." Proceedings of the Twentieth ACM International Symposium on Mobile Ad Hoc Networking and Computing. 2019.
基于声音信号的工作，常见Baseline，比InaudibleKey稍早

Xi W, Qian C, Han J, et al. Instant and robust authentication and key agreement among mobile devices[C]//Proceedings of the 2016 ACM SIGSAC Conference on Computer and Communications Security. 2016: 616-627.
基于邻近度的工作