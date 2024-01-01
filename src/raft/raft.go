package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	lastTime  time.Time
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor     int
	currentState RaftState
	votes        int
	leaderchan   chan int

	log         []LogRaft
	commitIndex int
	lastApplied int

	co sync.Cond

	//Leader
	nextIndex  []int //for every follower
	matchIndex []int
}

type LogRaft struct {
	// Index   int
	Term    int
	Command interface{}
}

type RaftState int

const (
	followerState  RaftState = 101
	candidateState RaftState = 102
	LeaderState    RaftState = 103
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.currentState == LeaderState {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// func (rf *Raft) persist() {
// 	// Your code here (2C).
// 	// Example:
// 	// w := new(bytes.Buffer)
// 	// e := labgob.NewEncoder(w)
// 	// e.Encode(rf.xxx)
// 	// e.Encode(rf.yyy)
// 	// raftstate := w.Bytes()
// 	// rf.persister.Save(raftstate, nil)
// }

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentState = followerState
			// fmt.Printf("%v become follower in RequestVote at %v\n", rf.me, time.Now())
			rf.currentTerm = args.Term
			rf.votes = 0
			rf.votedFor = -1
		}
		// fmt.Printf("argsTerm is %v and rfTerm is %v\n", args.LastLogTerm, rf.log[rf.lastApplied].Term)
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if (args.LastLogTerm == rf.log[rf.commitIndex].Term && args.LastLogIndex >= rf.commitIndex) ||
				(args.LastLogTerm > rf.currentTerm) {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.lastTime = time.Now().Add(time.Duration(200+rand.Int()%250) * time.Millisecond)
				// fmt.Printf("%v vote to %v in term %v at %v\n", rf.me, args.CandidateId, args.Term, time.Now())
			}
		} else {
			reply.VoteGranted = false
		}
		reply.Term = rf.currentTerm

	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState == candidateState && reply.VoteGranted {
		rf.votes++
		if rf.currentState == candidateState && (rf.votes*2 > (len(rf.peers))) {
			rf.currentState = LeaderState
			rf.leaderchan <- 1
			// fmt.Printf("%v become leader with term %v at %v\n", rf.me, rf.currentTerm, time.Now())
		}
	} else if ok && reply.Term == 0 {
		rf.currentState = followerState
		// fmt.Printf("%v become follower in sendRequestVote at %v\n", rf.me, time.Now())
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState == LeaderState {
		isLeader = true
		index = rf.nextIndex[rf.me]
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me] = index
		// fmt.Printf("%v index is %v\n", command, index)
		term = rf.currentTerm
		go func(index int, term int, command interface{}) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			curLog := LogRaft{
				// Index:   index,
				Term:    term,
				Command: command,
			}
			for len(rf.log) < index {
				rf.co.Wait()
			}
			rf.log = append(rf.log, curLog)
			rf.co.Broadcast()
			// fmt.Printf("%v log is %v\n", rf.me, rf.log)
			// rf.matchIndex[rf.me]++
			// rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
			go rf.newCommand(index)
		}(index, term, command)
	} else {
		isLeader = false
	}
	return index, term, isLeader
}

func (rf *Raft) newCommand(newCmdIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println(rf.log)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// fmt.Println(rf.log[newCmdIndex:])
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			LeaderCommit: rf.commitIndex,
			Entries:      rf.log[rf.nextIndex[i]:],
		}
		// fmt.Printf("send to %v, Entries is %v, PrevLogIndex is %v\n", i, args.Entries, args.PrevLogIndex)
		reply := AppendEntriesReply{}
		go rf.sendHeartBeat(i, &args, &reply)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const heartbeatTime = 100

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 200)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(time.Duration(heartbeatTime) * time.Millisecond)
		rf.mu.Lock()
		if rf.currentState != LeaderState && time.Now().After(rf.lastTime) {
			//trans to candidate
			go rf.becomeCandidate()
		}
		if rf.commitIndex > rf.lastApplied {
			go rf.applyCmd()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeLeader() {
	for !rf.killed() {
		// time.Sleep(time.Duration(heartbeatTime) * time.Millisecond)
		// fmt.Printf("%v check leader at %v\n", rf.me, time.Now())
		select {
		case <-time.After(time.Duration(heartbeatTime) * time.Millisecond):
		case <-rf.leaderchan:
			// rf.currentState = LeaderState
			rf.mu.Lock()
			next := rf.commitIndex + 1
			// fmt.Printf("%v cmtIndex is %v \n", rf.me, rf.commitIndex)
			for i := range rf.nextIndex {
				rf.nextIndex[i] = next
				rf.matchIndex[i] = 0
			}
			rf.mu.Unlock()
		}
		rf.mu.Lock()

		if rf.currentState == LeaderState {
			count := 1
			N := rf.commitIndex + 100
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				go rf.sendHeartBeat(i, &args, &reply)
			}

			for i, v := range rf.matchIndex {
				if i == rf.me {
					continue
				}
				if v > rf.commitIndex {
					N = min(N, v)
					count++
				}
			}

			if len(rf.log)-1 >= N && rf.log[N].Term == rf.currentTerm && count*2 >= len(rf.peers) {
				rf.commitIndex = N
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyCmd() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("%v log is %v, cmtIndex is %v\n", rf.me, rf.log, rf.commitIndex)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentState = candidateState
	rf.currentTerm++
	// fmt.Printf("%v become candidate in term %v at %v\n", rf.me, rf.currentTerm, time.Now())
	rf.votedFor = rf.me
	rf.votes = 1
	rf.lastTime = time.Now().Add(time.Duration(200+rand.Int()%250) * time.Millisecond)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.currentState != candidateState {
			break
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogRaft
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// fmt.Printf("%v send heartbeat to %v in term %v at %v\n", rf.me, server, args.Term, time.Now())
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return ok
	}
	if !reply.Success {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term == 0 {
			// term not correct, become follower
			rf.lastTime = time.Now().Add(time.Duration(200+rand.Int()%250) * time.Millisecond)
			rf.currentState = followerState
		} else {
			rf.nextIndex[server]--
			// fmt.Printf("%v nextIndex --, is %v\n", server, rf.nextIndex[server])
		}
		// fmt.Printf("%v become follower in sendHeartBeat at %v\n", rf.me, time.Now())
		// fmt.Printf("%v has bigger term than %v: %v > %v\n", server, args.LeaderId, reply.Term, args.Term)
	} else if reply.Success && args.Entries != nil {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// fmt.Printf("PrevLogIndex is %v and Entries is %v\n", args.PrevLogIndex, args.Entries)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		// fmt.Printf("%v matchIndex is %v\n", server, rf.matchIndex[server])
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("%v get heartbeat from %v in term %v at %v\n", rf.me, args.LeaderId, args.Term, time.Now())
	if args.Term < rf.currentTerm {
		// fmt.Printf("%v has bigger term than %v: %v > %v\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
	} else {
		rf.lastTime = time.Now().Add(time.Duration(200+rand.Int()%250) * time.Millisecond)
		// fmt.Printf("%v next timeout is %v\n", rf.me, rf.lastTime)
		// reply.Success = true
		rf.currentTerm = args.Term
		rf.currentState = followerState
		// fmt.Printf("%v become follower in AppendEntries at %v\n", rf.me, time.Now())
		rf.votedFor = args.LeaderId
		reply.Term = rf.currentTerm
		loglen := len(rf.log)
		// fmt.Printf("Index: %v, Term: %v\n", args.PrevLogIndex, args.PrevLogTerm)
		if (loglen == 1) || (loglen-1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
			reply.Success = true
			if args.Entries != nil {
				// fmt.Printf("%v log append %v, now is %v\n", rf.me, args.Entries, rf.log)
				// rf.log = rf.log[0:args.PrevLogIndex]
				j := 0
				for i := args.PrevLogIndex + 1; i < len(rf.log); i++ {
					if j >= len(args.Entries) {
						break
					}
					if rf.log[i].Term != args.Entries[j].Term {
						rf.log = rf.log[0 : i+1]
						break
					} else {
						j++
					}
				}
				rf.log = append(rf.log, args.Entries[j:]...)
				// fmt.Printf("%v log is %v\n", rf.me, rf.log)
			}
		} else {
			reply.Success = false
			// fmt.Printf("%v loglen-1 is %v, PrevLogIndex is %v\n", rf.me, loglen-1, args.PrevLogIndex)
			// fmt.Printf("last log term is %v, PrevLogTerm is %v\n", rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
			// if loglen-1 >= args.PrevLogIndex {
			// 	rf.log = rf.log[0:args.PrevLogIndex]
			// }
		}
		// fmt.Printf("%v cmtIndex is %v, leaderCmt is %v, len(log)-1 is %v\n", rf.me, rf.commitIndex, args.LeaderCommit, len(rf.log)-1)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentState = followerState
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votes = 0
	rf.leaderchan = make(chan int, 1)
	rf.co = *sync.NewCond(&rf.mu)

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]LogRaft, 0)

	rf.log = append(rf.log, LogRaft{
		Term: 0,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastTime = time.Now().Add(time.Duration(200+rand.Int()%250) * time.Millisecond)
	// fmt.Printf("%v first timeout is %v\n", rf.me, rf.lastTime)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.becomeLeader()

	return rf
}
