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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type ServerState int32

const (
	Follower      ServerState = 0
	Candidate     ServerState = 1
	Leader        ServerState = 2
)

const (
	min_t   int = 300
	max_t   int = 1000
)

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

//
//A Go object implementing log entry
//
type LogEntry struct{
	index	int
	term	int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state
	currentTerm int         //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int		    //candidateId that received vote in current term (or null if none)
	log         []LogEntry  //log entries; each entry contains command for state machine

	//Volatile state all server
	state       			ServerState
	heartBeatsLastReceived  time.Time
	commitIndex 			int 		//index of highest log entry known to be committed
	lastApplied 			int			//index of highest log entry applied to state machine

	//leader's volatile state
	nextIndex  []int		//for each server, index of the next log entry to send to that server
	matchIndex []int		//for each server, index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader{
		isleader = true
	}else{
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 	     int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//to do: need to check whether the log is as updated as itself
	//Part B

	//if rpc request's term is bigger than server's current term,
	//set state to follower and set currentTerm to the request's term
	if args.Term > rf.currentTerm{
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	///if rpc request's term is smaller than server's current term,
	//or the state is leader or candidate, refuse vote, or already voted
	if args.Term < rf.currentTerm || rf.state == Leader || rf.state == Candidate || rf.votedFor != -1{
		return
	}
	rf.votedFor = args.CandidateID
	reply.VoteGranted = true
	rf.heartBeatsLastReceived = time.Now()
	fmt.Printf("server %d vote to %d\n",rf.me,rf.votedFor)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct{
	Term 				  int       //leader’s term
	LeaderId    		  int       //so follower can redirect clients
	PrevLogIndex 		  int       //index of log entry immediately preceding new ones
	PrevLogTermEntries  []int       //term of prevLogIndex entry log entries to store
									// (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit 	 	  int	    //leader’s commitIndex

}

type AppendEntriesReply struct{
	Term     	int  		//currentTerm, for leader to update itself
	Success     bool 		//true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = true

	//If argument's term is smaller than currentTerm, means it is not a valid message
	if args.Term < rf.currentTerm{
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
	}

	logLength := len(args.PrevLogTermEntries)
	if logLength == 0{
		fmt.Printf("server %d received heartbeat from %d\n",rf.me,args.LeaderId)
		// it is a heartbeat reply record the time
		rf.heartBeatsLastReceived = time.Now()
		//if rf is candidate or leader, convert it to follower
		if rf.state != Follower {
			rf.state = Follower
			rf.votedFor = args.LeaderId
		}
		return
	}else{
		//Need to be filled in log part
		//....
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) HeartBeat() {
	//if the server is elected as leader, send heart beat to all other servers
	//Also, periodically leader send HeartBeat to other servers
	AckReceived := 1
	ReplyReceived := 1
	AckChan := make(chan bool,len(rf.peers))
	startTime := time.Now()

	for i,_ := range rf.peers{
		if i != rf.me{
			 go func(target int){
					var appendEntriesArgs  		AppendEntriesArgs
					var appendEntriesReply 		AppendEntriesReply
					rf.mu.Lock()
					appendEntriesArgs.PrevLogTermEntries =  nil
					appendEntriesArgs.LeaderId 			 = 	rf.me
					appendEntriesArgs.Term     			 = 	rf.currentTerm
					appendEntriesArgs.PrevLogIndex 		 =  -1
					appendEntriesArgs.LeaderCommit       =  -1
					rf.mu.Unlock()

					ok :=  rf.sendAppendEntries(target, &appendEntriesArgs, &appendEntriesReply)
					if ok {
						if appendEntriesReply.Success{
							AckChan <- true
						}else {
							AckChan <- false
						}
					}else {
						rf.mu.Lock()
						fmt.Printf("Heartbeat fail from %d to send to %d\n",rf.me,target)
						if rf.currentTerm < appendEntriesReply.Term{
							rf.currentTerm =  appendEntriesReply.Term
							rf.state = Follower
							rf.votedFor = -1
						}
						rf.mu.Unlock()
						AckChan <- false
					}
			}(i)
		}
	}


	for{

		if time.Now().After(startTime.Add(time.Millisecond * time.Duration(max_t * 2))){
			break
		}

		select {
		case AckResult := <-AckChan:
			ReplyReceived++
			if AckResult {
				AckReceived++
			}
			if ReplyReceived > len(rf.peers)/2 {
				goto end
			}
		}
	}
end:
	rf.mu.Lock()
	if AckReceived <= len(rf.peers)/2{
		rf.state = Follower
		rf.votedFor = -1
		fmt.Printf("server %d -> follower.\n",rf.me)//Follower case
	}
	rf.mu.Unlock()
}

func (rf *Raft) HeartBeatTicker(){
	for rf.killed() == false{
		time.Sleep(time.Millisecond * 150)

		_, isLeader := rf.GetState()
		if isLeader {
			rf.HeartBeat()
		}else{
			return
		}
	}
}

func (rf *Raft) ConvertState(target ServerState) {
	rf.mu.Lock()
	if target == rf.state {
		rf.mu.Unlock()
		return
	}
	if target == Follower{
		fmt.Printf("server %d -> follower.\n",rf.me)
		rf.votedFor = -1
		rf.state = Follower
		rf.mu.Unlock()
	} else if target == Candidate{
		fmt.Printf("server %d -> candidate.\n",rf.me)
		rf.state = Candidate						//change state to candidate
		rf.currentTerm++
		rf.votedFor = rf.me 						//vote for itself
		rf.heartBeatsLastReceived = time.Now()      //reset the election timer
		rf.mu.Unlock()
	} else{
		fmt.Printf("server %d -> leader.\n",rf.me)//Leader case
		rf.state = Leader
		rf.mu.Unlock()
		rf.HeartBeat()
		go rf.HeartBeatTicker()
	}
}

func (rf *Raft) StartElection() {
	voteCnt := 1
	VoteResultCnt := 1
	voteChan := make(chan bool,len(rf.peers))
	startTime := time.Now()


	for i,_ := range rf.peers{
		if i == rf.me{
			continue
		}
		go func(target int) {
			rf.mu.Lock()
			var requestVoteArg     RequestVoteArgs
			var requestVoteReply   RequestVoteReply
			requestVoteArg.Term        = rf.currentTerm
			requestVoteArg.CandidateID = rf.me
			rf.mu.Unlock()
			//Code for 2B
			//requestVoteArg.lastLogIndex =
			//requestVoteArg.lastLogTerm  =
			ok := rf.sendRequestVote(target, &requestVoteArg, &requestVoteReply)
				if ok {
					fmt.Printf("%d send vote request to %d\n",rf.me,target)
					if requestVoteReply.VoteGranted {
						voteChan <- true
					}else {
						voteChan <-false
					}
				}else{
					fmt.Printf("requsetvote fail from %d  to %d\n",rf.me,target)
					voteChan <-false
				}
			}(i)
		}
	for {
		//If didn't get response enough response in 2 times max timeout duration
		//stop the election process
		if time.Now().After(startTime.Add(time.Millisecond * time.Duration(max_t * 2))){
			break
		}

		select {
		case voteResult := <-voteChan:
			VoteResultCnt++
			if voteResult {
				voteCnt++
			}
			if VoteResultCnt == len(rf.peers) || voteCnt > len(rf.peers)/2 {
				goto end
			}
		}
	}
	end:
		if voteCnt > len(rf.peers)/2{
			rf.mu.Lock()
			rf.state = Leader
			fmt.Printf("server %d -> leader.\n",rf.me)//Leader case
			rf.mu.Unlock()
			rf.HeartBeat()
		}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ElectionTicker() {
	for rf.killed() == false {
		// Check if a leader election should be started and to randomize sleeping time using time.Sleep().
		sleepT := rand.Intn(max_t - min_t + 1) + min_t
		time.Sleep(time.Duration(sleepT) * time.Millisecond)
		rf.mu.Lock()
		heartBeatsLastReceived := rf.heartBeatsLastReceived
		state := rf.state
		rf.mu.Unlock()
		//Debug message
		fmt.Printf("server %d heartBeatsLastReceived: %d ms.\n",rf.me,time.Now().Sub(heartBeatsLastReceived).Milliseconds())
		fmt.Printf("server %d sleepT %d\n",rf.me,sleepT)
		//if is not leader and in the sleep time didn't receive the heartbeat message
		if state != Leader {
			if time.Since(heartBeatsLastReceived) > time.Duration(sleepT)*time.Millisecond {
				fmt.Printf("server %d -> candidate.\n", rf.me)
				rf.mu.Lock()
				rf.state = Candidate                   //change state to candidate
				rf.currentTerm++                       //increment term
				rf.votedFor = rf.me                    //vote for itself
				rf.heartBeatsLastReceived = time.Now() //reset the election timer
				rf.mu.Unlock()
				go rf.StartElection()
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.votedFor = -1
    rf.currentTerm = 0
    rf.log = make([]LogEntry,0)
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// election ticker goroutine to start elections
	go rf.ElectionTicker()
	// HearBeatTicker only for the Leader to send HeartBeat Message
	go rf.HeartBeatTicker()

	return rf
}
