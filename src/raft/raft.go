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
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const onceDuration = 10
const heartBeat = 50
const basicTimeout = 200

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
type Role int
const (
	FOLLOWER Role = iota
	LEADER
	CANDIDATE
)

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
	currentTerm int
	role 		Role
	votedFor 	int
	log			[]LogEntry
	nextTimeout	time.Time

	// vote info n is the number of peers, voteCount is the number of vote received
	n			int
	voteCount	int
}

//refresh or init the electron timeout
func (rf *Raft) refreshTimeout(){
	randomMillis := basicTimeout + rand.Intn(201)
	randomDuration := time.Duration(randomMillis) * time.Millisecond
	rf.nextTimeout = time.Now().Add(randomDuration)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role==LEADER
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
	Term 			int
	CandidateId 	int
	LastLogIndex 	int
	LastLogTerm 	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int
	VoteGranted bool
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term 			int
	LeaderId    	int
	PrevLogIndex	int
	PrevLogTerm 	int
	Entries			[]LogEntry
	//for future
	LeaderCommit	int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term 		int
	Success		bool
}

// AppendEntries RPC hander
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	entries := args.Entries

	if entries==nil{

		if term < rf.currentTerm{
			reply.Term=rf.currentTerm
			reply.Success = false
			DPrintf("[HeartBeat]:Peer[%d] in term[%d] receive pre HeartBeat from Leader[%d] | %s\n",rf.me,rf.currentTerm,args.LeaderId,time.Now().Format("15:04:05.000"))
		}else if term == rf.currentTerm{
			rf.role=FOLLOWER
			rf.refreshTimeout()
			reply.Term = rf.currentTerm
			reply.Success = true
			DPrintf("[HeartBeat]:Peer[%d] in term[%d] receive HeartBeat from Leader[%d] | %s | nextTimeout[%s]\n",rf.me,rf.currentTerm,args.LeaderId,time.Now().Format("15:04:05.000"),rf.nextTimeout.Format("15:04:05.000"))
		}else{
			rf.role=FOLLOWER
			rf.refreshTimeout()
			rf.currentTerm=term
			rf.votedFor=-1
			reply.Term = rf.currentTerm
			reply.Success = true
			DPrintf("[HeartBeat]:Peer[%d] in term[%d] receive HeartBeat from Leader[%d] | %s | nextTimeout[%s]\n",rf.me,rf.currentTerm,args.LeaderId,time.Now().Format("15:04:05.000"),rf.nextTimeout.Format("15:04:05.000"))
		}
	}
}

// Send a RequestVote RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Handle other peers's AppendEntries RPC reply
func (rf *Raft) handleAppendEntries(reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := reply.Term

	if term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.refreshTimeout()
		return
	}

}

// Broadcast AppendEntries RPC to other peers
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i:=0;i<len(rf.peers);i++{
		if i==rf.me{
			continue
		}
		var args = AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			Entries: nil,
		}
		var server = i
		go func(server int, args *AppendEntriesArgs) {
			var reply = AppendEntriesReply{}
			ok:=rf.sendAppendEntries(server,args,&reply)
			if ok{
				rf.handleAppendEntries(&reply)
			}
		}(server,&args)
	}
}


type LogEntry struct {
	Command 	interface{}
	Term 		int
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm
	candidateId := args.CandidateId

	DPrintf("[Receive RequestVote]:Peer[%d] receive RequestVote RPC from Peer[%d] | %s\n",rf.me,candidateId,time.Now().Format("15:04:05.000"))

	if term < rf.currentTerm{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	var hasVoted = (term==rf.currentTerm&& rf.votedFor!=-1)
	var checkCandidateLog = true


	if lastLogTerm < rf.log[len(rf.log)-1].Term{
		checkCandidateLog = false
	}
	if lastLogTerm == rf.log[len(rf.log)-1].Term && lastLogIndex < len(rf.log)-1{
		checkCandidateLog = false
	}

	if term == rf.currentTerm{
		if !hasVoted && checkCandidateLog{
			rf.votedFor = candidateId
			reply.VoteGranted = true
			DPrintf("[Vote]:Peer[%d] give vote to Peer[%d] for term[%d] | %s\n",rf.me,candidateId,term,time.Now().Format("15:04:05.000"))
		}else{
			reply.VoteGranted = false
		}
		reply.Term=rf.currentTerm
		return
	}


	if term > rf.currentTerm{
		rf.currentTerm = term
		rf.role = FOLLOWER
		rf.votedFor = -1
		if checkCandidateLog{
			rf.votedFor = candidateId
		}
		rf.refreshTimeout()
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor==candidateId)
		if (reply.VoteGranted){
			DPrintf("[Vote]:Peer[%d] give vote to Peer[%d] for term[%d] | %s\n",rf.me,candidateId,term,time.Now().Format("15:04:05.000"))
		}
		return
	}



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

// start a new election
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Election]:Peer[%d] start election for term[%d] | %s\n",rf.me,rf.currentTerm+1,time.Now().Format("15:04:05.000"))
	//add term
	rf.currentTerm++
	//vote for self
	rf.votedFor = rf.me
	rf.voteCount = 1
	//become canididate
	rf.role = CANDIDATE
	rf.refreshTimeout()
	//send RequestVote for all other peers
	var args = RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
		LastLogIndex: len(rf.log)-1,
	}
	for i:=0;i<len(rf.peers);i++{
		if i==rf.me{
			continue
		}
		go func(server int,args *RequestVoteArgs) {
			var reply = RequestVoteReply{}
			DPrintf("[Send RequestVote]:Peer[%d] send RequestVote to Peer[%d] | %s\n",rf.me,server,time.Now().Format("15:04:05.000"))
			ok:= rf.sendRequestVote(server,args,&reply)
			if ok{
				rf.handleVoteResult(&reply)
			}
		}(i,&args)
	}

}

// handle vote result
func (rf *Raft) handleVoteResult(reply *RequestVoteReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := reply.Term
	voteGranted := reply.VoteGranted
	//give up candidate,and back to follower
	if term>rf.currentTerm{
		rf.role = FOLLOWER
		rf.currentTerm = term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.refreshTimeout()
		return
	}

	if term < rf.currentTerm{
		return
	}

	if term == rf.currentTerm{
		if voteGranted{
			rf.voteCount++
			if rf.voteCount>rf.n/2 && rf.role==CANDIDATE{
				DPrintf("[Leader]:Peer[%d] become leader for term[%d] | %s\n",rf.me,rf.currentTerm,time.Now().Format("15:04:05.000"))
				rf.role=LEADER
				rf.refreshTimeout()
				go rf.broadcastAppendEntries()
			}
		}
	}
	return

}


//
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


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var count = 0
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(onceDuration*time.Millisecond)
		count++
		rf.mu.Lock()
		if count%(heartBeat/onceDuration)==0 && rf.role == LEADER{
			count=0
			//Leader send heartbeat
			DPrintf("[Send HeartBeat]:Peer(Leader)[%d] send HeartBeat | %s\n",rf.me,time.Now().Format("15:04:05.000"))
			rf.mu.Unlock()
			rf.broadcastAppendEntries()
			continue

		}
		//timeout
		if rf.role != LEADER && time.Now().After(rf.nextTimeout){
			DPrintf("[Timeout]:Peer[%d] timeout:%v | %s\n",rf.me,rf.nextTimeout,time.Now().Format("15:04:05.000"))
			rf.mu.Unlock()
			rf.startElection()
			continue
		}
		rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.n = len(peers)
	rf.log = make([]LogEntry,1)
	rf.refreshTimeout()
	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
