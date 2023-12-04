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
	"6.824/labgob"
	"bytes"
	"fmt"
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
const basicTimeout = 150

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
	CommandTerm  int

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
	role        Role
	votedFor    int
	log         []LogEntry
	nextTimeout time.Time
	applyCh     chan ApplyMsg
	// vote info n is the number of peers, voteCount is the number of vote received
	n         int
	voteCount int

	// all servers
	commitIndex int
	lastApplied int

	// for leader
	nextIndex  []int
	matchIndex []int

	// data struct for 2D
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	applyCond 		  *sync.Cond
}

//refresh or init the electron timeout
func (rf *Raft) refreshTimeout() {
	randomMillis := basicTimeout + rand.Intn(200)
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
	isleader = rf.role == LEADER
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	//DPrintf("[Persist Success]:Persist success to persisted state! | %s\n", time.Now().Format("15:04:05.000"))
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil{
		DPrintf("[Restore Error][readPersist()] Restore fail from persisted state! | %s\n", time.Now().Format("15:04:05.000"))
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
		rf.mu.Unlock()
		DPrintf("[Restore Success][readPersist()] Restore success from persisted state! | %s\n", time.Now().Format("15:04:05.000"))
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC hander
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// from old Term
	if args.Term < rf.currentTerm{
		DPrintf("[InstallSnapshot][InstallSnapshot()]:Peer[%d] in term[%d] receive pre Snapshot from Leader[%d] in term[%d]| %s\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, time.Now().Format("15:04:05.000"))
		return
	}
	DPrintf("[InstallSnapshot start][InstallSnapshot()]:Peer[%d] in term[%d] start install a Snapshot from Leader[%d] in term[%d] rf.lastIndex:[%d] args.lastIndex:[%d]| %s\n", rf.me, rf.currentTerm, args.LeaderId, args.Term,rf.lastIncludedIndex,args.LastIncludedIndex, time.Now().Format("15:04:05.000"))

	rf.role = FOLLOWER
	rf.refreshTimeout()

	if args.LastIncludedIndex <= rf.commitIndex{
		DPrintf("[InstallSnapshot]:Peer[%d] in term[%d] receive old Snapshot with lastIndex[%d] but Peer's commitIndex[%d] and | %s\n", rf.me, rf.currentTerm, args.LastIncludedIndex,rf.commitIndex, time.Now().Format("15:04:05.000"))
		return
	}

	if args.LastIncludedIndex>rf.lastIncludedIndex{
		// update snapshot
		var storeIndex = rf.getStoreIndex(args.LastIncludedIndex)
		var lastIndex = rf.lastEntryLogIndex()
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		if args.LastIncludedIndex >= lastIndex{
			// remove all local log
			rf.log=[]LogEntry{{Term: rf.lastIncludedTerm}}

		}else{
			rf.log=append([]LogEntry{{Term: rf.lastIncludedTerm}},rf.log[storeIndex+1:]...)
		}
		// update lastApplied and commitIndex
		rf.snapshot = args.Data
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex

		// apply snapshot
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.log)
		e.Encode(rf.lastIncludedIndex)
		e.Encode(rf.lastIncludedTerm)
		data := w.Bytes()
		rf.persister.SaveStateAndSnapshot(data,rf.snapshot)

		var appMsg = ApplyMsg{
			CommandValid: false,
			Snapshot: args.Data,
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm: args.LastIncludedTerm,
		}
		rf.mu.Unlock()
		rf.applyCh<-appMsg
		rf.mu.Lock()

		DPrintf("[InstallSnapshot]:Peer[%d] in term[%d] receive and install a Snapshot[%d] from Leader[%d] in term[%d]| %s\n", rf.me, rf.currentTerm, args.LastIncludedIndex,args.LeaderId, args.Term, time.Now().Format("15:04:05.000"))
	}else{
		DPrintf("[InstallSnapshot]:Peer[%d] in term[%d] receive old Snapshot from Leader[%d] in term[%d]| %s\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, time.Now().Format("15:04:05.000"))
		return
	}
}

// Send a InstallSnapshot RPC to a server.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// get log entry by logical index
func (rf *Raft) getEntry(logIndex int) LogEntry {
	return rf.log[rf.getStoreIndex(logIndex)]
}
// get physical index by logical index
func (rf *Raft) getStoreIndex(logIndex int) int {
	return logIndex - rf.lastIncludedIndex
}
// get logical index by physical index
func (rf *Raft) getLogIndex(storeIndex int) int {
	return storeIndex + rf.lastIncludedIndex
}
// return the logical index of the last log entry
func (rf *Raft) lastEntryLogIndex() int {
	return rf.getLogIndex(len(rf.log) - 1)
}

// return the term of the last log entry
func (rf *Raft) lastEntryTerm() int {
	return rf.log[len(rf.log) - 1].Term
}

// return the logical length of log
func (rf *Raft) getLogLength() int {
	return len(rf.log)+rf.lastIncludedIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// replace log with snapshot
	DPrintf("[Snapshot start][Snapshot()]:Peer[%d] start save Snapshot with index:[%d] and lastIndex:[%d] log:[%d]| %s\n",rf.me,index,rf.lastIncludedIndex,len(rf.log),time.Now().Format("15:04:05.000"))
	var logIndex = rf.getStoreIndex(index)
	if logIndex<=0 || logIndex >=len(rf.log){
		return
	}
	DPrintf("[Snapshot Doing][Snapshot()]:Peer[%d] is saving Snapshot with index:[%d]| %s\n",rf.me,index,time.Now().Format("15:04:05.000"))
	rf.lastIncludedTerm = rf.getEntry(index).Term
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	rf.log = append([]LogEntry{{Term: rf.lastIncludedTerm}}, rf.log[logIndex+1:]...)
	// todo: check the following code if necessary
	if rf.commitIndex < rf.lastIncludedIndex{
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex{
		rf.lastApplied = rf.lastIncludedIndex
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data,rf.snapshot)
	DPrintf("[Snapshot success][Snapshot()]:Peer[%d] SaveStateAndSnapshot [index:%d | term:%d] log:%v| %s\n",rf.me,rf.lastIncludedIndex,rf.lastIncludedTerm,rf.log,time.Now().Format("15:04:05.000"))
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
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
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	//for future
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries RPC hander
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("[AppendEntries Start]:Peer[%d] in term[%d] receive AppendEntries from Leader[%d]  | %s\n", rf.me, rf.currentTerm, args.LeaderId, time.Now().Format("15:04:05.000"))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommit := args.LeaderCommit
	entries := args.Entries
	leaderId := args.LeaderId

	if term < rf.currentTerm {
		DPrintf("[Pre HeartBeat][AppendEntries()]:Peer[%d] in term[%d] receive pre HeartBeat from Leader[%d] in term[%d]| %s\n", rf.me, rf.currentTerm, leaderId, term, time.Now().Format("15:04:05.000"))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.role = FOLLOWER
	rf.refreshTimeout()
	reply.Term = rf.currentTerm

	// log conflict with leader
	if prevLogIndex >= rf.getLogLength() || (rf.getStoreIndex(prevLogIndex)>=0 && rf.getEntry(prevLogIndex).Term != prevLogTerm) {
		DPrintf("[Log Difference][AppendEntries()]:Peer[%d] in term[%d] logs are different from Leader[%d] in term[%d] with prevLogIndex:[%d]| %s\n", rf.me, rf.currentTerm, leaderId, term,prevLogIndex, time.Now().Format("15:04:05.000"))
		reply.Term = rf.currentTerm
		reply.Success = false
		if prevLogIndex >= rf.getLogLength() {
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.getLogLength()
		} else {
			reply.ConflictTerm = rf.getEntry(prevLogIndex).Term
			var firstIndex = prevLogIndex
			for i := prevLogIndex; i >= rf.lastIncludedIndex; i-- {
				if rf.getEntry(i).Term == reply.ConflictTerm {
					firstIndex = i
				} else {
					break
				}
			}
			reply.ConflictIndex = firstIndex
		}
		return
	}

	//Heartbeat RPC
	reply.Success = true
	if len(entries) == 0 {
		DPrintf("[HeartBeat Received][AppendEntries()]:Peer[%d] in term[%d] receive HeartBeat from Leader[%d] | %s | nextTimeout[%s]\n", rf.me, rf.currentTerm, args.LeaderId, time.Now().Format("15:04:05.000"), rf.nextTimeout.Format("15:04:05.000"))
		//update commitIndex and lastApplied
		if leaderCommit > rf.commitIndex {
			rf.commitIndex = leaderCommit
			if rf.getLogLength()-1 < leaderCommit {
				rf.commitIndex = rf.getLogLength() - 1
			}
			DPrintf("[CommitIndex Follower][AppendEntries()]:Peers[%d] update its CommitIndex to [%d] | %s\n", rf.me, rf.commitIndex, time.Now().Format("15:04:05.000"))
			// notify applyMessage when there are new log entries to apply
			rf.applyCond.Broadcast()
		}
	} else {
		//AppendEntries RPC
		for i := 0; i < len(entries); i++ {
			if prevLogIndex+1+i < rf.getLogLength() && (rf.getStoreIndex(prevLogIndex+1+i)>=0 &&rf.getEntry(prevLogIndex+1+i).Term != entries[i].Term) {
				rf.log = rf.log[:rf.getStoreIndex(prevLogIndex+1+i)]
				rf.log = append(rf.log, entries[i])
			} else if prevLogIndex+1+i >= rf.getLogLength() {
				rf.log = append(rf.log, entries[i])
			}
		}
		rf.persist()
		DPrintf("[AppendEntries Received][AppendEntries()]:Peer[%d] in term[%d] receive AppendEntries from Leader[%d] and log:[%v] keep same as leader | %s\n", rf.me, rf.currentTerm, args.LeaderId, rf.log, time.Now().Format("15:04:05.000"))
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
		rf.persist()
		rf.refreshTimeout()
		return
	} else {
		nextCommitIndex := rf.commitIndex + 1
		for i := nextCommitIndex; i < rf.getLogLength(); i++ {
			var cnt = 0
			for server := 0; server < rf.n; server++ {
				if rf.matchIndex[server] >= i {
					cnt++
				}
			}
			if cnt > rf.n/2 {
				if rf.getEntry(i).Term == rf.currentTerm {
					rf.commitIndex = i
					DPrintf("[CommitIndex Leader][handleAppendEntries()]:Leader[%d] update its CommitIndex to [%d] | %s\n", rf.me, rf.commitIndex, time.Now().Format("15:04:05.000"))
				}
			}else{
				break
			}
		}
		// notify applyMessage when there are new log entries to apply
		if rf.lastApplied < rf.commitIndex{
			rf.applyCond.Broadcast()
		}
	}

}

// Broadcast AppendEntries RPC to other peers
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		// ignore self
		if i == rf.me {
			continue
		}
		var server = i
		var prevLogIndex = rf.nextIndex[i] - 1
		go rf.replicate(server,prevLogIndex)
	}
}

// Ask peer[server] to replicate log entry or send snapshot
func (rf *Raft) replicate(server int,prevLogIndex int)  {
	retryCount:=0
	isSnapshot:=false
	for {
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		DPrintf("[AppendEntries Send][replicate()]:Leader[%d] send an AppendEntries to Peer[%d] with prevLogIndex[%d] | %s\n", rf.me, server, prevLogIndex, time.Now().Format("15:04:05.000"))
		// todo
		if rf.getStoreIndex(prevLogIndex)<0{
			//send snapshot
			var args = InstallSnapshotArgs{
				Term: 				rf.currentTerm,
				LeaderId:			rf.me,
				LastIncludedIndex: 	rf.lastIncludedIndex,
				LastIncludedTerm:   rf.lastIncludedTerm,
				Data:               rf.snapshot,
			}
			var reply = InstallSnapshotReply{}
			rf.mu.Unlock()
			ok := rf.sendInstallSnapshot(server, &args, &reply)
			if ok{
				// InstallSnapshot Fail
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					DPrintf("[InstallSnapshot Fail][replicate()]: Peer[%d]'s Term[%d] larger than Leader[%d]'s Term[%d] and back to follower | %s\n", server, reply.Term, rf.me, rf.currentTerm, time.Now().Format("15:04:05.000"))
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					rf.refreshTimeout()
					rf.mu.Unlock()
					return
				}else{
					// InstallSnapshot success
					DPrintf("[InstallSnapshot success][replicate()]: Peer[%d]'s Term[%d] install Snapshot from Leader[%d]'s Term[%d] and return success | %s\n", server, reply.Term, rf.me, rf.currentTerm, time.Now().Format("15:04:05.000"))
					if rf.matchIndex[server] < args.LastIncludedIndex{
						rf.matchIndex[server] = args.LastIncludedIndex
					}
					if rf.nextIndex[server] <= args.LastIncludedIndex{
						rf.nextIndex[server] = args.LastIncludedIndex+1
					}
					isSnapshot=true
					//rf.mu.Unlock()
					//break
				}
			}else{
				retryCount++
				if retryCount<3{
					DPrintf("[InstallSnapshot Fail][replicate()]: Peer[%d] timeout no reply to Leader[%d] and will retry[%d] | %s\n", server, rf.me, retryCount,time.Now().Format("15:04:05.000"))
					time.Sleep(time.Duration(retryCount*10) * time.Millisecond)
					continue
				}else{
					// Exceeded maximum retry count
					return
				}

			}
		}

		// if installSnapshot, we should update the prevLogIndex
		if isSnapshot{
			prevLogIndex = rf.nextIndex[server] - 1
		}
		// Check if a snapshot has just occurred
		if rf.getStoreIndex(prevLogIndex)<0{
			rf.mu.Unlock()
			continue
		}
		// send AppendEntriesRPC
		var args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.getEntry(prevLogIndex).Term,
			Entries:      make([]LogEntry, 0),
		}
		if prevLogIndex+1 < rf.getLogLength() {
			var temp = rf.log[rf.getStoreIndex(prevLogIndex+1):]
			args.Entries = make([]LogEntry,len(temp))
			copy(args.Entries,temp)
		}
		DPrintf("[AppendEntries Send][replicate()]: Leader[%d] send an AppendEntries to Peer[%d] with prevLogIndex:[%d] and log:[%v]| %s\n", rf.me, server, prevLogIndex, rf.log, time.Now().Format("15:04:05.000"))
		rf.mu.Unlock()
		var reply = AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		if ok {
			appendLogSuccess := reply.Success
			if appendLogSuccess {
				//update follow's nextIndex and matchIndex
				rf.mu.Lock()
				if rf.nextIndex[server] <= prevLogIndex + len(args.Entries){
					rf.nextIndex[server] = prevLogIndex + len(args.Entries) + 1
				}
				if rf.matchIndex[server] < prevLogIndex + len(args.Entries){
					rf.matchIndex[server] = prevLogIndex + len(args.Entries)
				}
				DPrintf("[AppendEntries Success][replicate()]: Peer[%d]'s return success and Leader update its nextIndex[%d] and matchIndex[%d] | %s\n", server,rf.nextIndex[server],rf.matchIndex[server], time.Now().Format("15:04:05.000"))
				rf.mu.Unlock()
				rf.handleAppendEntries(&reply)
				return
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					DPrintf("[AppendEntries Fail][replicate()]: Peer[%d]'s Term[%d] larger than Leader[%d]'s Term[%d] and back to follower | %s\n", server, reply.Term, rf.me, rf.currentTerm, time.Now().Format("15:04:05.000"))
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					rf.refreshTimeout()
					rf.mu.Unlock()
					return
				} else {
					//update args.PrevLogTerm and args.PrevLogIndex--. and try again
					//todo speed up
					if reply.ConflictTerm == -1 {
						prevLogIndex = reply.ConflictIndex - 1
					} else {
						var lastSameTermIndex = -1
						var startIndex = prevLogIndex
						if startIndex >= rf.getLogLength() {
							startIndex = rf.getLogLength() - 1
						}
						for i := startIndex; i >= rf.lastIncludedIndex; i-- {
							if rf.getEntry(i).Term == reply.ConflictTerm {
								lastSameTermIndex = i
								break
							}
						}
						if lastSameTermIndex == -1 {
							prevLogIndex = reply.ConflictIndex - 1
						} else {
							prevLogIndex = lastSameTermIndex
						}
					}
					//prevLogIndex--
					rf.mu.Unlock()
					continue
				}
			}
		}else {
			//timeout
			retryCount++
			if len(args.Entries) == 0 || retryCount>=3{
				return
			}
			//try again
			DPrintf("[AppendEntries Fail][replicate()]: Peer[%d] timeout no reply to Leader[%d] and will retry[%d]| %s\n", server, rf.me, retryCount,time.Now().Format("15:04:05.000"))
			time.Sleep(time.Duration(retryCount*10) * time.Millisecond)
			continue
		}
	}
}


type LogEntry struct {
	Command interface{}
	Term    int
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
	reply.Term = rf.currentTerm

	DPrintf("[Receive RequestVote]:Peer[%d] receive RequestVote RPC from Peer[%d] | %s\n", rf.me, candidateId, time.Now().Format("15:04:05.000"))

	if term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	var hasVoted = (term == rf.currentTerm && rf.votedFor != -1)
	var checkCandidateLog = true

	if lastLogTerm < rf.lastEntryTerm() {
		checkCandidateLog = false
	}
	if lastLogTerm == rf.lastEntryTerm() && lastLogIndex < rf.lastEntryLogIndex() {
		checkCandidateLog = false
	}

	if term == rf.currentTerm {
		if !hasVoted && checkCandidateLog {
			rf.votedFor = candidateId
			rf.persist()
			reply.VoteGranted = true
			DPrintf("[Vote]:Peer[%d] give vote to Peer[%d] for term[%d] | %s\n", rf.me, candidateId, term, time.Now().Format("15:04:05.000"))
		} else {
			reply.VoteGranted = false
		}
		return
	}

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.role = FOLLOWER
		rf.votedFor = -1
		if checkCandidateLog {
			rf.votedFor = candidateId
		}
		rf.persist()
		rf.refreshTimeout()
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == candidateId)
		if reply.VoteGranted {
			DPrintf("[Vote]:Peer[%d] give vote to Peer[%d] for term[%d] | %s\n", rf.me, candidateId, term, time.Now().Format("15:04:05.000"))
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
	DPrintf("[Election][startElection()]:Peer[%d] start election for term[%d] | %s\n", rf.me, rf.currentTerm+1, time.Now().Format("15:04:05.000"))
	//add term
	rf.currentTerm++
	//vote for self
	rf.votedFor = rf.me
	rf.voteCount = 1
	//store state
	rf.persist()
	//become canididate
	rf.role = CANDIDATE
	rf.refreshTimeout()
	// a flag for becomeLeader
	var becomeLeaderOnce sync.Once
	//send RequestVote for all other peers
	var args = RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.lastEntryTerm(),
		LastLogIndex: rf.lastEntryLogIndex(),
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			var reply = RequestVoteReply{}
			DPrintf("[Send RequestVote][startElection()]:Peer[%d] send RequestVote to Peer[%d] | %s\n", rf.me, server, time.Now().Format("15:04:05.000"))
			ok := rf.sendRequestVote(server, args, &reply)
			if ok {
				rf.handleVoteResult(&reply,&becomeLeaderOnce)
			}
		}(i, &args)
	}

}

// One peer electron success and become leader
func (rf *Raft) becomeLeader(){
	DPrintf("[Leader][becomeLeader()]:Peer[%d] become leader for term[%d] | %s\n", rf.me, rf.currentTerm, time.Now().Format("15:04:05.000"))
	rf.role = LEADER
	rf.refreshTimeout()
	//2B
	for i := 0; i < rf.n; i++ {
		rf.nextIndex[i] = rf.getLogLength()
		rf.matchIndex[i] = 0
	}
	go rf.broadcastAppendEntries()
}


// handle vote result
func (rf *Raft) handleVoteResult(reply *RequestVoteReply,becomeLeaderOnce *sync.Once) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := reply.Term
	voteGranted := reply.VoteGranted
	//give up candidate,and back to follower
	if term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.persist()
		rf.refreshTimeout()
	}else if term == rf.currentTerm {
		// receive a granted vote
		if voteGranted {
			rf.voteCount++
			if rf.voteCount > rf.n/2 && rf.role == CANDIDATE {
				// only execute once
				becomeLeaderOnce.Do(rf.becomeLeader)
			}
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		isLeader = false
	} else {
		index = rf.getLogLength()
		term = rf.currentTerm
		//Add to leader's log
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.persist()
		rf.matchIndex[rf.me] = rf.getLogLength() - 1
		rf.nextIndex[rf.me] = rf.getLogLength()
		//Broadcast command
		DPrintf("[Start][Start()]:Peer(Leader)[%d] received Request [%v] | %s\n", rf.me, command, time.Now().Format("15:04:05.000"))
		//DPrintf("[Send AppendEntries(Start)]:Peer(Leader)[%d] in term[%d] send AppendEntries [%v]| %s\n",rf.me,rf.currentTerm,command,time.Now().Format("15:04:05.000"))
		go rf.broadcastAppendEntries()
		//DPrintf("[Start Return (Start)]:Peer(Leader)[%d] start return | %s\n",rf.me,time.Now().Format("15:04:05.000"))
	}
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

func (rf *Raft) applyMessage() {
	for !rf.killed(){
		rf.mu.Lock()
		// wait
		if rf.lastApplied<rf.commitIndex{
			index := rf.lastApplied
			tempEntries := make([]LogEntry,rf.commitIndex-rf.lastApplied)
			copy(tempEntries,rf.log[rf.getStoreIndex(rf.lastApplied+1):rf.getStoreIndex(rf.commitIndex)+1])
			rf.mu.Unlock()
			for _,entry :=range(tempEntries){
				index ++
				fmt.Printf("[ApplyMsg Apply][applyMessage()]: Peer[%d] send a ApplyMsg[%d] lastIndex:[%d]| %s\n", rf.me, index,rf.lastIncludedIndex, time.Now().Format("15:04:05.000"))
				rf.applyCh<-ApplyMsg{
					Command:      entry.Command,
					CommandIndex: index,
					CommandTerm:  entry.Term,
					CommandValid: true,
				}
				//DPrintf("[ApplyMsg Apply][applyMessage()]: Peer[%d] send a ApplyMsg[%d] lastIndex:[%d]| %s\n", rf.me, index,rf.lastIncludedIndex, time.Now().Format("15:04:05.000"))
			}
			//fmt.Printf("[ApplyMsg Apply][applyMessage()]: Peer[%d] send a ApplyMsg[%d] lastIndex:[%d]| %s\n", rf.me, rf.lastApplied,rf.lastIncludedIndex, time.Now().Format("15:04:05.000"))
			//fmt.Printf("------\n")
			rf.mu.Lock()
			if index>rf.lastApplied{
				rf.lastApplied = index
			}
		}else{
			rf.applyCond.Wait()
		}
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var lastHeartBeatTime time.Time
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		//Leader send heartbeat
		if rf.role == LEADER && time.Since(lastHeartBeatTime) > time.Millisecond * heartBeat{
			//Leader send heartbeat
			DPrintf("[Send HeartBeat][ticker()]:Peer(Leader)[%d] send HeartBeat | %s\n", rf.me, time.Now().Format("15:04:05.000"))
			rf.mu.Unlock()
			rf.broadcastAppendEntries()
			lastHeartBeatTime = time.Now()
			continue

		}
		//follower or candidate timeout and start a new electrion
		if rf.role != LEADER && time.Now().After(rf.nextTimeout) {
			DPrintf("[Timeout][ticker()]:Peer[%d] timeout:%v | %s\n", rf.me, rf.nextTimeout, time.Now().Format("15:04:05.000"))
			rf.mu.Unlock()
			rf.startElection()
			continue
		}
		rf.mu.Unlock()
		time.Sleep(onceDuration * time.Millisecond)
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
	// 2A
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.n = len(peers)
	rf.votedFor = -1
	rf.refreshTimeout()
	// 2B
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, rf.n)
	rf.matchIndex = make([]int, rf.n)
	// 2D
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	//fmt.Printf("[Restart]:Peer[%d] , lastApplied[%d]\n",rf.me,rf.lastApplied)
	rf.readPersist(persister.ReadRaftState())
	//fmt.Printf("[Restart]:Peer[%d] from persisted state, lastApplied[%d]\n",rf.me,rf.lastApplied)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMessage()
	return rf
}
