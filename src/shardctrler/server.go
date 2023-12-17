package shardctrler

import (
	"6.824/raft"
	"log"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastApplied int
	waitChMap 		map[int]chan CommandReply
	cacheMap		map[int64]int64
	configs []Config // indexed by config num
}

type OpType string

const (
	JOIN OpType		="Join"
	LEAVE			="Leave"
	MOVE  			="Move"
	QUERY  			="Query"
)

type Op struct {
	// Your data here.
	// for Join
	Servers 	map[int][]string
	// for Leave
	GIDs []		int
	// for Move
	Shard 		int
	GID   		int
	// for Query
	Num 		int
	OpType 		OpType
	ClientId	int64
	CommandId	int64
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("[Join Start]:Receive args:%v | %s\n",args,time.Now().Format("15:04:05.000"))
	// Your code here.
	op := Op{
		OpType: JOIN,
		Servers: args.Servers,
		ClientId: args.ClientId,
		CommandId: args.CommandId,
	}
	commandReply := sc.Command(op)
	reply.Err = commandReply.Err
	reply.WrongLeader = commandReply.WrongLeader
	DPrintf("[Join Return]:Receive args:%v and Return reply:%v | %s\n",args,reply,time.Now().Format("15:04:05.000"))
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("[Leave Start]:Receive args:%v | %s\n",args,time.Now().Format("15:04:05.000"))
	// Your code here.
	op := Op{
		OpType: LEAVE,
		GIDs: args.GIDs,
		ClientId: args.ClientId,
		CommandId: args.CommandId,
	}
	commandReply := sc.Command(op)
	reply.Err = commandReply.Err
	reply.WrongLeader = commandReply.WrongLeader
	DPrintf("[Leave Return]:Receive args:%v and Return reply:%v | %s\n",args,reply,time.Now().Format("15:04:05.000"))
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	DPrintf("[Move Start]:Receive args:%v | %s\n",args,time.Now().Format("15:04:05.000"))
	// Your code here.
	op := Op{
		OpType: MOVE,
		Shard: args.Shard,
		GID: args.GID,
		ClientId: args.ClientId,
		CommandId: args.CommandId,
	}
	commandReply := sc.Command(op)
	reply.Err = commandReply.Err
	reply.WrongLeader = commandReply.WrongLeader
	DPrintf("[Move Return]:Receive args:%v and Return reply:%v | %s\n",args,reply,time.Now().Format("15:04:05.000"))
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	DPrintf("[Query Start]:Receive args:%v | %s\n",args,time.Now().Format("15:04:05.000"))
	// Your code here.
	op := Op{
		OpType: QUERY,
		Num: args.Num,
		ClientId: args.ClientId,
		CommandId: args.CommandId,
	}
	commandReply := sc.Command(op)
	reply.Err = commandReply.Err
	reply.WrongLeader = commandReply.WrongLeader
	reply.Config = commandReply.Config
	DPrintf("[Query Return]:Receive args:%v and Return reply:%v | %s\n",args,reply,time.Now().Format("15:04:05.000"))
}

func (sc *ShardCtrler) getWaitCh(index int) chan CommandReply {
	ch,ok := sc.waitChMap[index]
	if !ok{
		ch = make(chan CommandReply,1)
		sc.waitChMap[index] = ch
		DPrintf("[Create Waitch]:create ch[%d]\n",index)
	}
	return ch
}

func (sc *ShardCtrler) clearWaitCh(index int)  {
	sc.mu.Lock()
	DPrintf("[Clear Waitch]:clear ch[%d]\n",index)
	defer sc.mu.Unlock()
	delete(sc.waitChMap,index)
}

func (sc *ShardCtrler) hasCache(clientId int64,commandId int64) bool {
	item,ok := sc.cacheMap[clientId]
	if ok{
		return item>=commandId
	}else{
		return false
	}
}

func (sc *ShardCtrler) Command(op Op) CommandReply{
	sc.mu.Lock()
	// replicate?
	commandReply := CommandReply{}
	if op.OpType!=QUERY && sc.hasCache(op.ClientId,op.CommandId){
		commandReply.Err = OK
		commandReply.WrongLeader = false
		sc.mu.Unlock()
		return commandReply
	}
	sc.mu.Unlock()
	index, _, isLeader:=sc.rf.Start(op)
	if !isLeader{
		commandReply.WrongLeader = true
		return commandReply
	}
	DPrintf("[Command]:Receive Commmand %v\n",op)
	// wait for applyCh
	sc.mu.Lock()
	ch:=sc.getWaitCh(index)
	DPrintf("[Debug]: %v[%d]\n",ch,index)
	sc.mu.Unlock()
	select {
	case res := <-ch:
		commandReply.Err,  commandReply.WrongLeader, commandReply.Config = res.Err,res.WrongLeader,res.Config
		go sc.clearWaitCh(index)
		return commandReply
	}
}

func (sc *ShardCtrler) applier(){
	//goroutine to receive comand from raft and apply to state machine
	for {
		select {
		case msg:=<-sc.applyCh:
			if msg.CommandValid{
				// apply to state machine
				sc.mu.Lock()
				// outdated command
				if msg.CommandIndex<=sc.lastApplied{
					sc.mu.Unlock()
					continue
				}

				sc.lastApplied = msg.CommandIndex
				command:=msg.Command.(Op)
				DPrintf("[ApplyMsg]:Receive Commmand %v\n",command)
				var commandReply CommandReply
				if command.OpType!=QUERY && sc.hasCache(command.ClientId,command.CommandId){
					commandReply.Err = OK
					commandReply.WrongLeader = false
				}else{
					commandReply = sc.applyToStateMachine(&command)
					if command.OpType != QUERY{
						sc.cacheMap[command.ClientId]=command.CommandId
					}
				}
				// if leader
				currentTerm,isLeader:=sc.rf.GetState()
				if isLeader && currentTerm == msg.CommandTerm {
					DPrintf("[ApplyMsg findCh]:Receive Commmand %v index:[%d]\n",command,msg.CommandIndex)
					ch:= sc.getWaitCh(msg.CommandIndex)

					DPrintf("[ApplyMsg Ch]:Receive Commmand %v index:[%d]\n",command,msg.CommandIndex)
					ch<-commandReply

				}
				sc.mu.Unlock()
			}

		}

	}
}

func (sc *ShardCtrler) applyToStateMachine(op *Op) CommandReply{
	reply := CommandReply{}
	reply.WrongLeader = false
	reply.Err = OK
	if op.OpType == JOIN{
		curCfg := sc.getLastestConfig()
		newGroups := copyMap(curCfg.Groups)
		// add to newGroups
		for gid,servers := range op.Servers{
			newGroups[gid] = servers
		}
		// move to balance
		newShards := balance(&newGroups,&curCfg.Shards)
		newCfg := Config{
			Shards: newShards,
			Groups: newGroups,
			Num: len(sc.configs),
		}
		sc.configs = append(sc.configs,newCfg)
	}else if op.OpType == LEAVE{
		//need balance
		curCfg := sc.getLastestConfig()
		newGroups := copyMap(curCfg.Groups)
		// delete from groups
		for _,gid := range op.GIDs{
			delete(newGroups,gid)
		}
		newShards := balance(&newGroups,&curCfg.Shards)
		newCfg := Config{
			Shards: newShards,
			Groups: newGroups,
			Num: len(sc.configs),
		}
		sc.configs = append(sc.configs,newCfg)

	}else if op.OpType == MOVE{
		gid := op.GID
		shard := op.Shard

		curCfg := sc.getLastestConfig()

		shardMap := curCfg.Shards
		// move shard to new gid
		shardMap[shard] = gid
		newGroups := copyMap(curCfg.Groups)
		newCfg := Config{
			Shards: shardMap,
			Groups: newGroups,
			Num: len(sc.configs),
		}
		sc.configs = append(sc.configs,newCfg)

	}else if op.OpType == QUERY{
		idx := op.Num
		if idx<0 || idx >= len(sc.configs){
			reply.Config = sc.getLastestConfig()
			reply.WrongLeader = false
		}else{
			reply.Config = sc.configs[idx]
			reply.WrongLeader = false
		}
	}
	return reply
}

func balance(groups *map[int][]string,shards *[NShards]int) [NShards]int{
	// must be definite
	groupNum := len(*groups)

	reShards := [NShards]int{}
	if groupNum == 0{
		return reShards
	}
	// so every gid should be responsible for [balanceNum,balanceNum+1] shards
	balanceNum := NShards/groupNum

	copy(reShards[:],shards[:])
	newGids := make([]int,0)
	gidToNShards := make(map[int]int)
	for gid,_ := range *groups{
		newGids = append(newGids,gid)
		gidToNShards[gid] = 0
	}

	// sort new gids
	sort.Slice(newGids, func(i, j int) bool {
		return newGids[i] < newGids[j]
	})

	// obtain the number of shards that GID is responsible for
	for _,gid :=range shards{
		if _,ok := gidToNShards[gid];ok{
			gidToNShards[gid]+=1
		}
	}

	//move shard
	for i,gid := range shards{
		n,ok := gidToNShards[gid]
		// leave
		if n>balanceNum || !ok{
			for _,ngid := range newGids{
				if gid==ngid{
					continue
				}
				if (gidToNShards[ngid] < balanceNum && (ok || NShards%groupNum==0)) || (gidToNShards[ngid] <= balanceNum && !ok && NShards%groupNum!=0) {
					reShards[i]=ngid
					gidToNShards[ngid]++
					if ok{
						gidToNShards[gid]--
					}
					break
				}
			}
		}
	}
	return reShards
}

func copyMap(originalMap map[int][]string) map[int][]string {
	copiedMap := make(map[int][]string)
	// 遍历原始 map，并复制键值对到新的 map 中
	for key, value := range originalMap {
		// 创建一个新的切片来存储复制后的值
		copiedValue := make([]string, len(value))
		copy(copiedValue, value)
		// 复制键值对到新的 map 中
		copiedMap[key] = copiedValue
	}
	return copiedMap
}

func (sc *ShardCtrler) getLastestConfig() Config{
	return sc.configs[len(sc.configs)-1]
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

///
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.cacheMap = make(map[int64]int64)
	sc.waitChMap = make(map[int]chan CommandReply)

	go sc.applier()
	return sc
}
