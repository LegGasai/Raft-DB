package kvraft

import (
	"6.824/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// the possible leader id
	leaderId 	int
	nServers 	int
	clientId	int64
	commandId 	int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.nServers = len(servers)
	ck.clientId = nrand()
	ck.commandId = 0
	return ck
}

func (ck *Clerk) sendCommand(key string,value string,opType OpType) string {
	// add commanId
	atomic.AddInt64(&ck.commandId,1)

	var args = CommandArgs{
		Key: key,
		Value: value,
		Type: opType,
		ClientId: ck.clientId,
		CommandId: ck.commandId,
	}

	// keep send rpc to servers
	for{
		var reply = CommandReply{}
		//DPrintf("[Client Request][sendCommand]: Client[%d] received a request:[%v] to Server[%d]| %s\n",ck.clientId,args,ck.leaderId,time.Now().Format("15:04:05.000"))
		ok := ck.servers[ck.leaderId].Call("KVServer.Command", &args, &reply)
		if ok{
			if reply.Err == OK{
				return reply.Value
			}else if reply.Err == ErrNoKey{
				return ""
			}else{
				// try another,leaderId = (leaderId+1)%nServers
				ck.leaderId = (ck.leaderId+1)%ck.nServers
				time.Sleep(10*time.Millisecond)
				continue
			}
		}else{
			ck.leaderId = (ck.leaderId+1)%ck.nServers
			continue
		}
	}
	return ""
}


//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.sendCommand(key,"",GET)
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sendCommand(key,value,OpType(op))
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
