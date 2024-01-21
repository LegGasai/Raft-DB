package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout	   = "ErrTimeout"
	ErrNotReady	   = "ErrNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId 	int64
	CommandId 	int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId 	int64
	CommandId 	int64
}

type GetReply struct {
	Err   Err
	Value string
}


type ShardMigrationArgs struct {
	Shard 		int
	ConfigNum	int
}

type ShardMigrationReply struct {
	Err			Err
	Shard 		int
	ConfigNum	int
	CacheMap	map[int64]int64
	Data 		map[string]string
}