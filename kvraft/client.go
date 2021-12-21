package kvraft

import "6.824-golabs-2020/src/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID int64
	seqID int64
	leaderID int
	mu sync.Mutex
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
	ck.clientID=nrand()
	return ck
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
	args :=GetArgs{
	    Key: key,
	    ClinetID: ck.clientID,
	    SeqID: atomic.AddInt64(&ck.seqID,1)
	}
	DPrint("Get: clientID:%d	Key:%s",ck.clientID,key)
	
	ck.mu.Lock()
	leaderID :=ck.leaderID
	ck.mu.Unlock()
	
	for{
	    reply:=GetReply{}
	    if ck.servers[leaderID].Call("KVServer.Get",&args,&reply){
	        if reply.Err==OK{
	            return reply.Value
	        }
	        else if reply.Err==ErrNoKey{
	            return "";
	        }
	    }
	    
	    ck.mu.Lock()
	    ck.leaderID=(ck.LeaderID + 1) % len(ck.servers)
	    leaderID =ck.leaderID
	    ck.mu.Unlock()
	    
	    time.Sleep(1*time.Millisecond)
	}
	return ""
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
	args:=PutAppendArgs{
	    Key : key ,
	    Value : value ,
	    Op: op ,
	    ClientID : ck.clientID ,
	    SeqID : atomic.AddInt64(&ck.seqID,1),
	}
	
	DPrint("PutAppend: clientID:%d	Key:%s	Value:%s",ck.clientID,key,value)
	
	ck.mu.Lock()
	leaderID :=ck.leaderID
	ck.mu.Unlock()
	
	for{
	    reply := PutAppendReply{}
	    if ck.servers[LeaderID].Call("KVServer.PutAppend", &args, &reply){
	       if reply.Err == OK{
	           break
	       }
	    }
	    
	    ck.mu.Lock()
	    ck.leaderID=(ck.LeaderID + 1) % len(ck.servers)
	    leaderID =ck.leaderID
	    ck.mu.Unlock()
	    
	    time.Sleep(1*time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
