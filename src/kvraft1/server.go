package kvraft

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
	"bytes"
	"fmt"
)

type Entry struct {
	Value   string
	Version rpc.Tversion
}
type Key string
type KVServer struct {
	me  int
	rsm *rsm.RSM

	data map[Key]Entry
	// Your definitions here.
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		v, exist := kv.data[Key(args.Key)]
		if exist {
			return rpc.GetReply{Value: v.Value, Version: v.Version, Err: rpc.OK}
		} else {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
	case rpc.PutArgs:
		v, exist := kv.data[Key(args.Key)]
		if exist {
			if args.Version != v.Version {
				return rpc.PutReply{
					Err: rpc.ErrVersion,
				}
			} else {
				kv.data[Key(args.Key)] = Entry{
					Value:   args.Value,
					Version: args.Version + 1,
				}
				return rpc.PutReply{Err: rpc.OK}
			}
		} else {
			if args.Version == 0 {
				kv.data[Key(args.Key)] = Entry{
					Value:   args.Value,
					Version: 1,
				}
				return rpc.PutReply{Err: rpc.OK}
			} else {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
		}

	default:
		panic(fmt.Sprintf("kvraft: unexpected request type %T", req))
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.data); err != nil {
		panic(fmt.Sprintf("kvraft: encode snapshot: %v", err))
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var restored map[Key]Entry
	if err := d.Decode(&restored); err != nil {
		panic(fmt.Sprintf("kvraft: decode snapshot: %v", err))
	}
	kv.data = restored
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, result := kv.rsm.Submit(*args)
	if err == rpc.OK {
		*reply = result.(rpc.GetReply)
	} else if err == rpc.ErrWrongLeader {
		*reply = rpc.GetReply{Err: rpc.ErrWrongLeader}
	}

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, result := kv.rsm.Submit(*args)
	if err == rpc.OK {
		*reply = result.(rpc.PutReply)
	} else if err == rpc.ErrWrongLeader {
		*reply = rpc.PutReply{Err: rpc.ErrWrongLeader}
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, data: make(map[Key]Entry)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
