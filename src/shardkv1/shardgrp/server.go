package shardgrp

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
	"bytes"
	"fmt"
)

const (
	ENVKEY = "65840ENV"
)

type ShardStatus int

const (
	ShardAbsent ShardStatus = iota
	ShardServing
	ShardFrozen
)

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type Key string

type KVServer struct {
	me   int
	rsm  *rsm.RSM
	gid  tester.Tgid
	data map[Key]Entry

	statuses [shardcfg.NShards]ShardStatus
	lastNum  [shardcfg.NShards]shardcfg.Tnum
}

type SnapshotState struct {
	Data     map[Key]Entry
	Statuses [shardcfg.NShards]ShardStatus
	LastNum  [shardcfg.NShards]shardcfg.Tnum
}

type ShardData struct {
	Entries map[Key]Entry
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		shard := shardcfg.Key2Shard(args.Key)

		if kv.statuses[shard] != ShardServing {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}

		v, exist := kv.data[Key(args.Key)]
		if exist {
			return rpc.GetReply{Value: v.Value, Version: v.Version, Err: rpc.OK}
		} else {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}

	case rpc.PutArgs:
		shard := shardcfg.Key2Shard(args.Key)

		if kv.statuses[shard] != ShardServing {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
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

	case shardrpc.FreezeShardArgs:
		shard := args.Shard
		seenNum := kv.lastNum[shard]

		if args.Num < seenNum {
			return shardrpc.FreezeShardReply{
				Num: seenNum,
				Err: rpc.ErrVersion,
			}
		}

		if args.Num == seenNum &&
			kv.statuses[shard] == ShardFrozen {
			return shardrpc.FreezeShardReply{
				State: kv.encodeShard(shard),
				Num:   seenNum,
				Err:   rpc.OK,
			}
		}
		if args.Num == seenNum &&
			kv.statuses[shard] == ShardAbsent {
			return shardrpc.FreezeShardReply{
				State: nil,
				Num:   seenNum,
				Err:   rpc.OK,
			}
		}

		if args.Num == seenNum {
			return shardrpc.FreezeShardReply{
				Num: seenNum,
				Err: rpc.ErrVersion,
			}
		}

		if kv.statuses[shard] != ShardServing {
			return shardrpc.FreezeShardReply{
				Num: seenNum,
				Err: rpc.ErrWrongGroup,
			}
		}

		kv.statuses[shard] = ShardFrozen
		kv.lastNum[shard] = args.Num

		return shardrpc.FreezeShardReply{
			State: kv.encodeShard(shard),
			Num:   args.Num,
			Err:   rpc.OK,
		}
	case shardrpc.InstallShardArgs:
		shard := args.Shard
		seenNum := kv.lastNum[shard]
		if args.Num < seenNum {
			return shardrpc.InstallShardReply{
				Err: rpc.ErrVersion,
			}
		}
		if args.Num == seenNum &&
			kv.statuses[shard] == ShardServing {
			return shardrpc.InstallShardReply{
				Err: rpc.OK,
			}
		}
		if args.Num > seenNum &&
			kv.statuses[shard] == ShardAbsent {
			entries := decodeShard(args.State)

			for key := range kv.data {
				if shardcfg.Key2Shard(string(key)) == shard {
					delete(kv.data, key)
				}
			}

			for key, entry := range entries {
				kv.data[key] = entry
			}

			kv.statuses[shard] = ShardServing
			kv.lastNum[shard] = args.Num

			return shardrpc.InstallShardReply{Err: rpc.OK}
		}
		return shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
	case shardrpc.DeleteShardArgs:
		shard := args.Shard
		seenNum := kv.lastNum[shard]
		if args.Num != seenNum {
			return shardrpc.DeleteShardReply{
				Err: rpc.ErrVersion,
			}
		}
		if kv.statuses[shard] == ShardAbsent {
			return shardrpc.DeleteShardReply{
				Err: rpc.OK,
			}
		}
		if kv.statuses[shard] != ShardFrozen {
			return shardrpc.DeleteShardReply{
				Err: rpc.ErrWrongGroup,
			}
		}
		for key := range kv.data {
			if shardcfg.Key2Shard(string(key)) == shard {
				delete(kv.data, key)
			}
		}
		kv.statuses[shard] = ShardAbsent
		return shardrpc.DeleteShardReply{
			Err: rpc.OK,
		}
	default:
		panic(fmt.Sprintf("shardgrp: unexpected request type %T", req))
	}
}

func (kv *KVServer) Snapshot() []byte {
	state := SnapshotState{
		Data:     kv.data,
		Statuses: kv.statuses,
		LastNum:  kv.lastNum,
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(state); err != nil {
		panic(fmt.Sprintf("kvshard: encode snapshot: %v", err))
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var restored SnapshotState
	if err := d.Decode(&restored); err != nil {
		panic(fmt.Sprintf("kvshard: decode snapshot: %v", err))
	}
	kv.statuses = restored.Statuses
	kv.lastNum = restored.LastNum
	kv.data = restored.Data
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.PutReply)
}

func (kv *KVServer) encodeShard(shard shardcfg.Tshid) []byte {
	state := ShardData{
		Entries: make(map[Key]Entry),
	}

	for key, entry := range kv.data {
		if shardcfg.Key2Shard(string(key)) == shard {
			state.Entries[key] = entry
		}
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(state); err != nil {
		panic(fmt.Sprintf(
			"shardgrp: encode shard %d: %v",
			shard,
			err,
		))
	}
	return w.Bytes()
}

func decodeShard(data []byte) map[Key]Entry {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var state ShardData
	if err := d.Decode(&state); err != nil {
		panic(fmt.Sprintf("shardgrp: decode shard: %v", err))
	}

	return state.Entries
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	*reply = result.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	*reply = result.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	*reply = result.(shardrpc.DeleteShardReply)
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:  gid,
		me:   me,
		data: make(map[Key]Entry),
	}

	if gid == shardcfg.Gid1 {
		for shard := 0; shard < shardcfg.NShards; shard++ {
			kv.statuses[shard] = ShardServing
			kv.lastNum[shard] = shardcfg.NumFirst
		}
	}

	kv.rsm = rsm.MakeRSM(
		servers,
		me,
		persister,
		maxraftstate,
		kv,
	)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
