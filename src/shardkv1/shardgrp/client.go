package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {

	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for i := 0; i < len(ck.servers); i++ {
		server := ck.leader
		args := &rpc.GetArgs{Key: key}
		reply := &rpc.GetReply{}
		ok := ck.Call(ck.servers[server], "KVServer.Get", args, reply)
		if ok == false {
			ck.leader += 1
			ck.leader %= len(ck.servers)
			continue
		} else {
			if reply.Err == rpc.ErrWrongLeader {
				ck.leader += 1
				ck.leader %= len(ck.servers)
				continue
			}
			if reply.Err == rpc.OK {
				return reply.Value, reply.Version, rpc.OK
			}
			if reply.Err == rpc.ErrNoKey {
				return "", 0, rpc.ErrNoKey
			}
			if reply.Err == rpc.ErrWrongGroup {
				return "", 0, rpc.ErrWrongGroup
			}
		}
		ck.leader += 1
		ck.leader %= len(ck.servers)

	}
	return "", 0, rpc.ErrWrongLeader
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	retried := false
	for i := 0; i < len(ck.servers); i++ {
		server := ck.leader
		args := &rpc.PutArgs{Key: key, Value: value, Version: version}
		reply := &rpc.PutReply{}
		ok := ck.Call(ck.servers[server], "KVServer.Put", args, reply)
		if ok == false {
			ck.leader += 1
			ck.leader %= len(ck.servers)
			retried = true
			continue
		} else {
			if reply.Err == rpc.ErrWrongLeader {
				ck.leader += 1
				ck.leader %= len(ck.servers)
				retried = true
				continue
			} else if reply.Err == rpc.ErrVersion && retried == true {
				return rpc.ErrMaybe
			} else {
				return reply.Err
			}
		}
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := &shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	for {
		server := ck.leader
		reply := &shardrpc.FreezeShardReply{}

		ok := ck.Call(
			ck.servers[server],
			"KVServer.FreezeShard",
			args,
			reply,
		)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		return reply.State, reply.Err
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := &shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}

	for {
		server := ck.leader
		reply := &shardrpc.InstallShardReply{}

		ok := ck.Call(
			ck.servers[server],
			"KVServer.InstallShard",
			args,
			reply,
		)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		return reply.Err
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := &shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}

	for {
		server := ck.leader
		reply := &shardrpc.DeleteShardReply{}

		ok := ck.Call(
			ck.servers[server],
			"KVServer.DeleteShard",
			args,
			reply,
		)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		return reply.Err
	}
}
