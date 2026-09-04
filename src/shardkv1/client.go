package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		cfg := ck.sck.Query()

		shard := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(shard)
		if !ok {
			continue
		}
		groupClerk, exist := ck.rcks[gid]
		if !exist {
			groupClerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.rcks[gid] = groupClerk
		}
		value, version, err := groupClerk.Get(key)
		switch err {
		case rpc.OK:
			return value, version, rpc.OK

		case rpc.ErrNoKey:
			return "", 0, rpc.ErrNoKey

		case rpc.ErrWrongGroup:
			continue
		case rpc.ErrWrongLeader:
			continue

		}

	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	rerouted := false

	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(shard)
		if !ok {
			continue
		}

		groupClerk, exist := ck.rcks[gid]
		if !exist {
			groupClerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.rcks[gid] = groupClerk
		}

		err := groupClerk.Put(key, value, version)

		switch err {
		case rpc.ErrWrongGroup:
			rerouted = true
			continue
		case rpc.ErrWrongLeader:
			rerouted = true
			continue

		case rpc.ErrVersion:
			if rerouted {
				return rpc.ErrMaybe
			}
			return rpc.ErrVersion

		default:
			return err
		}
	}
}
