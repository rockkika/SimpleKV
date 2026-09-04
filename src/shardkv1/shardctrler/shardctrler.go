package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const currentConfigKey = "current-config"
const nextConfigKey = "next-config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	return sck
}
func (sck *ShardCtrler) initConfigKey(key string, cfg *shardcfg.ShardConfig) {
	wanted := cfg.String()

	for {
		err := sck.Put(key, wanted, 0)
		if err == rpc.OK {
			return
		}

		if err == rpc.ErrMaybe || err == rpc.ErrVersion {
			actual, _, getErr := sck.Get(key)
			if getErr == rpc.OK {
				if actual == wanted {
					return
				}
				panic("shardctrler: conflicting initial configuration")
			}

			if getErr == rpc.ErrNoKey {
				continue
			}
		}
	}
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	currentValue, currentVersion, currentErr := sck.Get(currentConfigKey)
	if currentErr != rpc.OK {
		return
	}

	nextValue, _, nextErr := sck.Get(nextConfigKey)
	if nextErr != rpc.OK {
		return
	}

	current := shardcfg.FromString(currentValue)
	next := shardcfg.FromString(nextValue)
	if next.Num != current.Num+1 {
		return
	}

	sck.completeConfigChange(current, next, currentVersion)
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.initConfigKey(currentConfigKey, cfg)
	sck.initConfigKey(nextConfigKey, cfg)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	value, version, err := sck.Get(currentConfigKey)
	if err != rpc.OK {
		return
	}

	old := shardcfg.FromString(value)

	if old.Num >= new.Num {
		return
	}

	if new.Num != old.Num+1 {
		return
	}

	nextValue, nextVersion, nextErr := sck.Get(nextConfigKey)
	if nextErr != rpc.OK {
		return
	}

	recordedNext := shardcfg.FromString(nextValue)
	if recordedNext.Num > old.Num {
		if recordedNext.Num != old.Num+1 {
			return
		}
		sck.completeConfigChange(old, recordedNext, version)
		return
	}
	if recordedNext.Num != old.Num {
		return
	}

	if sck.Put(nextConfigKey, new.String(), nextVersion) != rpc.OK {
		return
	}
	sck.completeConfigChange(old, new, version)

}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	value, _, err := sck.Get(currentConfigKey)
	if err != rpc.OK {
		panic("shardctrler: current configuration does not exist")
	}
	return shardcfg.FromString(value)
}

func (sck *ShardCtrler) completeConfigChange(old *shardcfg.ShardConfig,
	new *shardcfg.ShardConfig, version rpc.Tversion) {

	type shardMove struct {
		shard shardcfg.Tshid
		from  tester.Tgid
		to    tester.Tgid
		state []byte
	}

	moves := make([]shardMove, 0)

	for i := 0; i < shardcfg.NShards; i++ {
		shard := shardcfg.Tshid(i)
		oldGid := old.Shards[shard]
		newGid := new.Shards[shard]

		if oldGid == newGid {
			continue
		}

		moves = append(moves, shardMove{
			shard: shard,
			from:  oldGid,
			to:    newGid,
			state: make([]byte, 0),
		})
	}
	for i := range moves {
		move := &moves[i]

		servers, ok := old.Groups[move.from]
		if !ok {
			return
		}

		ck := shardgrp.MakeClerk(sck.clnt, servers)

		state, freezeErr := ck.FreezeShard(move.shard, new.Num)
		if freezeErr != rpc.OK {
			return
		}
		move.state = state
	}
	for i := range moves {
		move := &moves[i]
		servers, ok := new.Groups[move.to]
		if !ok {
			return
		}

		ck := shardgrp.MakeClerk(sck.clnt, servers)
		installErr := ck.InstallShard(
			move.shard,
			move.state,
			new.Num,
		)
		if installErr != rpc.OK {
			return
		}
	}

	for i := range moves {
		move := &moves[i]

		servers, ok := old.Groups[move.from]
		if !ok {
			return
		}

		ck := shardgrp.MakeClerk(sck.clnt, servers)

		deleteErr := ck.DeleteShard(move.shard, new.Num)
		if deleteErr != rpc.OK {
			return
		}
	}

	wanted := new.String()

	publishErr := sck.Put(currentConfigKey, wanted, version)
	if publishErr == rpc.ErrMaybe {
		actual, _, getErr := sck.Get(currentConfigKey)
		if getErr != rpc.OK || actual != wanted {
			return
		}
	} else if publishErr != rpc.OK {
		return
	}
}
