package kvraft

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	for {
		server := ck.leader
		args := &rpc.GetArgs{Key: key}
		reply := &rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", args, reply)
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
		}
		ck.leader += 1
		ck.leader %= len(ck.servers)

	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	retried := false
	for {
		server := ck.leader
		args := &rpc.PutArgs{Key: key, Value: value, Version: version}
		reply := &rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", args, reply)
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

}
