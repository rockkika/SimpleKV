package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck         kvtest.IKVClerk
	lockname   string
	curVersion rpc.Tversion
	owner      string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	lk.lockname = lockname
	// You may add code here

	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, ok := lk.ck.Get(lk.lockname)
		if ok != rpc.OK {
			o := kvtest.RandValue(8)
			ok := lk.ck.Put(lk.lockname, o, 0)
			if ok == rpc.OK {
				lk.owner = o
				return
			} else if ok == rpc.ErrMaybe {
				value, _, newOk := lk.ck.Get(lk.lockname)
				if newOk != rpc.OK {
					time.Sleep(time.Second)
					continue
				} else {
					if value == o {
						lk.owner = o
						return
					}
				}
			}
		} else {
			if value == "idle" {
				o := kvtest.RandValue(8)
				ok := lk.ck.Put(lk.lockname, o, version)
				if ok == rpc.OK {
					lk.owner = o
					return
				} else if ok == rpc.ErrMaybe {
					value, _, newOk := lk.ck.Get(lk.lockname)
					if newOk != rpc.OK {
						time.Sleep(time.Second)
						continue
					} else {
						if value == o {
							lk.owner = o
							return
						}
					}
				}
			}
		}
		time.Sleep(time.Second)
	}

}

func (lk *Lock) Release() {
	if lk.owner == "" {
		return
	}

	for {
		value, version, err := lk.ck.Get(lk.lockname)

		if err == rpc.ErrNoKey {
			lk.owner = ""
			return
		}

		if err != rpc.OK {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if value != lk.owner {
			// 当前锁已经不属于这个 Lock 对象。
			lk.owner = ""
			return
		}

		err = lk.ck.Put(lk.lockname, "idle", version)
		if err == rpc.OK {
			lk.owner = ""
			return
		} else if err == rpc.ErrMaybe {
			value, _, newOk := lk.ck.Get(lk.lockname)
			if newOk != rpc.OK {
				time.Sleep(time.Second)
				continue
			} else if value == "idle" {
				lk.owner = ""
			}
		}

		// ErrMaybe 或 ErrVersion：重新读取状态确认。
		time.Sleep(10 * time.Millisecond)
	}
}
