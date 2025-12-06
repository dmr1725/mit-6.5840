package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
	ID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		name: l, 
		ID: kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.name)

		if err != rpc.OK && err != rpc.ErrNoKey {
			// network/transient error, just retry
			continue
		}
		
		// if lock is free, try to acquire it
		if err == rpc.ErrNoKey || value == ""{
			// lock is free
			err = lk.ck.Put(lk.name, lk.ID, version)

			// successfully acquired lock
			if err == rpc.OK {
				return
			}

			if err == rpc.ErrMaybe {
				// maybe we acquired it; confirm with a Get
				value, _, _ := lk.ck.Get(lk.name)
				if value == lk.ID {
					return
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	value, version, _ := lk.ck.Get(lk.name)

	// Step 2: Only release if you are the owner
	if value == lk.ID{
		err := lk.ck.Put(lk.name, "", version)

		if err == rpc.OK {
			return
		}

		if err == rpc.ErrMaybe {
			value, _, _ = lk.ck.Get(lk.name)
			if value == "" {
				return
			}
		}
	}
}