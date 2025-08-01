package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	id string
	ck kvtest.IKVClerk
	l  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{id: kvtest.RandValue(8), ck: ck, l: l}
	err := lk.ck.Put(l, "", 0)
	// rpc.OK or rpc.ErrNoKey means the lock is initialized in KV server
	if err != rpc.OK && err == rpc.ErrNoKey {
		log.Fatalf("MakeLock: failed to initialize lock %s with Put; err=%v", l, err)
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.l)
		if err == rpc.ErrNoKey {
			log.Fatalf("Acquire: Get(%s) failed with ErrNoKey", lk.l)
		}
		switch value {
		case "":
			// lock is free, try to acquire it
			err = lk.ck.Put(lk.l, lk.id, version)
			// if Put succeeded, we acquired the lock
			// otherwise, retry to acquire again and confirm the lock is held by us
			switch err {
			case rpc.OK:
				return
			case rpc.ErrMaybe:
				continue
			}
		case lk.id:
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.l)
		if err == rpc.ErrNoKey {
			log.Fatalf("Release: Get(%s) failed with ErrNoKey", lk.l)
		}
		switch value {
		case lk.id:
			// lock is held by this client, try to release it
			err = lk.ck.Put(lk.l, "", version)
			// if Put succeeded, we released the lock
			// otherwise, retry to release again and confirm the lock is not held by us
			switch err {
			case rpc.OK:
				return
			case rpc.ErrMaybe:
				continue
			}
		case "":
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
