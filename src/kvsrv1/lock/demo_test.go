package lock

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

// workerForDemo acquires the shared lock once, touches demoKey while
// holding it, and logs every transition so you can see the
// serialization enforced by the lock.
func workerForDemo(name string, lk *Lock, ck kvtest.IKVClerk, demoKey string, wg *sync.WaitGroup) {
	defer wg.Done()
	lk.Acquire()
	fmt.Printf("%s acquired lock\n", name)

	val, ver, err := ck.Get(demoKey)
	if err == rpc.ErrNoKey {
		// first writer initializes key
		ver = 0
		val = ""
	} else if err != rpc.OK {
		fmt.Printf("%s unexpected Get error: %v\n", name, err)
		return
	}

	fmt.Printf("%s saw value %q at version %d\n", name, val, ver)
	newVal := fmt.Sprintf("%s was here", name)
	if putErr := ck.Put(demoKey, newVal, ver); putErr != rpc.OK {
		fmt.Printf("%s Put error: %v\n", name, putErr)
	} else {
		fmt.Printf("%s updated value to %q (next version %d)\n", name, newVal, ver+1)
	}

	time.Sleep(30 * time.Millisecond) // emphasize that only one runs inside CS
	lk.Release()
	fmt.Printf("%s released lock\n", name)
}

// workerWithoutLock performs a read-modify-write on demoKey without
// taking a lock so that two goroutines can collide and produce an
// ErrVersion.
func workerWithoutLock(name string, ck kvtest.IKVClerk, demoKey string, wg *sync.WaitGroup, results chan string) {
	defer wg.Done()

	val, ver, err := ck.Get(demoKey)
	if err == rpc.ErrNoKey {
		ver = 0
		val = ""
	} else if err != rpc.OK {
		results <- fmt.Sprintf("%s Get error %v", name, err)
		return
	}

	fmt.Printf("%s read (%q, v%d)\n", name, val, ver)

	// force overlap between the two workers to amplify the race
	time.Sleep(20 * time.Millisecond)

	newVal := fmt.Sprintf("%s update", name)
	if putErr := ck.Put(demoKey, newVal, ver); putErr != rpc.OK {
		results <- fmt.Sprintf("%s Put failed with %v (stale version)", name, putErr)
	} else {
		results <- fmt.Sprintf("%s Put succeeded; next version %d", name, ver+1)
	}
}

func TestLockDemo(t *testing.T) {
	ts := kvsrv.MakeTestKV(t, true)
	defer ts.Cleanup()

	// create two independent clerks sharing the same KV server
	ckA := ts.MakeClerk()
	defer ts.DeleteClerk(ckA)
	ckB := ts.MakeClerk()
	defer ts.DeleteClerk(ckB)

	const lockKey = "demo-lock"
	const demoKey = "demo-shared-value"

	lkA := MakeLock(ckA, lockKey)
	lkB := MakeLock(ckB, lockKey)

	fmt.Println("Lock demo starting…")
	var wg sync.WaitGroup
	wg.Add(2)
	go workerForDemo("worker A", lkA, ckA, demoKey, &wg)
	go workerForDemo("worker B", lkB, ckB, demoKey, &wg)
	wg.Wait()

	finalVal, finalVer, err := ckA.Get(demoKey)
	if err != rpc.OK {
		t.Fatalf("final get failed: %v", err)
	}
	fmt.Printf("Final value in %q: %q at version %d\n", demoKey, finalVal, finalVer)
}

func TestNoLockRaceDemo(t *testing.T) {
	ts := kvsrv.MakeTestKV(t, true)
	defer ts.Cleanup()

	ckA := ts.MakeClerk()
	defer ts.DeleteClerk(ckA)
	ckB := ts.MakeClerk()
	defer ts.DeleteClerk(ckB)

	const demoKey = "demo-shared-value-no-lock"

	// initialize key to make the race clearer
	if err := ckA.Put(demoKey, "start", 0); err != rpc.OK {
		t.Fatalf("initial put failed: %v", err)
	}

	fmt.Println("No-lock demo starting…")
	var wg sync.WaitGroup
	results := make(chan string, 2)
	wg.Add(2)
	go workerWithoutLock("worker A", ckA, demoKey, &wg, results)
	go workerWithoutLock("worker B", ckB, demoKey, &wg, results)
	wg.Wait()
	close(results)

	for msg := range results {
		fmt.Println(msg)
	}

	finalVal, finalVer, err := ckA.Get(demoKey)
	if err != rpc.OK {
		t.Fatalf("final get failed: %v", err)
	}
	fmt.Printf("Final value without lock: %q at version %d\n", finalVal, finalVer)
}
