package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"

	"math/rand"
)

var useRaftStateMachine bool // to plug in another raft besided raft1


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req any
}

type OpResult struct {
	Id 	   int64 
	Result any
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	// lookup table: "if anyone is waiting for this index, here's how to wake them up."
	pending		map[int]chan OpResult // key is an int (index), value is a channel that can send/receive values of type OpResult
	dead		bool                 // true after applyCh is closed (Raft killed); new Submit() calls must not block
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:	  make(map[int]chan OpResult),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.RaftChannelReader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	op := Op{Me: rsm.me, Id: rand.Int63(), Req: req}
	ch := make(chan OpResult, 1)


	index, term, isLeader := rsm.rf.Start(op)

	if isLeader {
		rsm.mu.Lock()
		if rsm.dead {
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
		rsm.pending[index] = ch
		rsm.mu.Unlock()

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop() // stop ticker when Submit() exits

		// pattern in Go to wait on multiple channels simultaneously and looping until one of them fires
		for {
			// select sits idle until one of two things happens:
			// 1. We got an event from RaftChannelReader (sends a result or channel closes)
			// 2. We got an event from ticker channel which fires every 100ms
			select {
			case result, ok := <-ch: // block channel (code pauses here until someone sends value to channel)
				if ok && result.Id == op.Id {
						return rpc.OK, result.Result
				} else {
					return rpc.ErrWrongLeader, nil
				}
			// Go Ticker object has a field C chan time.Time. Every 100ms, Go's runtime automatically sends the current time on that channel
			// so every 100ms we checked if leader/term matches when we called rf.Start(). If it doesn't match, it means that leader changed and op may not have committed
			// hence, return ErrWrongLeader as a conservative signal to the client to retry
			case <-ticker.C:
				currentTerm, _ := rsm.rf.GetState()
				if term != currentTerm {
						rsm.mu.Lock()
						delete(rsm.pending, index)
						rsm.mu.Unlock()
						return rpc.ErrWrongLeader, nil
					}
				}
			}
	}
	
	// your code here
	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}

// Raft tells the reader what was committed via applyCh, then the reader uses pending map to find the waiting Submit() channel for that index
func (rsm *RSM) RaftChannelReader() {

	// range on a channel blocks waiting for the next message, loops forever, and exits the loop automatically when the channel is closed — which is exactly when Raft is killed.
	// applyCh only closes in rf.applyEntries() (when it exits)
	for msg := range rsm.applyCh {
		commandValid := msg.CommandValid
		if commandValid {
			commandIndex := msg.CommandIndex
			op := msg.Command.(Op)

			result := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			resultCh, ok := rsm.pending[commandIndex]
			rsm.mu.Unlock()
			if ok {
				resultCh <- OpResult{Id: op.Id, Result: result} // wake up line 129
				rsm.mu.Lock()
				delete(rsm.pending, commandIndex)
				rsm.mu.Unlock()
			}
		}
	}

	// When applyCh closes (Raft killed), the above loop exits. Two independent problems to handle:
	//
	// 1. Submit() calls already blocking on <-ch: the reader was the one sending results;
	//    now that it's exiting, close all pending channels so those goroutines wake up and return ErrWrongLeader.
	//
	// 2. Submit() calls that haven't registered yet but are about to: Kill() doesn't change
	//    serverState, so Start() can still return isLeader=true during shutdown. A new Submit()
	//    could register pending[N]=ch after cleanup, and nobody would ever close it — hangs forever because RaftChannelReader already exited.
	//    Setting dead=true (under the same lock as the close loop) prevents new registrations.
	rsm.mu.Lock()
	rsm.dead = true
	for _, ch := range rsm.pending {
		close(ch)
	}
	rsm.mu.Unlock()
}


