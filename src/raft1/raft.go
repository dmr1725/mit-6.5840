package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type ServerState int 

const (
	// this assigns follower=0, leader=1, candidate=2
	follower ServerState = iota
	leader 
	candidate
)

type Entry struct {
	Command 	interface{}
	Term		int
}


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        			sync.Mutex          	// Lock to protect shared access to this peer's state
	peers     			[]*labrpc.ClientEnd 	// RPC end points of all peers
	persister 			*tester.Persister   	// Object to hold this peer's persisted state
	me        			int                 	// this peer's index into peers[]
	dead      			int32               	// set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// MY IMPLEMENTATION
	currentTerm 		int						// latest term server has seen
	votedFor    		int						// candidateId that received vote in current term
	log 				[]Entry					// log entries; entry contains command for state machine and term
	commitIndex			int						// index of highest log entry known to be committed
	lastApplied			int						// index of highest log entry applied to state machine
	nextIndex 			[]int					// for each server, index of the next log entry to send to that server
	matchIndex			[]int					// for each server, index of highest log entry known to be replicated on server
	serverState			ServerState
	lastHeartbeat		time.Time				// last time we got a successful heartbeat from a leader
	electionTimeout		time.Duration			// duration that determines whether an election should start. If time since our lastHearbeat was more than electionTimeout, start election
	applyCh				chan raftapi.ApplyMsg
	applyCond			*sync.Cond				// condition variable
	lastIncludedIndex 	int						// highest log index (last entry) in the snapshot
	lastIncludedTerm  	int						// term of last included index
}




// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.serverState == leader
	return term, isleader
}


// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()

	// for normal persist() calls (not during Snapshot), you should persist current snapshot, not nil.
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// This is when you want to persist a new snapshot - invoked by Snapshot() only
func (rf *Raft) persistSnapshot(snapshot []byte) {
	// Your code here (3C).
	
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int 
	var votedFor int 
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&rf.lastIncludedIndex) != nil || d.Decode(&rf.lastIncludedTerm) != nil {
	  return
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if we've already snapshotted up to (or beyond) this index, this request is stale
	if index <= rf.lastIncludedIndex {
		return 
	}

	// now all info up to and including index is trimmed
	// Creates a new slice and copy the entries we want to keep. This breaks the reference to the old array.  
	rf.lastIncludedTerm = rf.log[rf.logicalToPhysical(index)].Term
	startIndex := rf.logicalToPhysical(index)
	newLog := make([]Entry, len(rf.log[startIndex:]))
	copy(newLog, rf.log[startIndex:])
	rf.log = newLog

	rf.lastIncludedIndex = index

	rf.persistSnapshot(snapshot)
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term			int // candidate's term
	CandidateId 	int	// candidate requesting vote
	LastLogIndex	int	// index of candidate's last log entry
	LastLogTerm		int	// term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term			int  // currentTerm, for candidate to update itself
	VoteGranted		bool // true means candidate received vote
}

// AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term			int // leader's term
	LeaderId     	int	// so follower can redirect clients
	PrevLogIndex	int	// index of log entry immediately preceding new ones
	PrevLogTerm		int	// term of prevLogIndex entry
	Entries			[]Entry // log entries to store (empty for heartbeat; may send more than one efficiency)
	LeaderCommit	int // leader's commitIndex
}

// AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term			int  // currentTerm, for leader to update itself
	Success		    bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm		    int  // the term of the conflicting entry at PrevLogIndex (or -1 if the follower's log is too short)
	XIndex			int  // the first index where XTerm appears in the follower's log
	XLen			int  // the length of the follower's log (useful when the log is too short)
}

/////////////////////////////////////////////////////////////
// Translation formula when dealing with snapshots - assumes caller holds lock for rf.mu
// return physical index from logicalIndex
func (rf *Raft) logicalToPhysical(logicalIndex int) int {
	return logicalIndex - rf.lastIncludedIndex
}

// return local index from physicalIndex
func (rf *Raft) physicalToLogical(physicalIndex int) int {
	return physicalIndex + rf.lastIncludedIndex
}
////////////////////////////////////////////////////////////

// return true if candidate's log is at least up to date - section 5.4.1 of paper last paragraph
func (rf *Raft) isCandidateLogUpToDate(candidateLastLogTerm int, candidateLastLogIndex int) bool {
	var receiverLastLogTerm int
	var receiverLastLogIndex int
	if rf.physicalToLogical(len(rf.log)) > 1 {
		receiverLastLogTerm = rf.log[len(rf.log) - 1].Term
		receiverLastLogIndex = rf.physicalToLogical(len(rf.log) - 1)
	} else {
		receiverLastLogTerm = 0
		receiverLastLogIndex = -1
	}

	candidateUpToDate := false
	if candidateLastLogTerm > receiverLastLogTerm {
		candidateUpToDate = true
	} else if candidateLastLogTerm == receiverLastLogTerm {
		if candidateLastLogIndex >= receiverLastLogIndex {
			candidateUpToDate = true
		}
	}

	return candidateUpToDate
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	candidateTerm := args.Term
	candidateId := args.CandidateId

	if candidateTerm > rf.currentTerm {
		rf.currentTerm = candidateTerm
		rf.votedFor = -1
		rf.serverState = follower
		rf.persist()
	}

	if candidateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm
	if ((rf.votedFor == -1 || rf.votedFor == candidateId) && rf.isCandidateLogUpToDate(candidateLastLogTerm, candidateLastLogIndex)) {
		rf.votedFor = candidateId
		rf.lastHeartbeat = time.Now() // reset election timer
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

}

// example AppendEntries RPC handler - receiver of append entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if leader term is less than my term (I'm a follower), reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// update term if leader's term is higher (part of rules for all servers Figure 2 - raft paper)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.serverState = follower
		rf.persist()
	}

	// if you're receiving append entries, it is from a valid leader. 
	// Maybe you were a candidate in an election, need to make sure you're a follower because there is a valid leader
	if rf.serverState != follower {
		rf.serverState = follower
	}

	rf.lastHeartbeat = time.Now()

	// follower's log is too short (doesn't have an entry at PrevLogIndex)
	if rf.physicalToLogical(len(rf.log)) <= args.PrevLogIndex {
		reply.XTerm = -1 
		reply.XLen = rf.physicalToLogical(len(rf.log))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if receiver logs does not contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.log[rf.logicalToPhysical(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.logicalToPhysical(args.PrevLogIndex)].Term
		reply.XLen = rf.physicalToLogical(len(rf.log))
		index := 0

		// find the first index where XTerm appears
		for index < len(rf.log) && rf.log[index].Term != reply.XTerm {
			index += 1
		}
		reply.XIndex = rf.physicalToLogical(index)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if an existing entry (rf.log) conflicts with a new one (args.Entries) (same index, but different terms), 
	// delete the existing entry and all that follow it - section 5.3
	for index := range args.Entries {
		targetIndex := rf.logicalToPhysical(args.PrevLogIndex + 1 + index)

		/*

		Example: 
			leader entries [{command: nil, term: 0}, {command: A, term: 1}, {command: B, term: 3}, {command: C, term: 3}]
			prevLogIndex 1

			targetIndex = 2
			rf.log         [{command: nil, term: 0}, {command: A, term: 1}, {command: C, term: 2}]
			args.Entries.  [{command: B, term: 3}, {command: C, term: 3}]
		*/
		// means that I already have an entry at targetIndex with different term in args.Entries
		if targetIndex < len(rf.log) && rf.log[targetIndex].Term != args.Entries[index].Term {
			rf.log = rf.log[:targetIndex] // truncating log
			rf.log = append(rf.log, args.Entries[index:]...) // append entries to rf.log starting from index

			// update commitIndex after appending
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, rf.physicalToLogical(len(rf.log) - 1))
				rf.applyCond.Signal() // signal when commit index advances
			}

			reply.Term = rf.currentTerm
			reply.Success = true
			rf.persist()
			return
		}
	}

	// append entries, but don't append duplicates
	/*

		Example: 
			leader entries [{command: nil, term: 0}, {command: A, term: 1}, {command: B, term: 3}, {command: C, term: 3}]
			prevLogIndex 1

			targetIndex = 2
			rf.log         [{command: nil, term: 0}, {command: A, term: 1}, {command: B, term: 3}]
			args.Entries.  [{command: B, term: 3}, {command: C, term: 3}]
		*/
	if len(args.Entries) > 0 {
		firstNewIndex := rf.physicalToLogical(len(rf.log)) - (args.PrevLogIndex + 1)
		if firstNewIndex < len(args.Entries){
			rf.log = append(rf.log, args.Entries[firstNewIndex:]...)
			rf.persist()
		}
	}

	// update commitIndex after appending
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.physicalToLogical(len(rf.log) - 1))
		rf.applyCond.Signal() // signal when commit index advances
	}

	reply.Term = rf.currentTerm
	reply.Success = true

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) appendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// starts the replication process!!!
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if is leader, save command to your log in memory
	isLeader = rf.serverState == leader
	term = rf.currentTerm
	if isLeader {
		rf.log = append(rf.log, Entry{
			Command: command,
			Term: term,
		})
		index = rf.physicalToLogical(len(rf.log) - 1)
		rf.persist()
		return index, term, isLeader
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applyCond.Signal() // signal the condition variable so the applier can exit
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// if time since our last heartbeat > than our election timeout, we should start an election
		rf.mu.Lock()
		elapsedTime := time.Since(rf.lastHeartbeat)
		if elapsedTime > rf.electionTimeout {
			rf.serverState = candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me // vote for itself
			rf.electionTimeout = time.Duration(400+rand.Int63()%400) * time.Millisecond  // reset election timeout (range between 400 and 800 milliseconds)
			rf.lastHeartbeat = time.Now() // need to reset last heart beat because it can timeout again. Remember that ticker is running continuosly
			votesReceived := 1
			rf.persist()

			currentTerm := rf.currentTerm // THIS term election
			me := rf.me 

			var lastLogIndex int
			var lastLogTerm int
			if len(rf.log) > 0 {
				lastLogIndex = rf.physicalToLogical(len(rf.log) - 1)
				lastLogTerm = rf.log[len(rf.log) - 1].Term
			} else {
				lastLogIndex = -1
				lastLogTerm = 0
			}
			rf.mu.Unlock()
			
			// asking for votes to other peers as candidate
			for server := range rf.peers {
				if server != rf.me {
					go func(peer int) {
						// send rpc to peer
						args := &RequestVoteArgs{
							Term: currentTerm,
							CandidateId: me,
							LastLogIndex: lastLogIndex,
							LastLogTerm: lastLogTerm,
						}
						reply := &RequestVoteReply{}
						ok := rf.sendRequestVote(peer, args, reply)
						// handle reply
						if ok {
							rf.mu.Lock()
							lastLogIndex := rf.physicalToLogical(len(rf.log) - 1) // reassing lastLogIndex after releasing lock above and locking again (value may have changed)

							// check that server is still candidate in the same term that the election started
							if rf.serverState == candidate && currentTerm == rf.currentTerm {
								if reply.Term > currentTerm {
									// step down
									rf.currentTerm = reply.Term
									rf.serverState = follower
									rf.persist()
								} else if reply.VoteGranted {
									votesReceived++

									quorumMajority := (len(rf.peers) / 2) + 1
									// won election
									if votesReceived >= quorumMajority {
										rf.serverState = leader
										// initialize nextIndex[] and matchIndex[]
										for server := range rf.peers {
											rf.nextIndex[server] = lastLogIndex + 1
											rf.matchIndex[server] = 0
										}
									}
								}
							}
							rf.mu.Unlock()
							return
						}
					}(server)
				}
			}
		} else {
			// need to unlock, if not a deadlock could happen
			rf.mu.Unlock()
		}


		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeats() {
	for rf.killed() == false {
		_, isleader := rf.GetState()
		if isleader {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			me := rf.me 
			commitIndex := rf.commitIndex
			rf.mu.Unlock()

			// send heartbeats
			for server := range rf.peers {
				if server != rf.me {
					go func(peer int){
						var entries []Entry

						/*
						determine what entries to send to peer

						If the peer server is not caught up with me (leader), send the missing entries
						If the peer server is caught up with me (leader), send empty entry (heartbeat)
						*/
						rf.mu.Lock()
						peerNextIndex := rf.nextIndex[peer] // index of the next log entry to send to that follower server
						
						// leader can't send AppendEntries because entries are gone
						/*
						Example: Leader snapshotted at index 10 -> so lastIncludedIndex = 10 and log would start at index 10
						Follower is behind, nextIndex[peer] = 5
						// Leader needs to send entries 5, 6, 7, 8, 9... but they're gone
						*/
						if peerNextIndex <= rf.lastIncludedIndex {
							// TODO: send InstallSnapshot
							return
						}
						if peerNextIndex < rf.physicalToLogical(len(rf.log)) {
							// Make a copy to avoid sharing backing array with rf.log
							entries = make([]Entry, rf.physicalToLogical(len(rf.log))-peerNextIndex)
							copy(entries, rf.log[rf.logicalToPhysical(peerNextIndex):]) // send missing logs (starting from peerNextIndex)
						} else{ // caught up - send empty entry (heartbeat)
							entries = []Entry{}
						}
						prevLogIndex := rf.nextIndex[peer] - 1
						prevLogTerm := rf.log[rf.logicalToPhysical(prevLogIndex)].Term
						rf.mu.Unlock()
						args := &AppendEntriesArgs{
							Term: currentTerm,
							LeaderId: me,
							LeaderCommit: commitIndex,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm: prevLogTerm,
							Entries: entries,
						}
						reply := &AppendEntriesReply{}
						ok := rf.appendEntries(server, args, reply)
						if ok {
							rf.mu.Lock()
							if reply.Term > currentTerm {
								rf.currentTerm = reply.Term
								rf.serverState = follower
								rf.votedFor = -1
								rf.persist()

								rf.mu.Unlock()
								return
							}

							if rf.currentTerm != currentTerm || rf.serverState != leader {
								rf.mu.Unlock()
								return
							}

							if reply.Success == true {
								rf.nextIndex[peer] = prevLogIndex + 1 + len(args.Entries)
								rf.matchIndex[peer] = prevLogIndex + len(args.Entries)

								// what's the highest index (N) in my log that a majority of servers have replicated? Once we find N, we can update commitIndex of leader
								// If there exists an N such that N>commitIndex, a majority of matchIndex[i]≥N, and log[N].term==currentTerm, setcommitIndex=N - Figure 2 Rules for servers leader section
								for N := rf.physicalToLogical(len(rf.log) - 1); N > rf.commitIndex; N-- {

									count := 1 // number of servers that have replicated up to N (start with 1 - count yourself the leader)

									for peer := range rf.peers {
										if peer != rf.me && rf.matchIndex[peer] >= N {
											count++
										}
									}
									
										// Leaders can only commit entries from their current term by counting replicas. This prevents committing old entries from previous terms that might be overwritten.
									if rf.log[rf.logicalToPhysical(N)].Term == rf.currentTerm && count >= (len(rf.peers) / 2) + 1 {
										rf.commitIndex = N
										rf.applyCond.Signal() // signal when commit index advances
										break
									}
								}
							}
							
							// follower rejected probably because of a conflicting entry
							if reply.Success == false {
								if reply.XTerm == -1 {
									rf.nextIndex[peer] = reply.XLen
								} else {
									// check if XTerm exists in leader's log
									index := len(rf.log) -1 
									for index >= 0 && rf.log[index].Term != reply.XTerm {
										index--
									}

									// XTerm exists in leader's log, skip to end of that term in leader's log
									if index >= 0 {
										rf.nextIndex[peer] = rf.physicalToLogical(index + 1)
									}

									// XTerm does not exists in leader's log, skip entire conflicting term
									if index < 0 {
										rf.nextIndex[peer] = reply.XIndex
									}
								}
							}
							rf.mu.Unlock()
						}
					}(server)
				}
			}
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}


func (rf *Raft) applyEntries() {
	for rf.killed() == false {
		rf.mu.Lock()
		
		/*
		 We want to apply entries to state machine when commitIndex > lastApplied.

		If the loop condition is true (commitIndex <= lastApplied), we call Wait() 
		because there's nothing to apply. Wait() releases the lock, then sleeps.

		When another goroutine calls rf.applyCond.Signal(), this goroutine wakes up,
		reacquires the lock, and re-checks the condition. If commitIndex > lastApplied 
		now, we exit the loop and apply entries.

		Wait() can release/reacquire rf.mu because we initialized applyCond with 
		sync.NewCond(&rf.mu), linking the condition variable to our mutex.
		*/
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait() // sleeps, releases lock, re-acquires when woken by a signal
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// copy entries to apply (from lastApplied + 1 to commitIndex)
		entries := rf.log[rf.logicalToPhysical(rf.lastApplied + 1): rf.logicalToPhysical(rf.commitIndex + 1)]
		startIndex := rf.lastApplied + 1
		rf.lastApplied = rf.commitIndex

		rf.mu.Unlock()

		for index, entry := range entries {
			// send
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: startIndex + index,
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.electionTimeout = time.Duration(400+rand.Int63()%400) * time.Millisecond 
	rf.lastHeartbeat = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = []Entry{
		{Command: nil, Term: 0},
	}
	rf.commitIndex = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// if a snapshot was present, set commitIndex and lastApplied to lastIncludedIndex, why?
	// Because everything up to lastIncludedIndex is already "applied" via the snapshot. The service layer restore its state from the 
	// snapshot separately. So Raft should not try to re-apply those entries
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex


	// start ticker goroutine to start elections
	go rf.ticker()

	// start sendHeartBeats goroutine to send heartbeats
	go rf.sendHeartBeats()

	// apply entries to state machine after committing
	go rf.applyEntries()

	return rf
}
