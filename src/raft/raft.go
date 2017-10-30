package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	STATE_LEADER = iota
	STATE_CANDIDATE
	STATE_FOLLOWER
	HBINTERVAL   = 50 * time.Millisecond
	WAITINTERVAL = 150
)

const debugEnabled = false

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	chanGrantVote chan bool
	chanHeartBeat chan bool
	chanLeader    chan bool
	chanCommit chan bool

	state int

	currentTerm int
	votedFor    int
	numVote     int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == STATE_LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term {
		reply.Term = args.Term
		reply.Success = false
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		return
	}
	reply.Term = args.Term

	rf.chanHeartBeat <- true

	/*if args.Entries == nil {
		reply.Success = true
	} else */
	if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		if args.Entries != nil {
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			debug("ZYZ- Follower %v: append %v result %v.\n", rf.me, args.Entries, rf.log)
		}

		if rf.commitIndex < len(rf.log) - 1 && rf.commitIndex < args.LeaderCommit {
			if args.LeaderCommit > len(rf.log)-1 {
				rf.commitIndex = len(rf.log) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.chanCommit <- true
			debug("ZYZ- Follower %d commitIndex %d\n", rf.me, rf.commitIndex)
		}

		reply.Success = true
	} else {
		reply.Success = false
	}

	/*if args.PrevLogIndex < len(rf.log) && reply.Success && args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log) - 1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
		debug("ZYZ- Follower %d commitIndex %d\n", rf.me, rf.commitIndex)
	} */
}

func (rf *Raft) sendAppendEntries(server int) {
	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex

	// TODO may need lock
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

	if rf.nextIndex[server] >= len(rf.log) {
		args.Entries = nil
	} else {
		args.Entries = rf.log[rf.nextIndex[server]:]
		//debug("ZYZ- Leader %d send %v to peer %v\n", rf.me, args, server)
	}
	reply := new(AppendEntriesReply)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = STATE_FOLLOWER
			return
		}

		if rf.state != STATE_LEADER {
			return
		}

		if reply.Success {
			if args.Entries != nil {
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] -= 1
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = args.Term

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if rf.votedFor != -1 {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			reply.Term = args.Term
			rf.state = STATE_FOLLOWER
			rf.votedFor = -1
		}
		return
	}


	rf.currentTerm = args.Term

	myLastTerm := rf.log[len(rf.log) - 1].Term

	if args.LastLogTerm < myLastTerm {
		return
	}

	if args.LastLogTerm > myLastTerm || args.LastLogIndex >= len(rf.log) - 1 {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		return
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if reply.VoteGranted  {

	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != STATE_CANDIDATE {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.votedFor = -1
			return ok
		}

		if reply.VoteGranted {
			rf.numVote++
			if rf.numVote == (len(rf.peers)/2+1) && rf.state == STATE_CANDIDATE {
				rf.chanLeader <- true
			}
		}
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{command, term})
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.mu.Unlock()
		debug("ZYZ- Leader %d update log %v\n", rf.me, rf.log)
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) broadRequestVote() {
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func(peer int) {
				reply := new(RequestVoteReply)
				rf.sendRequestVote(peer, args, reply)
			}(i)
		}
	}
}

func (rf *Raft) broadAppendEntry() {
	i := rf.commitIndex + 1
	for {
		num := 0
		for j := range rf.peers {
			if rf.matchIndex[j] >= i {
				num += 1
			}
		}
		if num > len(rf.peers)/2 {
			rf.commitIndex = i
			debug("ZYZ- Leader %v Update commitIndex to %v\n", rf.me, rf.commitIndex)
			rf.chanCommit <- true
			i += 1
		} else {
			break
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				rf.sendAppendEntries(peer)
			}(i)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = STATE_FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = 0

	rf.chanGrantVote = make(chan bool, 100)
	rf.chanHeartBeat = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 1)
	rf.chanCommit = make(chan bool, 100)

	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0
	rf.log[0].Command = -1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	go func() {
		for {
			switch rf.state {
			case STATE_FOLLOWER:
				select {
				case <-rf.chanHeartBeat:
				case <-rf.chanGrantVote:
				case <-time.After(time.Duration(rand.Int()%(WAITINTERVAL * len(rf.peers)) + WAITINTERVAL) * time.Millisecond):
					//debug("ZYZ- Peer %d become Candidate. Term %v\n", rf.me, rf.currentTerm+1)
					rf.state = STATE_CANDIDATE
				}

			case STATE_CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.numVote = 1
				rf.mu.Unlock()

				go rf.broadRequestVote()
				select {
				case <-rf.chanHeartBeat:
					rf.state = STATE_FOLLOWER
					rf.votedFor = -1

				case <-rf.chanLeader:
					debug("\nZYZ- Peer %d become Leader. Term %v\n", rf.me, rf.currentTerm)
					rf.state = STATE_LEADER
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}

				case <-time.After(time.Duration(WAITINTERVAL) * time.Millisecond):
					//debug("ZYZ- Peer %d, not enough vote, Term +1 to %v\n", rf.me, rf.currentTerm+1)
				}

			case STATE_LEADER:
				rf.broadAppendEntry()
				time.Sleep(HBINTERVAL)
			}
		}
	}()

	go func() {
		for {
			select {
			case <- rf.chanCommit:
				commitIndex := rf.commitIndex
				for i := rf.lastApplied; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
					applyCh <- msg
					rf.lastApplied = i
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
