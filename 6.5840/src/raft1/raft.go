package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	Leader Role = iota
	Follower
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// added
	currentTerm   int
	votedFor      int
	commitIndex   int
	lastApplied   int
	role          Role
	lastHeartbeat time.Time
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}


func (rf *Raft) roleName() string {
	switch rf.role {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}

func (rf *Raft) persist() {}

func (rf *Raft) readPersist(data []byte) {}



func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {}



type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
	Success bool
	Term    int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	reply.Term = rf.currentTerm

	upToDate := args.LastLogTerm > rf.lastApplied ||
		(args.LastLogTerm == rf.lastApplied && args.LastLogIndex >= rf.commitIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.role = Follower
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.role = Follower
	rf.lastHeartbeat = time.Now()
	reply.Success = true
	reply.Term = rf.currentTerm
}


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}


func (rf *Raft) ticker() {
	for !rf.killed() {
		timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.lastHeartbeat) >= timeout {
			rf.mu.Unlock()
			rf.startElection(timeout)
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection(timeout time.Duration) {
	rf.mu.Lock()
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	termStarted := rf.currentTerm
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()

	voteCh := make(chan bool, len(rf.peers)-1)
	termCh := make(chan int, len(rf.peers)-1)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: rf.commitIndex,
				LastLogTerm:  rf.lastApplied,
			}
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				if reply.Term > termStarted {
					termCh <- reply.Term
				} else if reply.VoteGranted {
					voteCh <- true
				}
			}
		}(i)
	}

	votes := 1
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	totalPeers := len(rf.peers)

	for {
		select {
		case <-voteCh:
			votes++
			if votes > totalPeers/2 {
				rf.mu.Lock()
				if rf.currentTerm == termStarted && rf.role == Candidate {
					rf.role = Leader
					rf.lastHeartbeat = time.Now()
					fmt.Printf("Node %d became Leader in term %d\n", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					go rf.sendHeartbeats()
				} else {
					rf.mu.Unlock()
				}
				return
			}
		case newTerm := <-termCh:
			rf.mu.Lock()
			if newTerm > rf.currentTerm {
				rf.currentTerm = newTerm
				rf.votedFor = -1
				rf.role = Follower
			}
			rf.mu.Unlock()
			return
		case <-timer.C:
			return
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				args := AppendEntriesArgs{Term: term}
				var reply AppendEntriesReply
				rf.sendAppendEntries(server, &args, &reply)
			}(i)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		commitIndex:   0,
		lastApplied:   0,
		currentTerm:   0,
		votedFor:      -1,
		role:          Follower,
		lastHeartbeat: time.Now(),
	}

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}


