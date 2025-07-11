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
type LogEntry struct {
	Term    int
	Command interface{}
}

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

	log        []LogEntry
	applyCh    chan raftapi.ApplyMsg
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}
	entry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	index := len(rf.log) - 1
	term := rf.currentTerm
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	go rf.broadcastAppendEntries()
	return index, term, true
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
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

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.role = Follower
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.role = Follower
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.lastHeartbeat = time.Now()

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term != entry.Term {
				rf.log = rf.log[:index]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyEntries()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	start := rf.lastApplied + 1
	end := rf.commitIndex
	if start > end || end >= len(rf.log) {
		rf.mu.Unlock()
		return
	}
	entries := make([]LogEntry, end-start+1)
	copy(entries, rf.log[start:end+1])
	rf.lastApplied = end
	rf.mu.Unlock()

	for i, entry := range entries {
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: start + i,
		}
		rf.applyCh <- msg
	}
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
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
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
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
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
					rf.nextIndex = make([]int, totalPeers)
					rf.matchIndex = make([]int, totalPeers)
					lastIndex := len(rf.log)
					for i := range rf.peers {
						rf.nextIndex[i] = lastIndex
						rf.matchIndex[i] = 0
					}
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
		rf.broadcastAppendEntries()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return
	}
	term := rf.currentTerm
	leaderCommit := rf.commitIndex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		prevIndex := rf.nextIndex[i] - 1
		prevTerm := 0
		if prevIndex >= 0 && prevIndex < len(rf.log) {
			prevTerm = rf.log[prevIndex].Term
		}
		entries := make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
		copy(entries, rf.log[rf.nextIndex[i]:])
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		go func(server int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					return
				}
				if reply.Success {
					match := args.PrevLogIndex + len(args.Entries)
					rf.matchIndex[server] = match
					rf.nextIndex[server] = match + 1

					for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
						count := 1
						for j := range rf.peers {
							if j != rf.me && rf.matchIndex[j] >= N && rf.log[N].Term == rf.currentTerm {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = N
							go rf.applyEntries()
							break
						}
					}
				} else {
					rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
				}
			}
		}(i, args)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
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
		log:           []LogEntry{{Term: 0}}, // dummy entry at index 0
		applyCh:       applyCh,
	}

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}


