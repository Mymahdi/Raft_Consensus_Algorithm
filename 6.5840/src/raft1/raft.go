package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

// LogEntry struct represents a single entry in the Raft log.
type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft struct implements a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers (updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1, but we use 0-based with a dummy entry)

	// Volatile state on all servers
	commitIndex   int       // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied   int       // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	role          Role      // current role of the server (Leader, Follower, Candidate)
	lastHeartbeat time.Time // time of last heartbeat received from current leader or last grant vote to candidate

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh chan raftapi.ApplyMsg // channel to send committed log entries to the service (or tester)
}

// GetState returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// persist saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persisted.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// lastApplied is volatile state, not persisted according to Raft paper.
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil) // Snapshot is nil for now, will be used in 3D
	DPrintf("Node %d (Term %d, Role %s) persisted state. Log length: %d", rf.me, rf.currentTerm, rf.roleName(), len(rf.log))
}

// readPersist restores Raft's persistent state from stable storage.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("Node %d failed to read persisted state", rf.me)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	// lastApplied is volatile and should be initialized to 0 on restart.
	// The test harness handles re-applying commands from the beginning of the log.
	rf.lastApplied = 0 // Always initialize to 0 on restart
	// commitIndex remains 0, will be updated by leader's AppendEntries
	DPrintf("Node %d (Term %d, Role %s) read persisted state. Log length: %d, Initial lastApplied: %d", rf.me, rf.currentTerm, rf.roleName(), len(rf.log), rf.lastApplied)
}

// Start initiates the process of adding a new command to the replicated log.
// The service using Raft (e.g. a k/v server) wants to start agreement on the next command to be appended to Raft's log.
// If this server is not the leader, returns false. Otherwise, start the agreement and return immediately.
// The first return value is the index that the command will appear at.
// The second return value is the current term.
// The third return value is true if this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}

	entry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	index := len(rf.log) - 1 // 0-based index
	term := rf.currentTerm
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist() // Persist after appending to log
	DPrintf("Leader %d (Term %d) received command: %v at index %d", rf.me, rf.currentTerm, command, index)

	go rf.broadcastAppendEntries()

	return index, term, true
}

// Kill is called by the tester to stop this Raft peer.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("Node %d killed", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// roleName returns the string representation of the current role.
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

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Not implemented in 3C, will be in 3D
}

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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("Node %d (Term %d, Role %s) received RequestVote from Candidate %d (Term %d, LastLogIndex %d, LastLogTerm %d)", rf.me, rf.currentTerm, rf.roleName(), args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Node %d (Term %d) denied vote to Candidate %d (Term %d): old term", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // Reset votedFor if term changes
		rf.role = Follower
		DPrintf("Node %d (Term %d) updated term to %d from Candidate %d. Converted to Follower.", rf.me, rf.currentTerm, args.Term, args.CandidateId)
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now() // Reset election timer if vote granted
		reply.VoteGranted = true
		DPrintf("Node %d (Term %d) granted vote to Candidate %d (Term %d). My log: (%d, %d), Candidate log: (%d, %d)", rf.me, rf.currentTerm, args.CandidateId, args.Term, lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogIndex)
	} else {
		reply.VoteGranted = false
		DPrintf("Node %d (Term %d) denied vote to Candidate %d (Term %d). VotedFor: %d, UpToDate: %t. My log: (%d, %d), Candidate log: (%d, %d)", rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.votedFor, upToDate, lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogIndex)
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // Persist state after potential changes

	DPrintf("Node %d (Term %d, Role %s) received AppendEntries from Leader %d (Term %d, PrevLogIndex %d, PrevLogTerm %d, LeaderCommit %d, Entries: %d)", rf.me, rf.currentTerm, rf.roleName(), args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = -1 // Indicate no specific conflict
		reply.XIndex = -1
		reply.XLen = len(rf.log)
		DPrintf("Node %d (Term %d) rejected AppendEntries from Leader %d (Term %d): old term", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // Reset votedFor if term changes
		rf.role = Follower
		DPrintf("Node %d (Term %d) converted to Follower by Leader %d (Term %d)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	}

	rf.lastHeartbeat = time.Now() // Reset election timer on receiving valid AppendEntries

	if args.PrevLogIndex >= len(rf.log) {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XLen = len(rf.log) // Follower's log is too short
		reply.XTerm = -1
		reply.XIndex = -1
		DPrintf("Node %d (Term %d) AppendEntries from Leader %d (Term %d) failed: PrevLogIndex %d too large. My log length: %d. Setting XLen to %d", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, len(rf.log), reply.XLen)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term

		firstIndexWithXTerm := 0
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Term == reply.XTerm {
				firstIndexWithXTerm = i
				break
			}
		}
		reply.XIndex = firstIndexWithXTerm
		reply.XLen = len(rf.log)
		DPrintf("Node %d (Term %d) AppendEntries from Leader %d (Term %d) failed: PrevLogTerm mismatch at index %d. My term: %d, Leader's term: %d. Setting XTerm %d, XIndex %d, XLen %d", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term != entry.Term {
				rf.log = rf.log[:index] // Delete conflicting entry and all that follow
				rf.log = append(rf.log, entry)
				DPrintf("Node %d (Term %d) truncated log at index %d due to conflict. Appended entry %d (Term %d)", rf.me, rf.currentTerm, index, index, entry.Term)
			}
		} else {
			rf.log = append(rf.log, entry) // Append new entry
			DPrintf("Node %d (Term %d) appended entry %d (Term %d)", rf.me, rf.currentTerm, index, entry.Term)
		}
	}

	if rf.commitIndex >= len(rf.log) {
		rf.commitIndex = len(rf.log) - 1
		if rf.commitIndex < 0 {
			rf.commitIndex = 0
		}
		DPrintf("Node %d (Term %d) truncated log, adjusted commitIndex to %d", rf.me, rf.currentTerm, rf.commitIndex)
	}
	if rf.lastApplied >= len(rf.log) {
		rf.lastApplied = len(rf.log) - 1
		if rf.lastApplied < 0 {
			rf.lastApplied = 0
		}
		DPrintf("Node %d (Term %d) truncated log, adjusted lastApplied to %d", rf.me, rf.currentTerm, rf.lastApplied)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		DPrintf("Node %d (Term %d) updated commitIndex to %d. LeaderCommit: %d, My last log index: %d", rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit, len(rf.log)-1)
		go rf.applyEntries() // Apply committed entries
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	DPrintf("Node %d (Term %d) AppendEntries from Leader %d (Term %d) successful. New log length: %d", rf.me, rf.currentTerm, args.LeaderId, args.Term, len(rf.log))
}

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	DPrintf("Node %d (Term %d) applyEntries started. lastApplied: %d, commitIndex: %d, log length: %d", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, len(rf.log))

	for rf.lastApplied < rf.commitIndex {
		nextIndexToApply := rf.lastApplied + 1

		if nextIndexToApply >= len(rf.log) {
			DPrintf("Node %d (Term %d) applyEntries: nextIndexToApply %d out of bounds for log length %d. Breaking loop. This indicates a potential log inconsistency or commitIndex issue.", rf.me, rf.currentTerm, nextIndexToApply, len(rf.log))
			break
		}

		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[nextIndexToApply].Command,
			CommandIndex: nextIndexToApply,
		}

		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()

		rf.lastApplied = nextIndexToApply

		DPrintf("Node %d (Term %d) applied entry %d (Command: %v)", rf.me, rf.currentTerm, msg.CommandIndex, msg.Command)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Randomize election timeout to prevent split votes
		electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
		heartbeatInterval := 100 * time.Millisecond // Leaders send heartbeats periodically

		rf.mu.Lock()
		currentRole := rf.role
		lastHeartbeatTime := rf.lastHeartbeat
		rf.mu.Unlock()

		switch currentRole {
		case Follower, Candidate:
			if time.Since(lastHeartbeatTime) >= electionTimeout {
				DPrintf("Node %d (Term %d, Role %s) election timeout. Starting election.", rf.me, rf.currentTerm, rf.roleName())
				go rf.startElection(electionTimeout)
			}
		case Leader:
			// Leaders send heartbeats periodically
			time.Sleep(heartbeatInterval)
			go rf.broadcastAppendEntries()
		}

		// Sleep for a short duration to avoid busy-waiting
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
	rf.persist()
	DPrintf("Node %d (Term %d) started election. Voted for self. LastLog: (%d, %d)", rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)
	rf.mu.Unlock()

	votesReceived := 1 // Vote for self
	voteCh := make(chan bool, len(rf.peers))
	termCh := make(chan int, len(rf.peers))

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
			DPrintf("Node %d (Term %d) sending RequestVote to %d", rf.me, termStarted, server)
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					DPrintf("Node %d (Term %d) received higher term %d from %d. Converting to Follower.", rf.me, rf.currentTerm, reply.Term, server)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.role = Follower
					rf.persist() // Persist state after term change
					termCh <- reply.Term
					return
				}
				if rf.role == Candidate && rf.currentTerm == termStarted {
					if reply.VoteGranted {
						DPrintf("Node %d (Term %d) received vote from %d", rf.me, termStarted, server)
						voteCh <- true
					} else {
						DPrintf("Node %d (Term %d) denied vote from %d", rf.me, termStarted, server)
					}
				}
			} else {
				DPrintf("Node %d (Term %d) failed to send RequestVote to %d", rf.me, termStarted, server)
			}
		}(i)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	totalPeers := len(rf.peers)

	for {
		select {
		case <-voteCh:
			votesReceived++
			if votesReceived > totalPeers/2 {
				rf.mu.Lock()
				if rf.currentTerm == termStarted && rf.role == Candidate {
					rf.role = Leader
					rf.lastHeartbeat = time.Now()
					rf.nextIndex = make([]int, totalPeers)
					rf.matchIndex = make([]int, totalPeers)
					lastLogIndex := len(rf.log) - 1
					for i := range rf.peers {
						rf.nextIndex[i] = lastLogIndex + 1
						rf.matchIndex[i] = 0
					}
					rf.matchIndex[rf.me] = lastLogIndex
					DPrintf("Node %d became Leader in Term %d. Initializing nextIndex to %v, matchIndex to %v", rf.me, rf.currentTerm, rf.nextIndex, rf.matchIndex)
					rf.mu.Unlock()
					go rf.broadcastAppendEntries() // Send initial heartbeats
				} else {
					rf.mu.Unlock()
				}
				return // Election won or role changed
			}
		case newTerm := <-termCh:

			rf.mu.Lock()
			if newTerm > rf.currentTerm {
				rf.currentTerm = newTerm
				rf.votedFor = -1
				rf.role = Follower
				rf.persist() // Persist state after term change
			}
			rf.mu.Unlock()
			return // Election lost
		case <-timer.C:
			DPrintf("Node %d (Term %d) election timed out. Retrying election.", rf.me, termStarted)
			return // Election timed out, start a new one
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()

		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := 0

		if prevLogIndex < 0 {
			prevLogIndex = 0
			prevLogTerm = rf.log[0].Term
		} else if prevLogIndex >= len(rf.log) {

			DPrintf("Leader %d (Term %d) prevLogIndex %d out of bounds for server %d. My log length: %d. This might indicate a need for snapshot (3D).", rf.me, rf.currentTerm, prevLogIndex, i, len(rf.log))

		} else {
			prevLogTerm = rf.log[prevLogIndex].Term
		}

		entries := []LogEntry{}
		if rf.nextIndex[i] < len(rf.log) {
			entries = make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
			copy(entries, rf.log[rf.nextIndex[i]:])
		}
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		rf.mu.Unlock()

		go func(server int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			DPrintf("Leader %d (Term %d) sending AppendEntries to %d. PrevLogIndex: %d, PrevLogTerm: %d, Entries: %d", rf.me, args.Term, server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					DPrintf("Leader %d (Term %d) received higher term %d from %d. Converting to Follower.", rf.me, rf.currentTerm, reply.Term, server)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.role = Follower
					rf.persist() // Persist state after term change
					return
				}

				if rf.role != Leader || args.Term != rf.currentTerm {
					return
				}

				if reply.Success {

					newMatchIndex := args.PrevLogIndex + len(args.Entries)
					newNextIndex := newMatchIndex + 1
					if newMatchIndex > rf.matchIndex[server] { // Only update if it's an improvement
						rf.matchIndex[server] = newMatchIndex
						rf.nextIndex[server] = newNextIndex
						DPrintf("Leader %d (Term %d) AppendEntries to %d successful. matchIndex[%d] = %d, nextIndex[%d] = %d", rf.me, rf.currentTerm, server, server, rf.matchIndex[server], server, rf.nextIndex[server])
					}

					for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
						if rf.log[N].Term != rf.currentTerm {
							continue
						}
						count := 1
						for j := range rf.peers {
							if j != rf.me && rf.matchIndex[j] >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = N
							DPrintf("Leader %d (Term %d) updated commitIndex to %d. Majority reached for index %d", rf.me, rf.currentTerm, rf.commitIndex, N)
							go rf.applyEntries()
							break
						}
					}
				} else {
					DPrintf("Leader %d (Term %d) AppendEntries to server %d failed. Reply: %+v", rf.me, rf.currentTerm, server, reply)

					if reply.XTerm != -1 {
						leaderLastTermIndex := -1
						for i := len(rf.log) - 1; i >= 0; i-- {
							if rf.log[i].Term == reply.XTerm {
								leaderLastTermIndex = i
								break
							}
						}
						if leaderLastTermIndex != -1 {
							rf.nextIndex[server] = leaderLastTermIndex + 1
							DPrintf("Leader %d (Term %d) adjusted nextIndex[%d] to %d (Leader has XTerm)", rf.me, rf.currentTerm, server, rf.nextIndex[server])
						} else {
							if reply.XIndex != -1 && reply.XIndex < len(rf.log) {
								rf.nextIndex[server] = reply.XIndex
								DPrintf("Leader %d (Term %d) adjusted nextIndex[%d] to %d (Leader doesn't have XTerm, using XIndex)", rf.me, rf.currentTerm, server, rf.nextIndex[server])
							} else {
								rf.nextIndex[server] = reply.XLen
								DPrintf("Leader %d (Term %d) adjusted nextIndex[%d] to %d (Leader doesn't have XTerm, XIndex invalid, using XLen)", rf.me, rf.currentTerm, server, rf.nextIndex[server])
							}
						}
					} else {
						rf.nextIndex[server] = reply.XLen
						DPrintf("Leader %d (Term %d) adjusted nextIndex[%d] to %d (Follower too short, using XLen)", rf.me, rf.currentTerm, server, rf.nextIndex[server])
					}

					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
						DPrintf("Leader %d (Term %d) adjusted nextIndex[%d] to %d (Ensuring min 1)", rf.me, rf.currentTerm, server, rf.nextIndex[server])
					}
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
		commitIndex:   0, // Initialized to 0
		lastApplied:   0, // Initialized to 0, will be set from readPersist or log length
		currentTerm:   0,
		votedFor:      -1, // -1 indicates no vote
		role:          Follower,
		lastHeartbeat: time.Now(),
		log:           []LogEntry{{Term: 0}}, // Dummy entry at index 0
		applyCh:       applyCh,
	}

	// Read persistent state from persister.
	rf.readPersist(persister.ReadRaftState())

	if rf.lastApplied > 0 {
		DPrintf("Node %d sending initial SnapshotValid ApplyMsg for index %d (Term %d) to tester. (Synchronous send)", rf.me, rf.lastApplied, rf.log[rf.lastApplied].Term)
		initialApplyMsg := raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      nil,
			SnapshotTerm:  rf.log[rf.lastApplied].Term,
			SnapshotIndex: rf.lastApplied,
		}
		applyCh <- initialApplyMsg
	}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	rf.persist()

	// Start ticker goroutine to start elections
	go rf.ticker()

	DPrintf("Node %d (Term %d, Role %s) initialized. Log length: %d, LastApplied: %d", rf.me, rf.currentTerm, rf.roleName(), len(rf.log), rf.lastApplied)

	return rf
}
