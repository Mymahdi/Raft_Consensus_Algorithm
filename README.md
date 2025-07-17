# 🗂️ Raft Consensus Algorithm

## Part A: Leader Election

## Objective

The goal of Part 3A is to implement the leader election mechanism in the Raft consensus algorithm. This includes:
- Allowing a node to start an election when it detects a leader failure.
- Granting and requesting votes according to Raft rules.
- Ensuring a single leader per term through a majority vote.
- Maintaining consistent term numbers and state transitions.

---

## Key Concepts

- **Roles**: Each node can be a `Follower`, `Candidate`, or `Leader`.
- **Terms**: Logical clocks that determine election cycles.
- **Voting**: Each node can vote only once per term for a candidate.
- **Election Timeout**: Randomized timeout triggers election if no heartbeat is received.

---

## Main Components Implemented

### `Raft` Struct
```go
type Raft struct {
    mu        sync.Mutex
    peers     []*labrpc.ClientEnd
    persister *tester.Persister
    me        int
    dead      int32

    currentTerm   int
    votedFor      int
    commitIndex   int
    lastApplied   int
    role          Role
    lastHeartbeat time.Time
}
```

### Leader Election Trigger (`ticker`)
A background goroutine checks if the election timeout has passed and initiates an election if no heartbeat is received.

```go
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
```

### Starting an Election (`startElection`)
- Transition to candidate state
- Increment term
- Vote for self
- Send `RequestVote` RPCs
- Wait for majority votes or higher term reply

```go
rf.role = Candidate
rf.currentTerm++
rf.votedFor = rf.me
```

### Voting Logic (`RequestVote`)
Votes are granted if:
- The request has a higher term
- The requester’s log is at least as up-to-date
- This node has not already voted in the current term

```go
if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
    rf.votedFor = args.CandidateId
    reply.VoteGranted = true
}
```

### Heartbeats (`AppendEntries`)
Leaders periodically send empty `AppendEntries` RPCs to assert authority.

---

## Test Results

```bash
$ go test -race -run 3A
Test (3A): initial election (reliable network)... Passed
Test (3A): election after network failure... Passed
Test (3A): multiple elections (reliable network)... Passed
```

All subtests for Part 3A passed, confirming the correctness and stability of the election process.

---
---

## Part B: Log Replication

## Objective

The goal of Part 3B is to implement the core log replication mechanism of the Raft consensus algorithm. This includes:
- Appending client commands to the leader’s log.
- Replicating entries from the leader to followers via AppendEntries RPCs.
- Ensuring committed entries are applied consistently on all servers.

---

## Key Concepts

- **AppendEntries RPC**: Used by the leader to replicate log entries and send heartbeats.
- **Log Matching**: Ensures followers’ logs match the leader's up to a certain index.
- **Commit Index**: Updated when a log entry is safely replicated on a majority of servers.
- **Application to State Machine**: Committed entries are applied to the service using `applyCh`.

---

## Main Components Implemented

### AppendEntries Handler

Handles RPCs received by followers from the leader. Verifies term, consistency, and updates logs accordingly.

```go
func (rf *Raft) AppendEntries(args *raftapi.AppendEntriesArgs, reply *raftapi.AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    // Term and log consistency checks
    // Append new entries, update commitIndex and send entries to applyCh
}
```

### Log Replication by Leader

Leader sends AppendEntries to all followers periodically or when new entries are added.

```go
for peer := range rf.peers {
    if peer != rf.me {
        go rf.sendAppendEntries(peer)
    }
}
```

### Commitment and Application

When a leader confirms a log entry is stored on a majority of servers, it updates the `commitIndex`.

```go
if majorityReplicated {
    rf.commitIndex = newIndex
}
```

Background goroutine applies committed log entries:

```go
for rf.lastApplied < rf.commitIndex {
    rf.lastApplied++
    applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
}
```

---

## Challenges Encountered

- **Concurrency**: Ensuring thread-safe log access using mutexes.
- **Term Conflicts**: Correctly updating terms and roles upon stale RPCs.
- **Index Mismatches**: Implementing log truncation and backtracking in case of conflicts.

---

## Test Results

```bash
$ go test -race -run 3B
Test (3B): basic agreement... Passed
Test (3B): RPC byte count... Passed
Test (3B): agreement after follower reconnects... Passed
Test (3B): no agreement if too few servers... Passed
Test (3B): concurrent Start()s... Passed
Test (3B): rejoin of partitioned leader... Passed
Test (3B): leader backs up quickly over incorrect follower logs... Passed
Test (3B): lots of agreement... Passed
```

---
---

## Part C : Log Replication and Commitment

## Objective

The goal of Part 3C is to implement **log replication** and **commitment** in the Raft consensus algorithm. This includes:
- Appending new log entries from the leader to followers.
- Ensuring logs are consistent across servers.
- Committing log entries once a majority has acknowledged them.
- Applying committed entries to the state machine in order.

---

## Key Concepts

- **Log Replication**: Leaders send `AppendEntries` RPCs to followers with log entries.
- **Consistency Check**: Followers reject inconsistent entries and provide hints (`XTerm`, `XIndex`, `XLen`) for log correction.
- **Commit Index**: Leader advances the `commitIndex` when a log entry is replicated on a majority of servers.
- **Apply Channel**: Committed entries are sent via `applyCh` to the service for application to the state machine.

---

## Implementation Overview

### `Start(command interface{})`
Leaders append a new command to their local log, persist it, and trigger a broadcast to replicate it:
```go
entry := LogEntry{Term: rf.currentTerm, Command: command}
rf.log = append(rf.log, entry)
rf.persist()
go rf.broadcastAppendEntries()
```

### `broadcastAppendEntries()`
Leaders send `AppendEntries` to all peers. The method includes:
- Matching the follower's `PrevLogIndex` and `PrevLogTerm`
- Sending new log entries starting from `nextIndex[peer]`
- Handling failures with backtracking using `XTerm`, `XIndex`, and `XLen`
- Advancing `matchIndex[]` and `nextIndex[]` on success

Majority commit check:
```go
for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
    if rf.log[N].Term != rf.currentTerm {
        continue
    }
    count := 1
    for j := range rf.peers {
        if rf.matchIndex[j] >= N {
            count++
        }
    }
    if count > len(rf.peers)/2 {
        rf.commitIndex = N
        go rf.applyEntries()
        break
    }
}
```

### `AppendEntries(...)`
Followers validate the `PrevLogIndex` and `PrevLogTerm`. If mismatched, they return failure with conflict information (`XTerm`, `XIndex`, `XLen`). Otherwise:
- Conflicting entries are truncated
- New entries are appended
- `commitIndex` is updated and `applyEntries()` is triggered if needed

### Conflict Optimization
To reduce backtracking, the reply includes:
- `XTerm`: Term of conflicting entry
- `XIndex`: First index of `XTerm`
- `XLen`: Length of follower’s log

### `applyEntries()`
Applies committed entries to the state machine via `applyCh`:
```go
for rf.lastApplied < rf.commitIndex {
    next := rf.lastApplied + 1
    msg := raftapi.ApplyMsg{CommandValid: true, Command: rf.log[next].Command, CommandIndex: next}
    rf.applyCh <- msg
    rf.lastApplied = next
}
```

---
