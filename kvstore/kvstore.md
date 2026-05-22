# Distributed KV Store with Raft

**Also learning**: Golang (near-zero experience, strong Python, intermediate Rust)

## What We're Building

A distributed key-value store with Raft consensus. Stages:
1. Single-node KV store (in-memory, Get/Put/Delete)
2. HTTP or gRPC API
3. Raft leader election
4. Raft log replication
5. Snapshots and compaction
6. Cluster membership changes

## Progress Log

### 2026-01-20
- Picked KV store as the toy model for learning Go + distributed systems
- Next: Task 1 - basic in-memory KV store with tests

### 2026-01-27
- Completed Task 1: basic in-memory KV store (Get/Put/Delete)
- Covered:
  - Go structs and methods
  - Pointer receivers vs value receivers
  - Maps are reference types (pointer to underlying data)
  - Guideline: use pointer receivers if the type will ever mutate
  - Package structure: can't have main() outside package main
- Built: `store.go` with Store struct, `store_test.go` with basic tests
- Next: Task 2 - HTTP or gRPC API layer

### 2026-01-28
- Completed Task 2: gRPC API layer
- Covered:
  - Import paths: module name + subdirectory (e.g., `kvstore/proto`)
  - Same-package code doesn't need imports
  - All function parameters must be named if any are named
  - `context` is a stdlib package, used by gRPC for cancellation/timeouts
  - gRPC adds error return type automatically (not defined in proto)
  - Design choice: use gRPC errors OR response fields for "not found", not both
  - Server reflection needed for grpcurl service discovery
  - Unexported struct + exported constructor pattern prevents nil pointer issues
- Built: `server.go` with gRPC handlers, `cmd/server/main.go` entry point
- Next: Task 3 - Raft leader election


Task 2: gRPC API Layer for KV Store

Goal

Add a gRPC API to the existing KV store. This introduces:
- Protocol Buffers (protobuf) for defining services
- Code generation with protoc
- Implementing Go interfaces (the generated service interface)
- Context handling (every gRPC call takes a context.Context)
- gRPC error handling with status codes

Files to Create/Modify
┌──────────────────────────┬────────────────────────────────────────────┐
│           File           │                  Purpose                   │
├──────────────────────────┼────────────────────────────────────────────┤
│ proto/kvstore.proto      │ Service definition (Get, Put, Delete RPCs) │
├──────────────────────────┼────────────────────────────────────────────┤
│ proto/kvstore.pb.go      │ Generated protobuf code (auto-generated)   │
├──────────────────────────┼────────────────────────────────────────────┤
│ proto/kvstore_grpc.pb.go │ Generated gRPC code (auto-generated)       │
├──────────────────────────┼────────────────────────────────────────────┤
│ server.go                │ gRPC server implementation                 │
├──────────────────────────┼────────────────────────────────────────────┤
│ cmd/server/main.go       │ Entry point to run the server              │
└──────────────────────────┴────────────────────────────────────────────┘
Steps

1. Setup: Install protoc and Go plugins

# Install protoc compiler (if not present)
# Install Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

2. Define proto/kvstore.proto

syntax = "proto3";
package kvstore;
option go_package = "./proto";

service KVStore {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Put(PutRequest) returns (PutResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}

message GetRequest { string key = 1; }
message GetResponse { string value = 1; }
message PutRequest { string key = 1; string value = 2; }
message PutResponse {}
message DeleteRequest { string key = 1; }
message DeleteResponse {}

3. Generate Go code

protoc --go_out=. --go-grpc_out=. proto/kvstore.proto

4. Implement server.go

- Create a server struct (unexported) that embeds the generated UnimplementedKVStoreServer
- Hold a reference to the existing *Store
- Create NewServer() constructor to initialize the store (prevents nil pointer issues)
- Implement Get, Put, Delete methods matching the generated interface

5. Create cmd/server/main.go

- Create a TCP listener on a port (e.g., :50051)
- Create a gRPC server
- Register the KVStore service
- Enable server reflection: `reflection.Register(grpcServer)` (needed for grpcurl)
- Call Serve()

6. Test with grpcurl

# Start server
go run cmd/server/main.go

# Test (in another terminal)
grpcurl -plaintext -d '{"key":"foo","value":"bar"}' localhost:50051 kvstore.KVStore/Put
grpcurl -plaintext -d '{"key":"foo"}' localhost:50051 kvstore.KVStore/Get

Go Concepts to Cover

- Interfaces: The generated code defines an interface. You implement it.
- Embedding: UnimplementedKVStoreServer provides default implementations.
- Context: ctx context.Context is the first parameter of every RPC. Used for cancellation/timeouts.
- Error handling: Return status.Error(codes.NotFound, "key not found") for gRPC errors.

Verification

1. go build ./... compiles without errors
2. Server starts and listens on port 50051
3. grpcurl or a test client can Put, Get, and Delete keys

---

## Task 3: Raft Leader Election (Single-Node Mechanics)

**Concept**: Raft consensus starts with leader election. A cluster of nodes must agree on a single leader. This task covers the single-node state machine. You'll learn:
- Goroutines and channels for concurrent state management
- Timers and timeouts
- State machines
- Mutex for protecting shared state

**Background**

Raft nodes have three states:
- **Follower**: Passive. Waits for heartbeats from leader.
- **Candidate**: Actively seeking votes to become leader.
- **Leader**: Handles client requests, sends heartbeats.

Key rules:
- Time is divided into **terms** (monotonically increasing integers)
- Each node votes once per term
- Candidate needs majority of votes to become leader
- If a follower doesn't hear from a leader, it starts an election

**Files to Create**

| File | Purpose |
|------|---------|
| `raft.go` | Raft node struct and election logic |
| `raft_test.go` | Tests for election behavior |
| `proto/raft.proto` | RequestVote RPC definition |

### Part A: Node State and Terms

Make this test pass:

```go
// raft_test.go
func TestNewRaftNode(t *testing.T) {
    node := NewRaftNode("node1")

    if node.State() != Follower {
        t.Errorf("new node should be Follower, got %v", node.State())
    }
    if node.CurrentTerm() != 0 {
        t.Errorf("new node should have term 0, got %d", node.CurrentTerm())
    }
}
```

### Part B: RequestVote RPC

Add to proto/raft.proto:

```protobuf
service Raft {
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
}

message RequestVoteRequest {
  uint64 term = 1;
  string candidate_id = 2;
}

message RequestVoteResponse {
  uint64 term = 1;
  bool vote_granted = 2;
}
```

Make these tests pass:

```go
func TestRequestVote_GrantsVote(t *testing.T) {
    node := NewRaftNode("node1")

    resp := node.HandleRequestVote(&RequestVoteRequest{
        Term:        1,
        CandidateId: "node2",
    })

    if !resp.VoteGranted {
        t.Error("should grant vote to candidate with higher term")
    }
    if node.CurrentTerm() != 1 {
        t.Errorf("should update term to 1, got %d", node.CurrentTerm())
    }
}

func TestRequestVote_DeniesVote_AlreadyVoted(t *testing.T) {
    node := NewRaftNode("node1")

    // First vote granted
    node.HandleRequestVote(&RequestVoteRequest{Term: 1, CandidateId: "node2"})

    // Second vote denied (already voted this term)
    resp := node.HandleRequestVote(&RequestVoteRequest{Term: 1, CandidateId: "node3"})

    if resp.VoteGranted {
        t.Error("should deny vote, already voted this term")
    }
}
```

### Part C: Election Timeout and Becoming Candidate

Make this test pass:

```go
func TestElectionTimeout_BecomeCandidate(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(50 * time.Millisecond) // Short timeout for testing

    node.Start()
    defer node.Stop()

    time.Sleep(100 * time.Millisecond)

    if node.State() != Candidate {
        t.Errorf("should become Candidate after timeout, got %v", node.State())
    }
    if node.CurrentTerm() != 1 {
        t.Errorf("should increment term to 1, got %d", node.CurrentTerm())
    }
}
```

**Go Concepts to Cover**
- Goroutines: `go func() {...}()` for background election timer
- Channels: For signaling state changes and stopping the node
- `time.Timer` or `time.After` for election timeouts
- `sync.Mutex` for protecting shared state
- `select` statement for handling multiple channels

**Hints available**: yes

**Verification**
1. `go test ./...` passes all new tests
2. Node starts as Follower with term 0
3. RequestVote grants/denies votes correctly
4. Node becomes Candidate after election timeout

---

## Task 3.5: Raft Election with Proper Concurrency

**Concept**: Refactor the election timeout mechanism to use proper Go concurrency patterns. This addresses the minimal implementation from Task 3.

**Requirements**:
- `Stop()` must terminate the background goroutine within 100ms
- All public methods must be safe to call from multiple goroutines (tests run with `-race`)
- Election timer must reset when a valid heartbeat is received
- Node must be restartable after Stop()

**New Tests to Add** (`raft_test.go`):

```go
func TestStop_TerminatesGoroutine(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(500 * time.Millisecond)
    node.Start()

    time.Sleep(50 * time.Millisecond) // Let it start
    node.Stop()
    time.Sleep(100 * time.Millisecond) // Give it time to stop

    // Should still be Follower (election didn't fire)
    if node.State() != Follower {
        t.Errorf("stop should prevent election, got %v", node.State())
    }
}

func TestHeartbeat_ResetsElectionTimer(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(100 * time.Millisecond)
    node.Start()
    defer node.Stop()

    // Send heartbeats faster than election timeout
    for i := 0; i < 5; i++ {
        time.Sleep(50 * time.Millisecond)
        node.ReceiveHeartbeat(1) // term 1
    }

    // Should still be Follower after 250ms (5 * 50ms)
    if node.State() != Follower {
        t.Errorf("heartbeats should prevent election, got %v", node.State())
    }
}

func TestConcurrentAccess(t *testing.T) {
    // Run with: go test -race
    node := NewRaftNode("node1")
    node.SetElectionTimeout(50 * time.Millisecond)
    node.Start()
    defer node.Stop()

    done := make(chan bool)

    // Concurrent readers
    go func() {
        for i := 0; i < 100; i++ {
            _ = node.State()
            _ = node.CurrentTerm()
        }
        done <- true
    }()

    // Concurrent vote requests
    go func() {
        for i := 0; i < 100; i++ {
            node.HandleRequestVote(&proto.RequestVoteRequest{
                Term:        uint64(i),
                CandidateId: "node2",
            })
        }
        done <- true
    }()

    <-done
    <-done
}
```

**Go Concepts to Cover** (hints, not requirements):
- `sync.Mutex` for protecting shared state
- Channels for stop signaling
- `time.Timer` with Reset() for resettable timeouts
- `select` statement for handling multiple event sources

**Hints available**: yes

**Verification**:
1. `go test -race ./...` passes all tests (no race conditions)
2. `Stop()` actually stops the election timer
3. Heartbeats prevent election timeout from firing



# Task 4: Candidate Requests Votes and Becomes Leader

Concept

When a node's election timer fires and it becomes a Candidate, it must:
1. Vote for itself (always gets 1 vote)
2. Send RequestVote RPCs to all other nodes in the cluster
3. If it receives votes from a majority (including itself), become Leader
4. If not, stay Candidate (retry on next timeout)

A 3-node cluster needs 2 votes. A 5-node cluster needs 3.

Requirements

- A candidate in a 3-node cluster that receives votes from both peers must become Leader
- A candidate that receives only its own vote must stay Candidate
- Vote requests must be sent to all peers concurrently (not sequentially)
- The node must be testable without real network connections
- Single-node cluster becomes Leader immediately (majority of 1)

Test file: kvstore/raft_test.go

Add this interface to raft.go:

// Peer represents a remote Raft node that can be called for votes
type Peer interface {
    RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error)
}

Make these tests pass:

// Mock peer for testing
type mockPeer struct {
    voteGranted bool
    term        uint64
    mu          sync.Mutex
}

func (m *mockPeer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    return &proto.RequestVoteResponse{
        Term:        m.term,
        VoteGranted: m.voteGranted,
    }, nil
}

func TestCandidate_WinsMajority_BecomesLeader(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(50 * time.Millisecond)

    // Two peers that will grant votes
    peer1 := &mockPeer{voteGranted: true, term: 1}
    peer2 := &mockPeer{voteGranted: true, term: 1}
    node.SetPeers([]Peer{peer1, peer2}) // 3-node cluster

    node.Start()
    defer node.Stop()

    time.Sleep(100 * time.Millisecond)

    if node.State() != Leader {
        t.Errorf("should become Leader with majority, got %v", node.State())
    }
}

func TestCandidate_NoMajority_StaysCandidate(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(50 * time.Millisecond)

    // Two peers that deny votes
    peer1 := &mockPeer{voteGranted: false, term: 1}
    peer2 := &mockPeer{voteGranted: false, term: 1}
    node.SetPeers([]Peer{peer1, peer2})

    node.Start()
    defer node.Stop()

    time.Sleep(100 * time.Millisecond)

    if node.State() != Candidate {
        t.Errorf("should stay Candidate without majority, got %v", node.State())
    }
}

func TestCandidate_PartialVotes_NeedsMajority(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(50 * time.Millisecond)

    // 5-node cluster: need 3 votes for majority
    peer1 := &mockPeer{voteGranted: true, term: 1}  // grants
    peer2 := &mockPeer{voteGranted: false, term: 1} // denies
    peer3 := &mockPeer{voteGranted: false, term: 1} // denies
    peer4 := &mockPeer{voteGranted: false, term: 1} // denies
    node.SetPeers([]Peer{peer1, peer2, peer3, peer4})

    node.Start()
    defer node.Stop()

    time.Sleep(100 * time.Millisecond)

    // 2 votes (self + peer1) out of 5 - not majority
    if node.State() != Candidate {
        t.Errorf("should stay Candidate with 2/5 votes, got %v", node.State())
    }
}

func TestCandidate_VotesRequestedConcurrently(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(50 * time.Millisecond)

    // Peers that take time to respond
    slowPeer1 := &slowMockPeer{delay: 30 * time.Millisecond, voteGranted: true, term: 1}
    slowPeer2 := &slowMockPeer{delay: 30 * time.Millisecond, voteGranted: true, term: 1}
    node.SetPeers([]Peer{slowPeer1, slowPeer2})

    start := time.Now()
    node.Start()
    defer node.Stop()

    time.Sleep(150 * time.Millisecond)
    elapsed := time.Since(start)

    if node.State() != Leader {
        t.Errorf("should become Leader, got %v", node.State())
    }

    // If sequential: 50ms + 30ms + 30ms = 110ms minimum
    // If concurrent: 50ms + 30ms = 80ms
    if elapsed > 120*time.Millisecond {
        t.Errorf("votes appear sequential, took %v (expected < 120ms)", elapsed)
    }
}

type slowMockPeer struct {
    delay       time.Duration
    voteGranted bool
    term        uint64
}

func (m *slowMockPeer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
    time.Sleep(m.delay)
    return &proto.RequestVoteResponse{
        Term:        m.term,
        VoteGranted: m.voteGranted,
    }, nil
}

func TestSingleNodeCluster_BecomesLeader(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(50 * time.Millisecond)
    node.SetPeers([]Peer{}) // No peers

    node.Start()
    defer node.Stop()

    time.Sleep(100 * time.Millisecond)

    if node.State() != Leader {
        t.Errorf("single node should become Leader, got %v", node.State())
    }
}

Go Concepts to Cover

- Interfaces: Define Peer interface for testability (mock peers vs real gRPC clients)
- Goroutines: Send RequestVote to each peer concurrently with go func()
- Channels or sync.WaitGroup: Collect responses from concurrent goroutines
- Majority calculation: majority := (clusterSize / 2) + 1
- Context: Pass context to peer RPCs

What to modify in startElection()

Currently:
func (node *node) startElection() {
    node.mu.Lock()
    defer node.mu.Unlock()
    node.state = Candidate
    node.term++
}

Needs to:
1. Increment term, become Candidate
2. Count self-vote (1)
3. Send RequestVote to all peers concurrently
4. Collect responses, count granted votes
5. If votes >= majority, become Leader

Files to modify

- kvstore/raft.go - Add Peer interface, peers field, SetPeers(), update startElection()
- kvstore/raft_test.go - Add the test cases above

Verification

go test -race ./kvstore/...

All tests pass, including:
- Majority votes → Leader
- No majority → stays Candidate
- Single node → Leader
- Concurrent timing test passes

Hints available

Yes - ask if stuck on goroutine coordination, channel patterns, or majority calculation.

---

# Task 5: Fix Loose Ends in the Raft Node

Concept

Tasks 3, 3.5, and 4 produced a node that holds elections and can become Leader, but the implementation has accumulated TODOs and quiet bugs that will compound once we add AppendEntries / log replication. This task is a cleanup pass: tighten the state machine so heartbeats and log replication can be built on a sound base.

This teaches:
- Raft safety invariants beyond election (step-down on higher term, votedFor)
- State-aware run loops (a Leader must not run an election timer)
- Non-blocking channel sends (select with default)
- Randomized timeouts to avoid split votes
- Defensive coding under -race

Requirements

1. ReceiveHeartbeat(term) must not block if Start has not been called. Calling it on a stopped node is a no-op.
2. A Leader must not increment its term on its own election timer. Once state is Leader, the run loop must not call startElection.
3. If startElection observes a peer response with Term > node.term, the node must step down to Follower at that term and abort the election (no Leader transition for that term).
4. HandleRequestVote must track votedFor explicitly:
   - On term change: clear votedFor.
   - At the same term: grant only if votedFor is unset OR equals the requesting candidate (idempotent).
5. RequestVote errors from peers must be logged (use the stdlib log package). The result channel still receives false on error so vote counting is unaffected.
6. Election timeout must be randomized: the actual wait is uniform in [timeout, 2*timeout) on each (re)set. The lower bound is the configured value, so existing tests that set a short timeout still fire within their expected window.

Test file: kvstore/raft_test.go

Make these tests pass:

func TestReceiveHeartbeat_BeforeStart_DoesNotBlock(t *testing.T) {
    node := NewRaftNode("node1")
    done := make(chan struct{})
    go func() {
        node.ReceiveHeartbeat(0)
        close(done)
    }()
    select {
    case <-done:
    case <-time.After(100 * time.Millisecond):
        t.Fatal("ReceiveHeartbeat blocked when Start had not been called")
    }
}

func TestLeader_DoesNotStartNewElection(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(50 * time.Millisecond)
    node.SetPeers([]Peer{
        &mockPeer{voteGranted: true, term: 1},
        &mockPeer{voteGranted: true, term: 1},
    })
    node.Start()
    defer node.Stop()

    // Wait until Leader.
    deadline := time.After(500 * time.Millisecond)
    for node.State() != Leader {
        select {
        case <-deadline:
            t.Fatal("never became Leader")
        case <-time.After(5 * time.Millisecond):
        }
    }
    leaderTerm := node.CurrentTerm()

    // Sleep well past several election timeouts.
    time.Sleep(300 * time.Millisecond)

    if node.State() != Leader {
        t.Errorf("Leader should not step down without external cause, got %v", node.State())
    }
    if node.CurrentTerm() != leaderTerm {
        t.Errorf("Leader term advanced from %d to %d (phantom election)", leaderTerm, node.CurrentTerm())
    }
}

func TestCandidate_StepsDown_OnHigherTermResponse(t *testing.T) {
    node := NewRaftNode("node1")
    node.SetElectionTimeout(50 * time.Millisecond)
    node.SetPeers([]Peer{
        &mockPeer{voteGranted: false, term: 99},
        &mockPeer{voteGranted: false, term: 99},
    })
    node.Start()
    defer node.Stop()

    time.Sleep(150 * time.Millisecond)

    if node.State() != Follower {
        t.Errorf("should step down to Follower on higher-term response, got %v", node.State())
    }
    if node.CurrentTerm() != 99 {
        t.Errorf("should adopt peer term 99, got %d", node.CurrentTerm())
    }
}

func TestRequestVote_SameTerm_DifferentCandidate_Denied(t *testing.T) {
    node := NewRaftNode("node1")
    node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "node2"})
    resp := node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "node3"})
    if resp.VoteGranted {
        t.Error("should deny: already voted for node2 in term 5")
    }
}

func TestRequestVote_SameTerm_SameCandidate_Granted(t *testing.T) {
    node := NewRaftNode("node1")
    node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "node2"})
    resp := node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "node2"})
    if !resp.VoteGranted {
        t.Error("re-vote for same candidate at same term must be idempotent")
    }
}

func TestElectionTimeout_IsRandomized(t *testing.T) {
    const N = 20
    base := 80 * time.Millisecond
    fires := make([]time.Duration, N)
    var wg sync.WaitGroup
    for i := 0; i < N; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            n := NewRaftNode("n")
            n.SetElectionTimeout(base)
            n.SetPeers([]Peer{&mockPeer{voteGranted: false, term: 0}, &mockPeer{voteGranted: false, term: 0}})
            start := time.Now()
            n.Start()
            defer n.Stop()
            for n.State() == Follower {
                time.Sleep(2 * time.Millisecond)
            }
            fires[i] = time.Since(start)
        }(i)
    }
    wg.Wait()
    // All firings must be >= base, and not all identical.
    var min, max time.Duration = fires[0], fires[0]
    for _, f := range fires {
        if f < base {
            t.Errorf("fired before configured minimum: %v < %v", f, base)
        }
        if f < min { min = f }
        if f > max { max = f }
    }
    if max-min < 5*time.Millisecond {
        t.Errorf("election timeouts look fixed (spread %v); expected randomization", max-min)
    }
}

Go Concepts to Cover

- Non-blocking channel send: `select { case ch <- v: default: }`
- State guards inside a run loop: check `node.state` before acting on a timer event
- math/rand for jittered durations; seeded once at process start (or per-node if preferred)
- Idiomatic step-down: a single helper `becomeFollower(term uint64)` that resets state, term, and votedFor under the lock
- log.Printf for peer-RPC errors (avoid swallowing diagnostics)

What to modify

- kvstore/raft.go
  - Add `votedFor string` to the node struct; clear in becomeFollower.
  - Add `becomeFollower(term uint64)` helper (caller holds the lock or method takes it — pick one and stick to it).
  - In Start's select loop: when timeoutTimer fires, only call startElection if state != Leader.
  - In startElection: while collecting responses, if any resp.Term > node.term, call becomeFollower(resp.Term) and return without promoting to Leader.
  - In HandleRequestVote: implement the votedFor logic above.
  - In ReceiveHeartbeat: use a non-blocking send.
  - Replace `time.NewTimer(node.electionTimeout)` with a helper that returns `node.electionTimeout + jitter`, where jitter is uniform in [0, electionTimeout). Apply on initial creation and on every Reset.

- kvstore/raft_test.go
  - Add the six tests above.

Verification

cd /workspace/kvstore
go test -race ./...

All previous tests plus the six new ones pass. No new TODOs introduced.

Hints available

Yes - ask if stuck on the votedFor reset rule, the step-down race in startElection, or the timer-jitter helper.

---

# Task 6: Heartbeats and a Per-State Run Loop

Concept

A Leader holds authority by sending periodic AppendEntries RPCs (empty = heartbeat) to all peers. Without heartbeats, followers' election timers fire and a new election starts. This task adds the heartbeat path and refactors the run loop so each role owns its own goroutine, eliminating the `if state != Leader` guards that started accumulating in Task 5.

This teaches:
- AppendEntries RPC (heartbeat-only form; log replication is Task 7)
- Per-role goroutines as a state-machine pattern (`runFollower`, `runCandidate`, `runLeader`)
- Decoupling RPC dispatch from state mutation (`requestVotes()` returns an outcome; the role loop decides the transition)
- Heartbeat interval vs. election timeout invariant

Background

In Raft, AppendEntries serves two purposes: replicating log entries (later) and acting as the leader's heartbeat (now). A follower that receives a valid AppendEntries resets its election timer. A candidate that receives an AppendEntries from a leader at term >= its own steps down to Follower. The heartbeat interval must be well below the election timeout — typically `heartbeatInterval ~ electionTimeout / 5`.

Requirements

1. Add `AppendEntries(ctx, req)` to the `Peer` interface. The proto carries `term` and `leader_id` for now.
2. The Leader sends `AppendEntries` to every peer every `heartbeatInterval` (constructor param, e.g., 50ms when election timeout is 250ms). All peer calls dispatched concurrently, not sequentially.
3. A Leader that observes any peer response with `Term > currentTerm` steps down to Follower at the new term and stops sending heartbeats.
4. `HandleAppendEntries(req)` on a node:
   - If `req.Term < currentTerm`: reply `{Term: currentTerm, Success: false}`, no state change.
   - If `req.Term > currentTerm`: become Follower at `req.Term`.
   - If `req.Term == currentTerm` and state is Candidate: become Follower at the same term (a leader exists for this term).
   - In all accept paths: reset the election timer (signal via `heartbeatChan`).
   - Reply `{Term: currentTerm, Success: true}` on accept.
5. Refactor `Start` to a per-role driver:
   ```go
   func (n *node) run(ctx context.Context) {
       for ctx.Err() == nil {
           switch n.State() {
           case Follower:  n.runFollower(ctx)
           case Candidate: n.runCandidate(ctx)
           case Leader:    n.runLeader(ctx)
           }
       }
   }
   ```
   Each `runX` blocks until its role ends, then returns. No `if state != Leader` checks remain in any select case.
6. `startElection` is split: a pure `requestVotes()` that issues the RPCs and returns a tally; `runCandidate` interprets the tally and transitions.
7. Tests run with `-race` and pass.

Proto additions

Add to `proto/raft.proto`:

```protobuf
service Raft {
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
}

message AppendEntriesRequest {
  uint64 term = 1;
  string leader_id = 2;
}

message AppendEntriesResponse {
  uint64 term = 1;
  bool success = 2;
}
```

Regenerate with `protoc --go_out=. --go-grpc_out=. proto/raft.proto`.

Test file: kvstore/raft_test.go

Update `mockPeer` to satisfy the extended `Peer` interface (record `AppendEntries` calls):

```go
type mockPeer struct {
    voteGranted   bool
    term          uint64
    appendCalls   atomic.Int64
    mu            sync.Mutex
}

func (m *mockPeer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    return &proto.RequestVoteResponse{Term: m.term, VoteGranted: m.voteGranted}, nil
}

func (m *mockPeer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
    m.appendCalls.Add(1)
    m.mu.Lock()
    defer m.mu.Unlock()
    return &proto.AppendEntriesResponse{Term: m.term, Success: true}, nil
}
```

Make these tests pass:

```go
func TestLeader_SendsHeartbeats(t *testing.T) {
    node := NewRaftNode("node1", 250*time.Millisecond, 50*time.Millisecond)
    p1 := &mockPeer{voteGranted: true, term: 1}
    p2 := &mockPeer{voteGranted: true, term: 1}
    node.SetPeers([]Peer{p1, p2})
    node.Start()
    defer node.Stop()

    // Wait for Leader.
    deadline := time.After(1 * time.Second)
    for node.State() != Leader {
        select {
        case <-deadline:
            t.Fatal("never became Leader")
        case <-time.After(5 * time.Millisecond):
        }
    }
    // Within ~3 heartbeat intervals, expect >= 3 AppendEntries calls per peer.
    time.Sleep(180 * time.Millisecond)
    if p1.appendCalls.Load() < 3 || p2.appendCalls.Load() < 3 {
        t.Errorf("expected >=3 heartbeats per peer, got p1=%d p2=%d",
            p1.appendCalls.Load(), p2.appendCalls.Load())
    }
}

func TestLeader_StepsDown_OnHigherTermAppendResponse(t *testing.T) {
    node := NewRaftNode("node1", 100*time.Millisecond, 25*time.Millisecond)
    // Peers grant votes at term 1, then start replying with a higher term.
    p1 := &mockPeer{voteGranted: true, term: 1}
    p2 := &mockPeer{voteGranted: true, term: 1}
    node.SetPeers([]Peer{p1, p2})
    node.Start()
    defer node.Stop()

    for node.State() != Leader {
        time.Sleep(5 * time.Millisecond)
    }
    // Bump peer term to force step-down on next heartbeat response.
    p1.mu.Lock(); p1.term = 99; p1.mu.Unlock()
    p2.mu.Lock(); p2.term = 99; p2.mu.Unlock()

    deadline := time.After(500 * time.Millisecond)
    for node.State() == Leader {
        select {
        case <-deadline:
            t.Fatal("Leader did not step down on higher-term response")
        case <-time.After(5 * time.Millisecond):
        }
    }
    if node.State() != Follower {
        t.Errorf("expected Follower after step-down, got %v", node.State())
    }
    if node.CurrentTerm() != 99 {
        t.Errorf("expected term 99 after step-down, got %d", node.CurrentTerm())
    }
}

func TestFollower_ResetsTimer_OnAppendEntries(t *testing.T) {
    node := NewRaftNode("node1", 100*time.Millisecond, 25*time.Millisecond)
    node.SetPeers([]Peer{&mockPeer{}, &mockPeer{}})
    node.Start()
    defer node.Stop()

    // Heartbeat faster than election timeout for 300ms total.
    for i := 0; i < 6; i++ {
        time.Sleep(50 * time.Millisecond)
        node.HandleAppendEntries(&proto.AppendEntriesRequest{Term: 1, LeaderId: "leader"})
    }
    if node.State() != Follower {
        t.Errorf("AppendEntries should keep Follower, got %v", node.State())
    }
    if node.CurrentTerm() != 1 {
        t.Errorf("expected term 1 after AppendEntries, got %d", node.CurrentTerm())
    }
}

func TestCandidate_StepsDown_OnAppendEntries(t *testing.T) {
    node := NewRaftNode("node1", 50*time.Millisecond, 25*time.Millisecond)
    // Peers deny votes so node stays Candidate.
    node.SetPeers([]Peer{
        &mockPeer{voteGranted: false, term: 0},
        &mockPeer{voteGranted: false, term: 0},
    })
    node.Start()
    defer node.Stop()

    for node.State() != Candidate {
        time.Sleep(5 * time.Millisecond)
    }
    candidateTerm := node.CurrentTerm()

    // A leader at the same term sends AppendEntries.
    resp := node.HandleAppendEntries(&proto.AppendEntriesRequest{
        Term: candidateTerm, LeaderId: "leader",
    })
    if !resp.Success {
        t.Errorf("AppendEntries at same term should be accepted by Candidate")
    }
    // Give runCandidate a moment to observe the state change.
    time.Sleep(20 * time.Millisecond)
    if node.State() != Follower {
        t.Errorf("Candidate should step down on AppendEntries, got %v", node.State())
    }
}

func TestHandleAppendEntries_RejectsLowerTerm(t *testing.T) {
    node := NewRaftNode("node1", 250*time.Millisecond, 50*time.Millisecond)
    node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "x"})
    resp := node.HandleAppendEntries(&proto.AppendEntriesRequest{Term: 4, LeaderId: "stale"})
    if resp.Success {
        t.Error("AppendEntries from lower term must be rejected")
    }
    if resp.Term != 5 {
        t.Errorf("response should carry currentTerm=5, got %d", resp.Term)
    }
}
```

Adapt earlier tests to the new constructor signature `NewRaftNode(name, electionTimeout, heartbeatInterval)`. Drop calls to `SetElectionTimeout`.

Go Concepts to Cover

- Per-role goroutines: each `runX` is a small select loop that returns when the role ends. The driver `run(ctx)` re-dispatches based on the new state.
- Decoupling: `requestVotes()` is pure — it sends RPCs and returns a tally. `runCandidate` decides what to do with the tally.
- `time.Ticker` for the leader's heartbeat cadence.
- Reading state inside a role loop: reads still need the mutex; transitions should go through `becomeFollower` / `becomeLeader` helpers.
- Atomic counters in tests: `atomic.Int64` for cheap call counts without mutex contention.

What to modify

- `kvstore/proto/raft.proto`
  - Add `AppendEntries` RPC and messages. Regenerate.
- `kvstore/raft.go`
  - Extend `Peer` interface with `AppendEntries`.
  - Add `heartbeatInterval time.Duration` field, set in constructor.
  - Replace `SetElectionTimeout` with constructor param `NewRaftNode(name string, electionTimeout, heartbeatInterval time.Duration)`.
  - Replace the single goroutine in `Start` with `run(ctx)` dispatching to `runFollower`, `runCandidate`, `runLeader`.
  - `runFollower`: select on election timer + heartbeatChan + ctx; on timer, transition to Candidate and return.
  - `runCandidate`: call `requestVotes()` (split out from old `startElection`), set Leader/Follower based on tally, return on any state change.
  - `runLeader`: `time.Ticker` at `heartbeatInterval`; on each tick, fan out `AppendEntries` to peers. Inspect responses; on `Term > currentTerm`, `becomeFollower(term)` and return.
  - Add `HandleAppendEntries(req) *AppendEntriesResponse` per requirement 4.
  - Add `becomeLeader()` helper symmetric with `becomeFollower`.
- `kvstore/raft_test.go`
  - Update `mockPeer` to implement `AppendEntries`.
  - Update existing test constructors to the new signature.
  - Add the five tests above.

Verification

```
cd /workspace/kvstore
go test -race ./...
```

All previous tests pass under the new constructor, plus the five new ones. No `if state != Leader` guards remain in `raft.go`.

Hints available

Yes — ask if stuck on the role-loop teardown (how `runX` returns cleanly), the heartbeat fan-out pattern, or the AppendEntries / step-down race.

---

# Tentative Plan: Tasks 7+

This is a sketch of the remaining tasks to reach a complete pedagogical Raft toy model. Not binding — we can add, drop, reorder, or rescope as we learn what's interesting. Revisit before starting each task.

## Core (must-have for a "decent, complete" model)

**Task 7 — Log replication.** The heart of Raft. AppendEntries carries actual entries; introduce log matching (`prevLogIndex`/`prevLogTerm` consistency check), per-peer `nextIndex`/`matchIndex`, follower truncation on conflict, leader commit index advancing once replicated to majority. Add the election restriction to RequestVote (candidate's log must be at least as up-to-date as voter's). Longest task — Raft is mostly this.

**Task 8 — Apply committed entries to the KV store.** Wire the Raft log to `store.go`. Leader proposes entries from gRPC `Put`/`Delete` calls, blocks until committed, then replies. A dedicated `applyLoop` drains committed entries into the state machine. Non-leaders redirect or return a "not leader, try X" error to the client.

**Task 9 — Real gRPC transport between nodes.** Replace `mockPeer` with a `grpcPeer` that dials real addresses. Multi-process cluster runnable as `./server --id=n1 --peers=...`. This is where the integration bugs the unit tests miss show up.

## Optional extensions (if we want to go deeper)

**Task 10 — Persistence.** Dump `term`, `votedFor`, `log[]` to disk before responding to RPCs. Conceptually trivial; the value is internalizing *why* (e.g., votedFor across crashes prevents split votes). Skippable if read in the paper instead.

**Task 11 — Snapshots + log compaction.** Separate sub-protocol (`InstallSnapshot` RPC), discard log prefixes, ship full state to lagging followers. Self-contained module; significant code.

**Task 12 — Membership changes (joint consensus).** Genuinely subtle — the only part of the paper with known issues. High learning value, often skipped in pedagogical implementations.

## Default plan

Dive deep on 6–9. Read 10 conceptually. Skip 11–12 unless interest pulls us back. Reaching the end of Task 9 means we could read the etcd `raft` package and follow it.

It's OK to deviate from this plan as understanding grows.
