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
