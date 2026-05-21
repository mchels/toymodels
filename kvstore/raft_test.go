package kvstore

import (
	"context"
	"kvstore/proto"
	"sync"
	"testing"
	"time"
)

func TestNewRaftNode(t *testing.T) {
	node := NewRaftNode("node1")

	if node.State() != Follower {
		t.Errorf("new node should be Follower, got %v", node.State())
	}
	if node.CurrentTerm() != 0 {
		t.Errorf("new node should have term 0, got %d", node.CurrentTerm())
	}
}

func TestRequestVote_GrantsVote(t *testing.T) {
	node := NewRaftNode("node1")

	resp := node.HandleRequestVote(&proto.RequestVoteRequest{
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
	node.HandleRequestVote(&proto.RequestVoteRequest{Term: 1, CandidateId: "node2"})

	// Second vote denied (already voted this term)
	resp := node.HandleRequestVote(&proto.RequestVoteRequest{Term: 1, CandidateId: "node3"})

	if resp.VoteGranted {
		t.Error("should deny vote, already voted this term")
	}
}

func TestStop_TerminatesGoroutine(t *testing.T) {
	node := NewRaftNode("node1")
	node.SetElectionTimeout(150 * time.Millisecond)
	node.Start()

	time.Sleep(50 * time.Millisecond) // Let it start
	node.Stop()
	time.Sleep(200 * time.Millisecond) // Give it time to stop

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

// =============================================================================
// Task 4: Candidate Requests Votes and Becomes Leader
// =============================================================================

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

// Slow peer for testing concurrency
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

	deadline := time.After(200 * time.Millisecond)
	for node.State() != Leader {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for Leader")
		case <-time.After(5 * time.Millisecond):
		}
	}
	elapsed := time.Since(start)

	// If concurrent: 50ms (timeout) + 30ms (parallel votes) ≈ 80ms
	// If sequential: 50ms + 30ms + 30ms ≈ 110ms
	if elapsed > 100*time.Millisecond {
		t.Errorf("votes appear sequential, took %v (expected < 100ms)", elapsed)
	}
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
