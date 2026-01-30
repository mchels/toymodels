package kvstore

import (
	"kvstore/proto"
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
