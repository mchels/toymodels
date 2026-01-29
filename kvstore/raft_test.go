package kvstore

import (
	"kvstore/proto"
	"testing"
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
