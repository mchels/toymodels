package kvstore

import "testing"

func TestNewRaftNode(t *testing.T) {
    node := NewRaftNode("node1")

    if node.State() != Follower {
        t.Errorf("new node should be Follower, got %v", node.State())
    }
    if node.CurrentTerm() != 0 {
        t.Errorf("new node should have term 0, got %d", node.CurrentTerm())
    }
}
