package kvstore

import (
	"kvstore/proto"
)

type NodeState string

const (
	Follower  NodeState = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

type node struct {
	name  string
	state NodeState
	term  uint64
}

func NewRaftNode(name string) *node {
	return &node{
		name:  name,
		state: Follower,
		term:  0,
	}
}

func (node *node) State() NodeState {
	return node.state
}

func (node *node) CurrentTerm() uint64 {
	return node.term
}

func (node *node) HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	if req.Term > node.term {
		node.term = req.Term
		node.state = Follower
		return &proto.RequestVoteResponse{
			Term:        req.Term,
			VoteGranted: true,
		}
	}
	return &proto.RequestVoteResponse{
		Term:        node.term,
		VoteGranted: false,
	}
}
