package kvstore

import (
	"kvstore/proto"
	"time"
)

type NodeState string

const (
	Follower  NodeState = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

type node struct {
	name            string
	state           NodeState
	term            uint64
	electionTimeout time.Duration
}

func NewRaftNode(name string) *node {
	return &node{
		name:            name,
		state:           Follower,
		term:            0,
		electionTimeout: 300 * time.Millisecond,
	}
}

func (node *node) State() NodeState {
	return node.state
}

func (node *node) CurrentTerm() uint64 {
	return node.term
}

func (node *node) Start() {
	go node.waitForHeartbeat()
}

func (node *node) Stop() {

}

func (node *node) waitForHeartbeat() {
	time.Sleep(node.electionTimeout)
	node.state = Candidate
	node.term += 1
}

func (node *node) SetElectionTimeout(timeout time.Duration) {
	node.electionTimeout = timeout
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
