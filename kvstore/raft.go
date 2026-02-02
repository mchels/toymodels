package kvstore

import (
	"kvstore/proto"
	"sync"
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
	heartbeatChan   chan uint64
	quitChan        chan bool
	mu              sync.Mutex
}

func NewRaftNode(name string) *node {
	return &node{
		name:            name,
		state:           Follower,
		term:            0,
		electionTimeout: 300 * time.Millisecond,
		heartbeatChan:   make(chan uint64),
		quitChan:        make(chan bool),
	}
}

func (node *node) State() NodeState {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.state
}

func (node *node) CurrentTerm() uint64 {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.term
}

func (node *node) Start() {
	timeoutTimer := time.NewTimer(node.electionTimeout)
	go func() {
		for {
			select {
			case <-timeoutTimer.C:
				node.startElection()
			case _ = <-node.heartbeatChan:
				// TODO do something with received term.
				timeoutTimer.Reset(node.electionTimeout)
			case <-node.quitChan:
				timeoutTimer.Stop()
				return
			}
		}
	}()
}

func (node *node) Stop() {
	close(node.quitChan)
}

func (node *node) startElection() {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.state = Candidate
	node.term += 1
}

func (node *node) ReceiveHeartbeat(term uint64) {
	node.heartbeatChan <- term
}

func (node *node) SetElectionTimeout(timeout time.Duration) {
	node.electionTimeout = timeout
}

func (node *node) HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	if req.Term > node.term {
		node.mu.Lock()
		defer node.mu.Unlock()
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
