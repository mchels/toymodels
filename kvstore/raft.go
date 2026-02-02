package kvstore

import (
	"context"
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
	cancel          context.CancelFunc
	mu              sync.Mutex
}

func NewRaftNode(name string) *node {
	return &node{
		name:            name,
		state:           Follower,
		term:            0,
		electionTimeout: 300 * time.Millisecond,
		heartbeatChan:   make(chan uint64),
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
	ctx, cancel := context.WithCancel(context.Background())
	node.mu.Lock()
	timeoutTimer := time.NewTimer(node.electionTimeout)
	if node.cancel != nil {
		node.cancel()
	}
	node.cancel = cancel
	node.mu.Unlock()
	go func(ctx context.Context) {
		for {
			select {
			case <-timeoutTimer.C:
				node.startElection()
			case <-node.heartbeatChan:
				// TODO do something with received term.
				timeoutTimer.Reset(node.electionTimeout)
			case <-ctx.Done():
				timeoutTimer.Stop()
				return
			}
		}
	}(ctx)
}

func (node *node) Stop() {
	node.mu.Lock()
	if node.cancel != nil {
		node.cancel()
	}
	node.mu.Unlock()
}

func (node *node) startElection() {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.state = Candidate
	node.term++
}

func (node *node) ReceiveHeartbeat(term uint64) {
	// TODO: This blocks forever if Start hasn't been called.
	node.heartbeatChan <- term
}

func (node *node) SetElectionTimeout(timeout time.Duration) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.electionTimeout = timeout
}

func (node *node) HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	node.mu.Lock()
	defer node.mu.Unlock()
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
