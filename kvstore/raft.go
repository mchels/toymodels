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

// Peer represents a remote Raft node that can be called for votes
type Peer interface {
	RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error)
}

type node struct {
	name            string
	state           NodeState
	term            uint64
	electionTimeout time.Duration
	heartbeatChan   chan uint64
	cancel          context.CancelFunc
	peers           []Peer
	mu              sync.Mutex
}

func NewRaftNode(name string) *node {
	return &node{
		name:            name,
		state:           Follower,
		term:            0,
		electionTimeout: 300 * time.Millisecond,
		heartbeatChan:   make(chan uint64),
		peers:           []Peer{},
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

func (node *node) SetPeers(peers []Peer) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.peers = peers
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
	node.state = Candidate
	node.term++
	nVotes := 1 // Start out by voting for self.
	var wg sync.WaitGroup
	// TODO: How to pass responses and error on a channel? Simple option: Wrap in a struct.
	// TODO: What should we do on errors, actually? Right now we just ignore them.
	results := make(chan bool)
	nVotesRequired := (len(node.peers)+1)/2 + 1
	for _, peer := range node.peers {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := &proto.RequestVoteRequest{
			Term:        node.term,
			CandidateId: node.name,
		}
		wg.Add(1)
		go func(p Peer) {
			defer wg.Done()
			resp, err := p.RequestVote(ctx, req)
			if err != nil {
				// TODO: Handle
			}
			if resp != nil {
				results <- resp.VoteGranted
			} else {
				results <- false
			}
		}(peer)
	}
	node.mu.Unlock()

	go func() {
		wg.Wait()
		close(results)
	}()

	for granted := range results {
		if granted {
			nVotes++
		}
	}

	if nVotes >= nVotesRequired {
		node.mu.Lock()
		node.state = Leader
		node.mu.Unlock()
	}
}

func (node *node) ReceiveHeartbeat(term uint64) {
	// If we do a standard blocking send and Start has NOT been called, this method blocks forever.
	// Therefore, we do a non-blocking send.
	select {
	case node.heartbeatChan <- term:
	default:
	}
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
