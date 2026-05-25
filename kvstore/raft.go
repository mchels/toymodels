package kvstore

import (
	"context"
	"kvstore/proto"
	"log"
	"math/rand"
	"slices"
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

	AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error)
}

type node struct {
	name              string
	state             NodeState
	term              uint64
	votedFor          string
	electionTimeout   time.Duration
	heartbeatInterval time.Duration
	heartbeatChan     chan uint64
	cancel            context.CancelFunc
	peers             []Peer
	mu                sync.Mutex
}

const defaultElectionTimeout time.Duration = 300 * time.Millisecond
const defaultHeartbeatInterval time.Duration = defaultElectionTimeout / 5

func drawRandomTimeout(baseTimeout time.Duration) time.Duration {
	return time.Duration(float64(baseTimeout) * (1 + rand.Float64()))
}

func NewRaftNode(name string, electionTimeout time.Duration, heartbeatInterval time.Duration) *node {
	if name == "" {
		panic("Node name must not be empty")
	}
	if electionTimeout == 0 {
		electionTimeout = defaultElectionTimeout
	}
	if heartbeatInterval == 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	return &node{
		name:              name,
		state:             Follower,
		term:              0,
		electionTimeout:   electionTimeout,
		heartbeatInterval: heartbeatInterval,
		heartbeatChan:     make(chan uint64),
		peers:             []Peer{},
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
	if node.cancel != nil {
		node.cancel()
	}
	node.cancel = cancel
	node.mu.Unlock()
	go node.run(ctx)
}

func (node *node) run(ctx context.Context) {
	for ctx.Err() == nil {
		switch node.State() {
		case Follower:
			node.runFollower(ctx)
		case Candidate:
			node.runCandidate(ctx)
		case Leader:
			node.runLeader(ctx)
		}
	}
}

func (node *node) runFollower(ctx context.Context) {
	electionTimer := time.NewTimer(drawRandomTimeout(node.electionTimeout))
	for {
		select {
		case <-electionTimer.C:
			node.mu.Lock()
			node.state = Candidate
			node.term = node.term + 1
			node.votedFor = node.name
			node.mu.Unlock()
			return
		case <-node.heartbeatChan:
			// TODO do something with received term. To be handled in Task 6.
			electionTimer.Reset(drawRandomTimeout(node.electionTimeout))
		case <-ctx.Done():
			electionTimer.Stop()
			return
		}
	}
}

func (node *node) runCandidate(ctx context.Context) {
	node.mu.Lock()
	if node.state != Candidate {
		node.mu.Unlock()
		return
	}
	nVotes := 1 // Start out by voting for self.
	nVotesRequired := (len(node.peers)+1)/2 + 1
	maxTermObserved := node.term
	nodeName := node.name
	electionTerm := node.term
	nodePeers := slices.Clone(node.peers)
	node.mu.Unlock()
	voteResults := requestVotes(nodeName, electionTerm, nodePeers)

	for _, result := range voteResults {
		if result.voteGranted {
			nVotes++
		}
		if result.term > maxTermObserved {
			maxTermObserved = result.term
		}
	}

	node.mu.Lock()
	if maxTermObserved > node.term {
		// Abort election since a peer was found that has a higher term than this node.
		node.becomeFollower(maxTermObserved)
		node.mu.Unlock()
		return
	}

	// Check that node wasn't bumped down to Follower with a RequestVote since we unlocked above.
	if nVotes >= nVotesRequired && node.term == electionTerm && node.state == Candidate {
		node.state = Leader
		node.mu.Unlock()
		return
	}
	node.mu.Unlock()

	// No one gained majority so `node` is still Candidate. Wait for a bit until having a new
	// election.
	select {
	case <-time.After(drawRandomTimeout(node.electionTimeout)):
	case <-ctx.Done():
	}
}

func requestVotes(candidateId string, nodeTerm uint64, nodePeers []Peer) []requestVoteResult {
	var wg sync.WaitGroup
	voteResults := make(chan requestVoteResult)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, peer := range nodePeers {
		req := &proto.RequestVoteRequest{
			Term:        nodeTerm,
			CandidateId: candidateId,
		}
		wg.Add(1)
		go func(p Peer) {
			defer wg.Done()
			resp, err := p.RequestVote(ctx, req)
			if err != nil {
				// We only halt execution on errors we do not understand. Errors from RequestVote
				// might be due to transient effect, e.g., network etc. which we do understand so
				// we do not halt here.
				log.Println("error:", err)
				voteResults <- requestVoteResult{
					term:        nodeTerm,
					voteGranted: false,
				}
				return
			}
			if resp != nil {
				voteResults <- requestVoteResult{
					term:        resp.Term,
					voteGranted: resp.VoteGranted,
				}
			}
		}(peer)
	}
	go func() {
		wg.Wait()
		close(voteResults)
	}()
	nPeers := len(nodePeers)
	results := make([]requestVoteResult, 0, nPeers)
	// TODO: Collecting all nPeer responses stalls for 5 seconds (from context.WithTimeout above)
	// if any peer doesn't respond. Consider aborting early once it's clear that we have or cannot
	// get a majority.
	for i := 0; i < nPeers; i++ {
		results = append(results, <-voteResults)
	}
	return results
}

func (node *node) runLeader(ctx context.Context) {
	heartbeatTicker := time.NewTicker(time.Duration(10) * time.Millisecond)
	select {
	case <-heartbeatTicker.C:
		ctxInner, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		node.mu.Lock()
		for _, peer := range node.peers {
			go func(p Peer) {
				req := &proto.AppendEntriesRequest{
					Term:     node.term,
					LeaderId: node.name,
				}
				resp, err := peer.AppendEntries(ctxInner, req)
				if err != nil {
					log.Println("error when receiving heartbeat:", err)
					// TODO: Return something?
				}
				if resp != nil {
					// TODO: Return resp?
				}

			}(peer)
		}
		node.mu.Unlock()
	}
}

func (node *node) Stop() {
	node.mu.Lock()
	if node.cancel != nil {
		node.cancel()
	}
	node.mu.Unlock()
}

type requestVoteResult struct {
	term        uint64
	voteGranted bool
}

func (node *node) ReceiveHeartbeat(term uint64) {
	// If we do a standard blocking send and Start has NOT been called, this method blocks forever.
	// Therefore, we do a non-blocking send.
	select {
	case node.heartbeatChan <- term:
	default:
	}
}

func (node *node) HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	node.mu.Lock()
	defer node.mu.Unlock()
	if req.Term > node.term {
		node.becomeFollower(req.Term)
		node.votedFor = req.CandidateId
		return &proto.RequestVoteResponse{
			Term:        req.Term,
			VoteGranted: true,
		}
	}
	// req.CandidateId == node.votedFor ensures idempotency if we've already voted for `peer`.
	if req.Term == node.term && (node.votedFor == "" || req.CandidateId == node.votedFor) {
		return &proto.RequestVoteResponse{
			Term:        node.term,
			VoteGranted: true,
		}
	}
	return &proto.RequestVoteResponse{
		Term:        node.term,
		VoteGranted: false,
	}
}

// Caller must do node.mu.Lock()
func (node *node) becomeFollower(term uint64) {
	node.term = term
	node.votedFor = ""
	node.state = Follower
}
