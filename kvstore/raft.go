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
	Name() NodeName

	RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error)

	AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error)
}

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

type NodeName string

type node struct {
	name              string
	state             NodeState
	term              uint64
	votedFor          string
	electionTimeout   time.Duration
	heartbeatInterval time.Duration
	heartbeatChan     chan uint64
	log               []LogEntry
	cancel            context.CancelFunc
	peers             []Peer
	mu                sync.Mutex
	nextIndex         map[NodeName]uint64
	matchIndex        map[NodeName]uint64
	runDone           chan struct{}
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
		// Insert a sentinel logentry in index 0 to make log entries 1-based.
		log:        []LogEntry{LogEntry{0, 0, []byte{}}},
		nextIndex:  nil,
		matchIndex: nil,
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

func (node *node) SetTerm(term uint64) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.term = term
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
		prev := node.runDone
		node.mu.Unlock()
		if prev != nil {
			<-prev // Wait for the previous run to stop before starting a new one.
		}
		node.mu.Lock()
	}
	runDone := make(chan struct{})
	node.cancel = cancel
	node.runDone = runDone
	node.mu.Unlock()
	go func() {
		defer close(runDone)
		node.run(ctx)
	}()
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
			node.becomeCandidate()
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
		node.becomeLeader()
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
	select {
	case <-time.After(node.heartbeatInterval):
		node.mu.Lock()
		nodeName := node.name
		nodeTerm := node.term
		nodePeers := slices.Clone(node.peers)
		// TODO: Consider cloning node.nextIndex to avoid mutation while using nextIndex.
		nextIndex := node.nextIndex
		nodeLog := slices.Clone(node.log)
		node.mu.Unlock()
		appendEntriesResults := requestAppendEntries(nodeName, nodeTerm, nodePeers, nextIndex, nodeLog)
		node.mu.Lock()
		maxTerm := node.term
		for _, result := range appendEntriesResults {
			if result.term > maxTerm {
				maxTerm = result.term
			}
		}
		// Check that we didn't change term and state since we unlocked above to send heartbeats.
		if maxTerm > node.term && node.state == Leader && node.term == nodeTerm {
			node.state = Follower
			node.term = maxTerm
			node.mu.Unlock()
			return
		}
		node.mu.Unlock()
	case <-ctx.Done():
	}
}

func requestAppendEntries(nodeName string, nodeTerm uint64, nodePeers []Peer, nextIndex map[NodeName]uint64, nodeLog []LogEntry) []appendEntriesResult {
	var wg sync.WaitGroup
	appendEntriesChan := make(chan appendEntriesResult)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, peer := range nodePeers {
		prevLogIndex := nextIndex[peer.Name()] - 1
		nEntries := len(nodeLog[nextIndex[peer.Name()]:])
		pbEntries := make([]*proto.LogEntry, 0, nEntries)
		for _, logEntry := range nodeLog[nextIndex[peer.Name()]:] {
			pbEntries = append(pbEntries,
				&proto.LogEntry{
					Term:    logEntry.Term,
					Index:   logEntry.Index,
					Command: logEntry.Command,
				})
		}
		req := &proto.AppendEntriesRequest{
			Term:         nodeTerm,
			LeaderId:     nodeName,
			Entries:      pbEntries,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  nodeLog[prevLogIndex].Term,
		}
		wg.Add(1)
		go func(p Peer) {
			defer wg.Done()
			resp, err := p.AppendEntries(ctx, req)
			if err != nil {
				log.Println("error when receiving heartbeat:", err)
				// TODO: Return something?
			}
			if resp != nil {
				appendEntriesChan <- appendEntriesResult{
					term:    resp.Term,
					success: resp.Success,
				}
			}
		}(peer)
	}
	go func() {
		wg.Wait()
		close(appendEntriesChan)
	}()
	nPeers := len(nodePeers)
	results := make([]appendEntriesResult, 0, nPeers)
	// TODO: Collecting all nPeer responses stalls for 5 seconds (from context.WithTimeout above)
	// if any peer doesn't respond. Consider aborting early once it's clear that we have or cannot
	// get a majority.
	for i := 0; i < nPeers; i++ {
		results = append(results, <-appendEntriesChan)
	}
	return results
}

type appendEntriesResult struct {
	term    uint64
	success bool
}

func (node *node) Stop() {
	node.mu.Lock()
	cancel := node.cancel
	runDone := node.runDone
	node.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if runDone != nil {
		<-runDone
	}
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

func HasMatchingEntry(nodeLog []LogEntry, idx uint64, term uint64) bool {
	return int(idx) <= LogLen(nodeLog) && nodeLog[idx].Term == term
}

func LogLen(nodeLog []LogEntry) int {
	// Subtract 1 because of sentinel value at position 0.
	return len(nodeLog) - 1
}

func (node *node) HandleAppendEntries(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
	node.mu.Lock()
	currentTerm := node.term
	if req.Term < currentTerm {
		node.mu.Unlock()
		return &proto.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
	} else if (req.Term > currentTerm) || (req.Term == currentTerm && node.state == Candidate) {
		node.becomeFollower(req.Term)
	} else if req.Term == currentTerm && node.state == Leader {
		panic("Received heartbeat at same term while leader. This should never happen.")
	} else if req.PrevLogIndex > 0 && !HasMatchingEntry(node.log, req.PrevLogIndex, req.Term) {
		return &proto.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
	}
	// Reset currentTerm since becomeFollower may have changed term.
	currentTerm = node.term
	node.mu.Unlock()
	node.ReceiveHeartbeat(currentTerm)
	return &proto.AppendEntriesResponse{
		Term:    currentTerm,
		Success: true,
	}
}

// Caller must do node.mu.Lock()
func (node *node) becomeFollower(term uint64) {
	node.term = term
	node.votedFor = ""
	node.state = Follower
	node.nextIndex = nil
	node.matchIndex = nil
}

func (node *node) Propose(cmd []byte) (index uint64, ok bool) {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.propose(cmd)
}

// Caller must do node.mu.Lock()
func (node *node) propose(cmd []byte) (index uint64, ok bool) {
	if node.state != Leader {
		return 0, false
	}
	newIndex := uint64(LogLen(node.log) + 1)
	node.log = append(node.log, LogEntry{Term: node.term, Index: newIndex, Command: cmd})
	return newIndex, true
}

// Caller must do node.mu.Lock()
func (node *node) becomeLeader() {
	node.state = Leader
	node.nextIndex = make(map[NodeName]uint64)
	node.matchIndex = make(map[NodeName]uint64)
	for _, peer := range node.peers {
		node.nextIndex[peer.Name()] = uint64(LogLen(node.log)) + 1
		node.matchIndex[peer.Name()] = 0
	}
}

// Caller must do node.mu.Lock()
func (node *node) becomeCandidate() {
	node.state = Candidate
	node.term = node.term + 1
	node.votedFor = node.name
	node.nextIndex = nil
	node.matchIndex = nil
}
