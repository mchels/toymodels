package kvstore

import (
	"bytes"
	"context"
	"fmt"
	"kvstore/proto"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewRaftNode(t *testing.T) {
	node := NewRaftNode("node1", 0, 0)

	if node.State() != Follower {
		t.Errorf("new node should be Follower, got %v", node.State())
	}
	if node.CurrentTerm() != 0 {
		t.Errorf("new node should have term 0, got %d", node.CurrentTerm())
	}
}

func TestRequestVote_GrantsVote(t *testing.T) {
	node := NewRaftNode("node1", 0, 0)

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
	node := NewRaftNode("node1", 0, 0)

	// First vote granted
	node.HandleRequestVote(&proto.RequestVoteRequest{Term: 1, CandidateId: "node2"})

	// Second vote denied (already voted this term)
	resp := node.HandleRequestVote(&proto.RequestVoteRequest{Term: 1, CandidateId: "node3"})

	if resp.VoteGranted {
		t.Error("should deny vote, already voted this term")
	}
}

func TestStop_TerminatesGoroutine(t *testing.T) {
	node := NewRaftNode("node1", 150*time.Millisecond, 0)
	node.Start()

	time.Sleep(50 * time.Millisecond) // Let it start
	node.Stop()
	time.Sleep(200 * time.Millisecond) // Give it time to stop

	// Should still be Follower (election didn't fire)
	if node.State() != Follower {
		t.Errorf("stop should prevent election, got %v", node.State())
	}
}

func TestHeartbeat_ResetsElectionTimer(t *testing.T) {
	node := NewRaftNode("node1", 100*time.Millisecond, 0)
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
	node := NewRaftNode("node1", 50*time.Millisecond, 0)
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

// =============================================================================
// Task 4: Candidate Requests Votes and Becomes Leader
// =============================================================================

// Mock peer for testing
type mockPeer struct {
	name        NodeName
	voteGranted bool
	term        uint64
	appendCalls atomic.Int64
	mu          sync.Mutex
}

func (m *mockPeer) Name() NodeName {
	return m.name
}

func (m *mockPeer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &proto.RequestVoteResponse{Term: m.term, VoteGranted: m.voteGranted}, nil
}

func (m *mockPeer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	m.appendCalls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return &proto.AppendEntriesResponse{Term: m.term, Success: true}, nil
}

// Slow peer for testing concurrency. Tracks the maximum number of concurrent
// in-flight RequestVote calls observed at this peer.
type slowMockPeer struct {
	name        NodeName
	delay       time.Duration
	voteGranted bool
	term        uint64
	appendCalls atomic.Int64
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	mu          sync.Mutex
}

func (m *slowMockPeer) Name() NodeName {
	return m.name
}

func (m *slowMockPeer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	n := m.inFlight.Add(1)
	for {
		old := m.maxInFlight.Load()
		if n <= old || m.maxInFlight.CompareAndSwap(old, n) {
			break
		}
	}
	time.Sleep(m.delay)
	m.inFlight.Add(-1)
	return &proto.RequestVoteResponse{
		Term:        m.term,
		VoteGranted: m.voteGranted,
	}, nil
}

func (m *slowMockPeer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	m.appendCalls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return &proto.AppendEntriesResponse{Term: m.term, Success: true}, nil
}

func TestCandidate_WinsMajority_BecomesLeader(t *testing.T) {
	node := NewRaftNode("node1", 50*time.Millisecond, 0)

	// Two peers that will grant votes
	peer1 := &mockPeer{voteGranted: true, term: 1}
	peer2 := &mockPeer{voteGranted: true, term: 1}
	node.SetPeers([]Peer{peer1, peer2}) // 3-node cluster

	node.Start()
	defer node.Stop()

	time.Sleep(200 * time.Millisecond)

	if node.State() != Leader {
		t.Errorf("should become Leader with majority, got %v", node.State())
	}
}

func TestCandidate_NoMajority_StaysCandidate(t *testing.T) {
	node := NewRaftNode("node1", 50*time.Millisecond, 0)

	// Two peers that deny votes
	peer1 := &mockPeer{voteGranted: false, term: 1}
	peer2 := &mockPeer{voteGranted: false, term: 1}
	node.SetPeers([]Peer{peer1, peer2})

	node.Start()
	defer node.Stop()

	time.Sleep(200 * time.Millisecond)

	if node.State() != Candidate {
		t.Errorf("should stay Candidate without majority, got %v", node.State())
	}
}

func TestCandidate_PartialVotes_NeedsMajority(t *testing.T) {
	node := NewRaftNode("node1", 50*time.Millisecond, 0)

	// 5-node cluster: need 3 votes for majority
	peer1 := &mockPeer{voteGranted: true, term: 1}  // grants
	peer2 := &mockPeer{voteGranted: false, term: 1} // denies
	peer3 := &mockPeer{voteGranted: false, term: 1} // denies
	peer4 := &mockPeer{voteGranted: false, term: 1} // denies
	node.SetPeers([]Peer{peer1, peer2, peer3, peer4})

	node.Start()
	defer node.Stop()

	time.Sleep(200 * time.Millisecond)

	// 2 votes (self + peer1) out of 5 - not majority
	if node.State() != Candidate {
		t.Errorf("should stay Candidate with 2/5 votes, got %v", node.State())
	}
}

func TestCandidate_VotesRequestedConcurrently(t *testing.T) {
	node := NewRaftNode("node1", 50*time.Millisecond, 0)

	// Peers that take time to respond. The delay must exceed the worst-case
	// election-timer fire (2*electionTimeout with jitter) so that if calls
	// were sequential, the second peer's call would not have started yet
	// while the first is still in flight.
	slowPeer1 := &slowMockPeer{delay: 200 * time.Millisecond, voteGranted: true, term: 1}
	slowPeer2 := &slowMockPeer{delay: 200 * time.Millisecond, voteGranted: true, term: 1}
	node.SetPeers([]Peer{slowPeer1, slowPeer2})

	node.Start()
	defer node.Stop()

	// Wait long enough for the timer to fire and both peers to be in flight
	// concurrently (max election fire is 2*50ms; both calls should be active
	// well before either returns at 200ms).
	time.Sleep(150 * time.Millisecond)

	if slowPeer1.maxInFlight.Load() < 1 || slowPeer2.maxInFlight.Load() < 1 {
		t.Fatalf("peers were not called: p1=%d p2=%d",
			slowPeer1.maxInFlight.Load(), slowPeer2.maxInFlight.Load())
	}
	// Both peers in flight at the same instant is what concurrent dispatch means.
	total := slowPeer1.maxInFlight.Load() + slowPeer2.maxInFlight.Load()
	if total < 2 {
		t.Errorf("votes appear sequential: max in-flight per peer p1=%d p2=%d (sum=%d, want >=2)",
			slowPeer1.maxInFlight.Load(), slowPeer2.maxInFlight.Load(), total)
	}
}

func TestSingleNodeCluster_BecomesLeader(t *testing.T) {
	node := NewRaftNode("node1", 50*time.Millisecond, 0)

	node.SetPeers([]Peer{}) // No peers

	node.Start()
	defer node.Stop()

	time.Sleep(200 * time.Millisecond)

	if node.State() != Leader {
		t.Errorf("single node should become Leader, got %v", node.State())
	}
}

// =============================================================================
// Task 5
// =============================================================================

func TestReceiveHeartbeat_BeforeStart_DoesNotBlock(t *testing.T) {
	node := NewRaftNode("node1", 0, 0)
	done := make(chan struct{})
	go func() {
		node.ReceiveHeartbeat(0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ReceiveHeartbeat blocked when Start had not been called")
	}
}

func TestLeader_DoesNotStartNewElection(t *testing.T) {
	node := NewRaftNode("node1", 50*time.Millisecond, 0)
	node.SetPeers([]Peer{
		&mockPeer{voteGranted: true, term: 1},
		&mockPeer{voteGranted: true, term: 1},
	})
	node.Start()
	defer node.Stop()

	// Wait until Leader.
	deadline := time.After(500 * time.Millisecond)
	for node.State() != Leader {
		select {
		case <-deadline:
			t.Fatal("never became Leader")
		case <-time.After(5 * time.Millisecond):
		}
	}
	leaderTerm := node.CurrentTerm()

	// Sleep well past several election timeouts.
	time.Sleep(300 * time.Millisecond)

	if node.State() != Leader {
		t.Errorf("Leader should not step down without external cause, got %v", node.State())
	}
	if node.CurrentTerm() != leaderTerm {
		t.Errorf("Leader term advanced from %d to %d (phantom election)", leaderTerm, node.CurrentTerm())
	}
}

func TestCandidate_StepsDown_OnHigherTermResponse(t *testing.T) {
	node := NewRaftNode("node1", 50*time.Millisecond, 0)
	node.SetPeers([]Peer{
		&mockPeer{voteGranted: false, term: 99},
		&mockPeer{voteGranted: false, term: 99},
	})
	node.Start()
	defer node.Stop()

	// Poll for the moment of step-down. The candidate may immediately start a
	// new election after adopting term 99, so we cannot assert on a fixed
	// snapshot — only that the higher term was observed and adopted.
	// Reaching term >= 99 within this window is only possible via the
	// becomeFollower path in runCandidate (term increments per election are
	// far too slow to reach 99 otherwise).
	deadline := time.After(500 * time.Millisecond)
	for node.CurrentTerm() < 99 {
		select {
		case <-deadline:
			t.Fatalf("never adopted peer term 99, current term %d", node.CurrentTerm())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func TestRequestVote_SameTerm_DifferentCandidate_Denied(t *testing.T) {
	node := NewRaftNode("node1", 0, 0)
	node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "node2"})
	resp := node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "node3"})
	if resp.VoteGranted {
		t.Error("should deny: already voted for node2 in term 5")
	}
}

func TestRequestVote_SameTerm_SameCandidate_Granted(t *testing.T) {
	node := NewRaftNode("node1", 0, 0)
	node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "node2"})
	resp := node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "node2"})
	if !resp.VoteGranted {
		t.Error("re-vote for same candidate at same term must be idempotent")
	}
}

func TestElectionTimeout_IsRandomized(t *testing.T) {
	const N = 20
	base := 80 * time.Millisecond
	fires := make([]time.Duration, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n := NewRaftNode("n", base, 0)
			n.SetPeers([]Peer{&mockPeer{voteGranted: false, term: 0}, &mockPeer{voteGranted: false, term: 0}})
			start := time.Now()
			n.Start()
			defer n.Stop()
			for n.State() == Follower {
				time.Sleep(2 * time.Millisecond)
			}
			fires[i] = time.Since(start)
		}(i)
	}
	wg.Wait()
	// All firings must be >= base, and not all identical.
	var min, max time.Duration = fires[0], fires[0]
	for _, f := range fires {
		if f < base {
			t.Errorf("fired before configured minimum: %v < %v", f, base)
		}
		if f < min {
			min = f
		}
		if f > max {
			max = f
		}
	}
	if max-min < 5*time.Millisecond {
		t.Errorf("election timeouts look fixed (spread %v); expected randomization", max-min)
	}
}

func TestNoEmptyName(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("Empty name construction did not raise panic")
		}
	}()

	NewRaftNode("", 0, 0)
}

// =============================================================================
// Task 6
// =============================================================================

func TestLeader_SendsHeartbeats(t *testing.T) {
	node := NewRaftNode("node1", 250*time.Millisecond, 50*time.Millisecond)
	p1 := &mockPeer{voteGranted: true, term: 1}
	p2 := &mockPeer{voteGranted: true, term: 1}
	node.SetPeers([]Peer{p1, p2})
	node.Start()
	defer node.Stop()

	// Wait for Leader.
	deadline := time.After(1 * time.Second)
	for node.State() != Leader {
		select {
		case <-deadline:
			t.Fatal("never became Leader")
		case <-time.After(5 * time.Millisecond):
		}
	}
	// Within ~3 heartbeat intervals, expect >= 3 AppendEntries calls per peer.
	time.Sleep(180 * time.Millisecond)
	if p1.appendCalls.Load() < 3 || p2.appendCalls.Load() < 3 {
		t.Errorf("expected >=3 heartbeats per peer, got p1=%d p2=%d",
			p1.appendCalls.Load(), p2.appendCalls.Load())
	}
}

func TestLeader_StepsDown_OnHigherTermAppendResponse(t *testing.T) {
	node := NewRaftNode("node1", 100*time.Millisecond, 25*time.Millisecond)
	// Peers grant votes at term 1, then start replying with a higher term.
	p1 := &mockPeer{voteGranted: true, term: 1}
	p2 := &mockPeer{voteGranted: true, term: 1}
	node.SetPeers([]Peer{p1, p2})
	node.Start()
	defer node.Stop()

	for node.State() != Leader {
		time.Sleep(5 * time.Millisecond)
	}
	// Bump peer term to force step-down on next heartbeat response.
	p1.mu.Lock()
	p1.term = 99
	p1.mu.Unlock()
	p2.mu.Lock()
	p2.term = 99
	p2.mu.Unlock()

	deadline := time.After(500 * time.Millisecond)
	for node.State() == Leader {
		select {
		case <-deadline:
			t.Fatal("Leader did not step down on higher-term response")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if node.State() != Follower {
		t.Errorf("expected Follower after step-down, got %v", node.State())
	}
	if node.CurrentTerm() != 99 {
		t.Errorf("expected term 99 after step-down, got %d", node.CurrentTerm())
	}
}

func TestFollower_ResetsTimer_OnAppendEntries(t *testing.T) {
	node := NewRaftNode("node1", 100*time.Millisecond, 25*time.Millisecond)
	node.SetPeers([]Peer{&mockPeer{}, &mockPeer{}})
	node.Start()
	defer node.Stop()

	// Heartbeat faster than election timeout for 300ms total.
	for i := 0; i < 6; i++ {
		time.Sleep(50 * time.Millisecond)
		node.HandleAppendEntries(&proto.AppendEntriesRequest{Term: 1, LeaderId: "leader"})
	}
	if node.State() != Follower {
		t.Errorf("AppendEntries should keep Follower, got %v", node.State())
	}
	if node.CurrentTerm() != 1 {
		t.Errorf("expected term 1 after AppendEntries, got %d", node.CurrentTerm())
	}
}

func TestCandidate_StepsDown_OnAppendEntries(t *testing.T) {
	node := NewRaftNode("node1", 50*time.Millisecond, 25*time.Millisecond)
	// Peers deny votes so node stays Candidate.
	node.SetPeers([]Peer{
		&mockPeer{voteGranted: false, term: 0},
		&mockPeer{voteGranted: false, term: 0},
	})
	node.Start()
	defer node.Stop()

	for node.State() != Candidate {
		time.Sleep(5 * time.Millisecond)
	}
	candidateTerm := node.CurrentTerm()

	// A leader at the same term sends AppendEntries.
	resp := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term: candidateTerm, LeaderId: "leader",
	})
	if !resp.Success {
		t.Errorf("AppendEntries at same term should be accepted by Candidate")
	}
	// Give runCandidate a moment to observe the state change.
	time.Sleep(20 * time.Millisecond)
	if node.State() != Follower {
		t.Errorf("Candidate should step down on AppendEntries, got %v", node.State())
	}
}

func TestHandleAppendEntries_RejectsLowerTerm(t *testing.T) {
	node := NewRaftNode("node1", 250*time.Millisecond, 50*time.Millisecond)
	node.HandleRequestVote(&proto.RequestVoteRequest{Term: 5, CandidateId: "x"})
	resp := node.HandleAppendEntries(&proto.AppendEntriesRequest{Term: 4, LeaderId: "stale"})
	if resp.Success {
		t.Error("AppendEntries from lower term must be rejected")
	}
	if resp.Term != 5 {
		t.Errorf("response should carry currentTerm=5, got %d", resp.Term)
	}
}

// =============================================================================
// Task 7
// =============================================================================

type recordingPeer struct {
	name        NodeName
	mu          sync.Mutex
	term        uint64
	voteGranted bool
	// Reply policy: if respond returns nil, fall back to {term, success: true}.
	respond func(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse
	// History.
	appendCalls []*proto.AppendEntriesRequest
}

func (m *recordingPeer) Name() NodeName {
	return m.name
}

func (m *recordingPeer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &proto.RequestVoteResponse{Term: m.term, VoteGranted: m.voteGranted}, nil
}

func (m *recordingPeer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendCalls = append(m.appendCalls, req)
	if m.respond != nil {
		if r := m.respond(req); r != nil {
			return r, nil
		}
	}
	return &proto.AppendEntriesResponse{Term: m.term, Success: true}, nil
}

func newLeaderForTest(t *testing.T) node {
	node := NewRaftNode("node1", 50*time.Millisecond, 0)

	// Two peers that will grant votes
	peer1 := &mockPeer{voteGranted: true, term: 1}
	peer2 := &mockPeer{voteGranted: true, term: 1}
	node.SetPeers([]Peer{peer1, peer2}) // 3-node cluster
	node.Start()
	t.Cleanup(node.Stop)
	for {
		if node.State() == Leader {
			return *node
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func newLeaderForTestWithPeers(t *testing.T, peer1 *recordingPeer, peer2 *recordingPeer) *node {
	node := NewRaftNode("node1", 50*time.Millisecond, 0)
	node.SetPeers([]Peer{peer1, peer2})
	node.Start()
	t.Cleanup(node.Stop)
	for {
		if node.State() == Leader {
			return node
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func peerSawEntry(peer *recordingPeer, want []byte) bool {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	for _, appendCall := range peer.appendCalls {
		for _, logEntry := range appendCall.Entries {
			if bytes.Equal(logEntry.Command, want) {
				return true
			}
		}
	}
	return false
}

func peerSawEntries(peer *recordingPeer, expectedEntries ...string) bool {
	for _, expected := range expectedEntries {
		if !peerSawEntry(peer, []byte(expected)) {
			return false
		}
	}
	return true
}

func TestPropose_NonLeader_Rejected(t *testing.T) {
	node := NewRaftNode("n1", 250*time.Millisecond, 50*time.Millisecond)
	defer node.Stop()
	if _, ok := node.Propose([]byte("x")); ok {
		t.Error("Follower must not accept Propose")
	}
}

func TestPropose_Leader_AppendsToLog(t *testing.T) {
	node := newLeaderForTest(t)
	node.mu.Lock()
	defer node.mu.Unlock()
	idx, ok := node.propose([]byte("set x=1"))
	if !ok || idx != 1 {
		t.Fatalf("Propose returned (%d,%v), want (1,true)", idx, ok)
	}
	if got := LogLen(node.log); got != 1 {
		t.Errorf("leader log len = %d, want 1", got)
	}
}

func TestLeader_ReplicatesEntries_ToPeers(t *testing.T) {
	p1 := &recordingPeer{voteGranted: true, term: 1}
	p2 := &recordingPeer{voteGranted: true, term: 1}
	node := newLeaderForTestWithPeers(t, p1, p2)

	node.Propose([]byte("a"))
	node.Propose([]byte("b"))

	// Within a few heartbeat intervals, each peer should see entries [a,b] in some call.
	time.Sleep(120 * time.Millisecond)
	if !peerSawEntries(p1, "a", "b") || !peerSawEntries(p2, "a", "b") {
		t.Error("A peer didn't see all entries")
	}
}

// func TestFollower_AcceptsAppend_WithMatchingPrev(t *testing.T) {
// 	node := NewRaftNode("n1", 250*time.Millisecond, 50*time.Millisecond)
// 	// Put the follower in a state where its log already has one entry at term 1.
// 	seedLog(node, []proto.LogEntry{{Term: 1, Index: 1, Command: []byte("a")}})
// 	setTerm(node, 1)

// 	resp := node.HandleAppendEntries(&proto.AppendEntriesRequest{
// 		Term: 1, LeaderId: "L",
// 		PrevLogIndex: 1, PrevLogTerm: 1,
// 		Entries: []*proto.LogEntry{{Term: 1, Index: 2, Command: []byte("b")}},
// 	})
// 	if !resp.Success {
// 		t.Fatal("expected Success on matching prev")
// 	}
// 	if LogLen(node.log) != 2 {
// 		t.Errorf("log len = %d, want 2", LogLen(node.log))
// 	}
// }

func TestFollower_RejectsAppend_WhenPrevMissing(t *testing.T) {
	node := NewRaftNode("n1", 250*time.Millisecond, 50*time.Millisecond)
	node.SetTerm(1)
	resp := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term: 1, LeaderId: "L",
		PrevLogIndex: 5, PrevLogTerm: 1, // follower has no entry at index 5
		Entries: []*proto.LogEntry{{Term: 1, Index: 6, Command: []byte("x")}},
	})
	if resp.Success {
		t.Error("must reject when PrevLogIndex is missing in follower log")
	}
}

// func TestFollower_TruncatesConflictingTail(t *testing.T) {
// 	node := NewRaftNode("n1", 250*time.Millisecond, 50*time.Millisecond)
// 	seedLog(node, []proto.LogEntry{
// 		{Term: 1, Index: 1, Command: []byte("a")},
// 		{Term: 1, Index: 2, Command: []byte("b")},
// 		{Term: 2, Index: 3, Command: []byte("stale")}, // wrong-term tail
// 	})
// 	setTerm(node, 3)
// 	resp := node.HandleAppendEntries(&proto.AppendEntriesRequest{
// 		Term: 3, LeaderId: "L",
// 		PrevLogIndex: 2, PrevLogTerm: 1,
// 		Entries: []*proto.LogEntry{{Term: 3, Index: 3, Command: []byte("good")}},
// 	})
// 	if !resp.Success {
// 		t.Fatal("expected Success on matching prev=2")
// 	}
// 	if cmd := node.EntryAt(3); string(cmd) != "good" {
// 		t.Errorf("index 3 = %q, want %q (truncate-and-replace)", cmd, "good")
// 	}
// 	if LogLen(node.log) != 3 {
// 		t.Errorf("log len = %d, want 3 (no spurious extension)", LogLen(node.log))
// 	}
// }

// func TestFollower_AdvancesCommit_FromLeaderCommit(t *testing.T) {
// 	node := NewRaftNode("n1", 250*time.Millisecond, 50*time.Millisecond)
// 	seedLog(node, []proto.LogEntry{
// 		{Term: 1, Index: 1, Command: []byte("a")},
// 		{Term: 1, Index: 2, Command: []byte("b")},
// 	})
// 	setTerm(node, 1)
// 	node.HandleAppendEntries(&proto.AppendEntriesRequest{
// 		Term: 1, LeaderId: "L",
// 		PrevLogIndex: 2, PrevLogTerm: 1,
// 		Entries:      nil,
// 		LeaderCommit: 5, // larger than log length on purpose
// 	})
// 	if got := node.CommitIndex(); got != 2 {
// 		t.Errorf("commitIndex = %d, want 2 (clamped to log length)", got)
// 	}
// }

// func TestLeader_CommitsAfterMajorityReplication(t *testing.T) {
// 	p1 := &recordingPeer{voteGranted: true, term: 1}
// 	p2 := &recordingPeer{voteGranted: true, term: 1}
// 	node := newLeaderForTestWithPeers(t, p1, p2)

// 	// p1 acks normally; p2 keeps rejecting (simulate slow follower).
// 	p2.mu.Lock()
// 	p2.respond = func(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
// 		return &proto.AppendEntriesResponse{Term: req.Term, Success: false}
// 	}
// 	p2.mu.Unlock()

// 	node.Propose([]byte("only-needs-one-other"))
// 	waitFor(t, 500*time.Millisecond, func() bool { return node.CommitIndex() == 1 })
// 	if node.CommitIndex() != 1 {
// 		t.Errorf("commitIndex = %d, want 1 (self + p1 = majority)", node.CommitIndex())
// 	}
// }

func TestLeader_DecrementsNextIndex_OnReject(t *testing.T) {
	p1 := &recordingPeer{name: "p1", voteGranted: true, term: 1}
	// Reject every AppendEntries with PrevLogIndex >= 2; succeed at 1.
	p1.respond = func(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
		if req.PrevLogIndex >= 2 {
			return &proto.AppendEntriesResponse{Term: req.Term, Success: false}
		}
		return nil // fall through to default {Success: true}
	}
	p2 := &recordingPeer{name: "p2", voteGranted: true, term: 1}
	node := newLeaderForTestWithPeers(t, p1, p2)

	node.Propose([]byte("a"))
	node.Propose([]byte("b"))
	node.Propose([]byte("c"))

	// Eventually p1 should receive a call with PrevLogIndex == 1 (i.e., leader walked back).
	time.Sleep(500 * time.Millisecond)
	// waitFor(t, 500*time.Millisecond, func() bool {
	{
		p1.mu.Lock()
		defer p1.mu.Unlock()
		for _, r := range p1.appendCalls {
			fmt.Println(r)
			if r.PrevLogIndex == 1 {
				// return true
			}
		}
		t.Error("error", p1)
		// return false
	}
	// })
}

// func TestRequestVote_DeniesStaleCandidate(t *testing.T) {
// 	node := NewRaftNode("n1", 250*time.Millisecond, 50*time.Millisecond)
// 	seedLog(node, []proto.LogEntry{
// 		{Term: 2, Index: 1, Command: []byte("a")},
// 		{Term: 2, Index: 2, Command: []byte("b")},
// 	})
// 	setTerm(node, 2)
// 	// Candidate has higher term but its log ends at term 1, index 1 — strictly stale.
// 	resp := node.HandleRequestVote(&proto.RequestVoteRequest{
// 		Term: 3, CandidateId: "stale",
// 		LastLogIndex: 1, LastLogTerm: 1,
// 	})
// 	if resp.VoteGranted {
// 		t.Error("must deny vote: candidate log is staler than voter's")
// 	}
// }
