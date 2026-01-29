package kvstore

type NodeState string

const (
	Follower NodeState = "Follower"
	Candidate = "Candidate"
	Leader = "Leader"
)

type node struct {
	name string
	state NodeState
	term int
}

func NewRaftNode(name string) *node {
	return &node{
		name: name,
		state: Follower,
		term: 0,
	}
}

func (node *node) State() NodeState {
	return node.state
}

func (node *node) CurrentTerm() int {
	return node.term
}
