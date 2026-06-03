package cluster

import (
	"key_value_store/protocol"
	"key_value_store/store"
	"sync"
	"time"
)

// Node wraps a KVStore and participates in Raft consensus.
// It satisfies engine.Store structurally.
type Node struct {
	mu         sync.Mutex
	db         *store.KVStore
	defaultTTL time.Duration
	raft       *RaftState
	leader     *Leader // non-nil only while this node is the leader
}

// NewNode creates a node that will self-elect via Raft.
// peers must include nodeAddr itself.
func NewNode(db *store.KVStore, defaultTTL time.Duration, nodeAddr string, peers []string) *Node {
	n := &Node{db: db, defaultTTL: defaultTTL}
	n.raft = NewRaftState(nodeAddr, peers, n.becomeLeader, n.becomeFollower)
	return n
}

// Start launches the Raft goroutine and the background replication loop.
func (n *Node) Start() {
	n.raft.Start()
	go RunFollower(n.raft, n.db, n.raft.nodeAddr)
}

// ForceLeader immediately promotes this node to leader without Raft.
// Used for standalone (single-node) mode.
func (n *Node) ForceLeader() {
	n.mu.Lock()
	n.leader = newLeader()
	n.mu.Unlock()
}

// ── engine.Store interface ────────────────────────────────────────────────────

func (n *Node) Set(key string, value []byte) error {
	expiredAt := time.Now().Add(n.defaultTTL)
	if err := n.db.SetWithExpiry(key, value, expiredAt); err != nil {
		return err
	}
	n.mu.Lock()
	l := n.leader
	n.mu.Unlock()
	if l != nil {
		l.BroadcastSet(key, value, expiredAt.UnixNano())
	}
	return nil
}

func (n *Node) Get(key string) ([]byte, error) {
	return n.db.Get(key)
}

func (n *Node) Delete(key string) error {
	if err := n.db.Delete(key); err != nil {
		return err
	}
	n.mu.Lock()
	l := n.leader
	n.mu.Unlock()
	if l != nil {
		l.BroadcastDelete(key)
	}
	return nil
}

// ── Raft integration ─────────────────────────────────────────────────────────

func (n *Node) IsLeader() bool {
	return n.raft.GetRole() == RoleLeader
}

func (n *Node) Leader() *Leader {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

func (n *Node) HandleHeartbeat(term uint64, leaderID string) {
	n.raft.HandleHeartbeat(term, leaderID)
}

func (n *Node) HandleVoteRequest(req *protocol.VoteRequest) (granted bool, term uint64) {
	return n.raft.HandleVoteRequest(req)
}

func (n *Node) GetTerm() uint64 { return n.raft.GetTerm() }

// GetTopology returns the current cluster view for topology requests.
func (n *Node) GetTopology() *protocol.TopologyResponse {
	leaderAddr := n.raft.GetLeaderAddr()
	peers := n.raft.GetPeers()

	nodes := make([]protocol.NodeInfo, 0, len(peers))
	for _, addr := range peers {
		role := protocol.TopologyRoleFollower
		if addr == leaderAddr {
			role = protocol.TopologyRoleLeader
		}
		nodes = append(nodes, protocol.NodeInfo{
			Address: addr,
			Role:    role,
			NodeID:  addr,
		})
	}
	return &protocol.TopologyResponse{LeaderAddr: leaderAddr, Nodes: nodes}
}

// ── Role-change callbacks (called by RaftState, must not hold n.mu) ──────────

func (n *Node) becomeLeader() {
	n.mu.Lock()
	n.leader = newLeader()
	n.mu.Unlock()
}

func (n *Node) becomeFollower() {
	n.mu.Lock()
	n.leader = nil
	n.mu.Unlock()
}
