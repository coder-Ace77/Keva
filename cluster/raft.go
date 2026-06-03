package cluster

import (
	"fmt"
	"key_value_store/protocol"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

type Role int32

const (
	RoleFollower  Role = 0
	RoleCandidate Role = 1
	RoleLeader    Role = 2
)

// RaftState implements a simplified Raft consensus algorithm.
// It drives leader election and detects leader failure.
type RaftState struct {
	mu          sync.Mutex
	nodeAddr    string
	peers       []string // all cluster nodes, including self
	currentTerm uint64
	votedFor    string
	role        Role
	leaderAddr  string

	heartbeatCh chan struct{} // closed/sent to reset election timer

	// Role-change callbacks (must not hold mu when called)
	onLeader   func()
	onFollower func()
}

func NewRaftState(nodeAddr string, peers []string, onLeader, onFollower func()) *RaftState {
	return &RaftState{
		nodeAddr:    nodeAddr,
		peers:       peers,
		heartbeatCh: make(chan struct{}, 1),
		onLeader:    onLeader,
		onFollower:  onFollower,
	}
}

func (r *RaftState) Start() { go r.run() }

// ── Public methods called by the TCP server ───────────────────────────────────

func (r *RaftState) HandleHeartbeat(term uint64, leaderID string) {
	r.mu.Lock()
	if term < r.currentTerm {
		r.mu.Unlock()
		return
	}
	wasLeader := r.role == RoleLeader
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
	}
	r.leaderAddr = leaderID
	r.role = RoleFollower
	r.mu.Unlock()

	if wasLeader {
		r.onFollower()
	}
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}
}

// HandleVoteRequest decides whether to grant a vote and returns (granted, currentTerm).
func (r *RaftState) HandleVoteRequest(req *protocol.VoteRequest) (bool, uint64) {
	r.mu.Lock()

	if req.Term < r.currentTerm {
		t := r.currentTerm
		r.mu.Unlock()
		return false, t
	}

	// A leader in the same term doesn't yield its vote.
	if r.role == RoleLeader && req.Term == r.currentTerm {
		t := r.currentTerm
		r.mu.Unlock()
		return false, t
	}

	wasLeader := r.role == RoleLeader
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = ""
		r.role = RoleFollower
	}

	if r.votedFor == "" || r.votedFor == req.CandidateID {
		r.votedFor = req.CandidateID
		t := r.currentTerm
		r.mu.Unlock()
		if wasLeader {
			r.onFollower()
		}
		// Signal timer reset so we don't immediately start a competing election.
		select {
		case r.heartbeatCh <- struct{}{}:
		default:
		}
		return true, t
	}

	t := r.currentTerm
	r.mu.Unlock()
	return false, t
}

func (r *RaftState) GetLeaderAddr() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderAddr
}

func (r *RaftState) GetRole() Role {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role
}

func (r *RaftState) GetTerm() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

func (r *RaftState) GetPeers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]string, len(r.peers))
	copy(cp, r.peers)
	return cp
}

// ── Internal state machine ────────────────────────────────────────────────────

func (r *RaftState) run() {
	for {
		r.mu.Lock()
		role := r.role
		r.mu.Unlock()

		switch role {
		case RoleFollower:
			r.runFollower()
		case RoleCandidate:
			r.runCandidate()
		case RoleLeader:
			r.runLeader()
		}
	}
}

func (r *RaftState) runFollower() {
	timer := time.NewTimer(randomTimeout())
	defer timer.Stop()

	select {
	case <-r.heartbeatCh:
		// Valid heartbeat received — reset timer by returning and re-entering.
	case <-timer.C:
		r.mu.Lock()
		r.role = RoleCandidate
		r.mu.Unlock()
	}
}

func (r *RaftState) runCandidate() {
	r.mu.Lock()
	r.currentTerm++
	r.votedFor = r.nodeAddr
	term := r.currentTerm
	others := r.otherPeers()
	r.mu.Unlock()

	fmt.Printf("raft[%s]: election term=%d peers=%v\n", r.nodeAddr, term, others)

	var votes int32 = 1 // self-vote
	var wg sync.WaitGroup
	for _, peer := range others {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			if r.requestVoteRPC(p, term) {
				atomic.AddInt32(&votes, 1)
			}
		}(peer)
	}

	voteDone := make(chan struct{})
	go func() { wg.Wait(); close(voteDone) }()

	select {
	case <-voteDone:
	case <-time.After(randomTimeout()):
	case <-r.heartbeatCh:
		// A valid leader is already up — step back.
		r.mu.Lock()
		r.role = RoleFollower
		r.mu.Unlock()
		return
	}

	r.mu.Lock()
	if r.role != RoleCandidate || r.currentTerm != term {
		r.mu.Unlock()
		return // stepped down by a higher-term message
	}

	quorum := len(r.peers)/2 + 1
	got := int(atomic.LoadInt32(&votes))
	if got >= quorum {
		fmt.Printf("raft[%s]: became leader term=%d votes=%d/%d\n",
			r.nodeAddr, term, got, len(r.peers))
		r.role = RoleLeader
		r.leaderAddr = r.nodeAddr
		r.mu.Unlock()
		r.onLeader()
	} else {
		fmt.Printf("raft[%s]: election failed term=%d votes=%d/%d\n",
			r.nodeAddr, term, got, len(r.peers))
		r.role = RoleFollower
		r.mu.Unlock()
	}
}

func (r *RaftState) runLeader() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		r.mu.Lock()
		if r.role != RoleLeader {
			r.mu.Unlock()
			return
		}
		term := r.currentTerm
		others := r.otherPeers()
		r.mu.Unlock()

		select {
		case <-ticker.C:
			for _, peer := range others {
				go r.sendHeartbeatRPC(peer, term)
			}
		case <-r.heartbeatCh:
			r.mu.Lock()
			role := r.role
			r.mu.Unlock()
			if role != RoleLeader {
				return
			}
		}
	}
}

// ── RPC helpers (short-lived connections) ────────────────────────────────────

func (r *RaftState) requestVoteRPC(peer string, term uint64) bool {
	conn, err := net.DialTimeout("tcp", peer, 200*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(200 * time.Millisecond)) //nolint

	if err := sendFrame(conn, &protocol.VoteRequest{
		Term:        term,
		CandidateID: r.nodeAddr,
	}); err != nil {
		return false
	}

	msg, err := readFrame(conn)
	if err != nil {
		return false
	}
	resp, ok := msg.(*protocol.VoteResponse)
	if !ok {
		return false
	}

	// If peer has a higher term we step down.
	r.mu.Lock()
	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.role = RoleFollower
		r.votedFor = ""
	}
	r.mu.Unlock()

	return resp.Granted
}

func (r *RaftState) sendHeartbeatRPC(peer string, term uint64) {
	conn, err := net.DialTimeout("tcp", peer, heartbeatInterval)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(heartbeatInterval)) //nolint

	_ = sendFrame(conn, &protocol.Heartbeat{
		Term:     term,
		LeaderID: r.nodeAddr,
	})
	_, _ = readFrame(conn) // drain ack so the peer can close cleanly
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (r *RaftState) otherPeers() []string {
	var out []string
	for _, p := range r.peers {
		if p != r.nodeAddr {
			out = append(out, p)
		}
	}
	return out
}

func randomTimeout() time.Duration {
	d := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(d)))
}
