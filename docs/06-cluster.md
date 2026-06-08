# Package: cluster

**Files:** `cluster/node.go`, `cluster/raft.go`, `cluster/leader.go`, `cluster/follower.go`, `cluster/wire.go`

The cluster package implements distributed coordination: Raft-based leader election, leader-to-follower replication streaming, and follower reconnect logic.

---

## Node — the glue struct

```go
type Node struct {
    mu         sync.Mutex
    db         *store.KVStore
    defaultTTL time.Duration
    raft       *RaftState
    leader     *Leader  // non-nil only while this node is the leader
}
```

`Node` is the bridge between the TCP server and the store. It implements `engine.Store` (`Set`, `Get`, `Delete`), so the engine can call it without knowing about Raft. When `Set` or `Delete` is called on a leader node, it writes locally and broadcasts to followers. On a non-leader, it just writes locally (followers receive writes only via replication).

### Standalone mode

```go
func (n *Node) ForceLeader() {
    n.mu.Lock()
    n.leader = newLeader()
    n.mu.Unlock()
}
```

Skips Raft entirely. The node is immediately a leader with no followers. Used when `KV_NODE_ADDR` or `KV_PEERS` is not configured.

### Cluster mode

```go
func (n *Node) Start() {
    n.raft.Start()                              // launches Raft goroutine
    go RunFollower(n.raft, n.db, n.raft.nodeAddr) // runs follower sync loop
}
```

Both the Raft goroutine and the follower sync goroutine run at all times. The follower goroutine is a no-op when the node is the leader (it just sleeps and checks again).

### Node.Set — the critical path

```go
func (n *Node) Set(key string, value []byte, ttl time.Duration) error {
    expiredAt := time.Now().Add(ttl)
    n.db.SetWithExpiry(key, value, expiredAt)  // 1. write local store + WAL
    l := n.leader                               // 2. snapshot the leader pointer
    if l != nil {
        l.BroadcastSet(key, value, expiredAt.UnixNano())  // 3. replicate
    }
    return nil
}
```

The `leader` field is protected by `n.mu`. Here we snapshot it under the lock and then call `BroadcastSet` outside the lock — this prevents holding `n.mu` during network I/O, which could create a deadlock or long hold times.

---

## RaftState — leader election

**File:** `cluster/raft.go`

This is a simplified Raft implementation. It covers leader election but not log replication in the Raft sense — replication here is done independently via the leader/follower streaming mechanism.

### State machine

Three roles: `RoleFollower` (0), `RoleCandidate` (1), `RoleLeader` (2).

The main loop:
```go
func (r *RaftState) run() {
    for {
        switch r.role {
        case RoleFollower:  r.runFollower()
        case RoleCandidate: r.runCandidate()
        case RoleLeader:    r.runLeader()
        }
    }
}
```

### Follower phase

```go
func (r *RaftState) runFollower() {
    timer := time.NewTimer(randomTimeout())  // 150–300 ms
    select {
    case <-r.heartbeatCh:  // reset: leader is alive
    case <-timer.C:        // timeout: become candidate
        r.role = RoleCandidate
    }
}
```

The election timeout is randomized between 150 ms and 300 ms. Randomization is the key Raft insight for preventing split votes: if all nodes had the same timeout, they'd all start elections simultaneously and likely split the vote every time.

`heartbeatCh` is a buffered channel of size 1. When a heartbeat arrives via `HandleHeartbeat`, a struct is sent on this channel (non-blocking due to `select default`). The follower's timer `select` picks it up and the follower stays a follower.

### Candidate phase

```go
func (r *RaftState) runCandidate() {
    r.currentTerm++
    r.votedFor = r.nodeAddr   // vote for self
    var votes int32 = 1       // self-vote counts
    for _, peer := range r.otherPeers() {
        go func(p string) {
            if r.requestVoteRPC(p, term) { atomic.AddInt32(&votes, 1) }
        }(peer)
    }
    // Wait for votes OR timeout OR a heartbeat from a valid leader
    // If got >= quorum: become leader; else: revert to follower
}
```

Vote requests are sent in parallel goroutines with a 200 ms timeout each. `atomic.AddInt32` is used because multiple goroutines write to `votes` concurrently. Quorum is `len(peers)/2 + 1` — a majority.

If a heartbeat arrives while waiting for votes, the node immediately steps back to follower. This handles the case where a leader was already elected and is running — no point competing.

### requestVoteRPC

```go
func (r *RaftState) requestVoteRPC(peer string, term uint64) bool {
    conn, _ := net.DialTimeout("tcp", peer, 200*time.Millisecond)
    defer conn.Close()
    sendFrame(conn, &protocol.VoteRequest{Term: term, CandidateID: r.nodeAddr})
    msg, _ := readFrame(conn)
    resp := msg.(*protocol.VoteResponse)
    if resp.Term > r.currentTerm {
        // Higher term seen — step down immediately
        r.role = RoleFollower
        r.votedFor = ""
    }
    return resp.Granted
}
```

Each RPC is a short-lived TCP connection. If the peer has a higher term, the candidate steps down (Raft safety rule: a higher term always wins).

### HandleVoteRequest (called by TCP server on incoming vote)

```go
func (r *RaftState) HandleVoteRequest(req *protocol.VoteRequest) (bool, uint64) {
    // Reject if req.Term < currentTerm
    // Reject if we're a leader in the same term (we won't yield)
    // If req.Term > currentTerm: update term, clear votedFor
    // Grant if votedFor is empty or already this candidate
}
```

The "already voted this term" check (`r.votedFor != ""`) prevents double-voting: once you vote for a candidate, you don't vote for another in the same term.

### Leader phase

```go
func (r *RaftState) runLeader() {
    ticker := time.NewTicker(heartbeatInterval)  // 50 ms
    for {
        <-ticker.C
        for _, peer := range r.otherPeers() {
            go r.sendHeartbeatRPC(peer, term)
        }
    }
}
```

The leader sends heartbeats to all peers every 50 ms. A heartbeat resets the election timer in each follower. If the leader dies, followers stop receiving heartbeats, their timers fire, and a new election begins.

The heartbeat interval (50 ms) is intentionally much shorter than the election timeout (150–300 ms), ensuring followers receive at least 3 heartbeats per election timeout window.

---

## Leader — replication to followers

**File:** `cluster/leader.go`

```go
type Leader struct {
    mu        sync.Mutex
    followers map[string]net.Conn  // nodeAddr → persistent TCP connection
    logIndex  atomic.Uint64
}
```

### AddFollower

Called by the TCP server when a `ReplicationJoin` message arrives:

```go
func (l *Leader) AddFollower(nodeAddr string, conn net.Conn) {
    l.followers[nodeAddr] = conn
    go l.drainAcks(nodeAddr, conn)  // background goroutine to read acks
}
```

The connection is kept open. The leader writes replication frames to it; the follower reads and applies them, then sends acks back.

### BroadcastSet / BroadcastDelete

```go
func (l *Leader) BroadcastSet(key string, value []byte, expiredAt int64) {
    l.broadcast(&protocol.ReplicationSync{
        LogIndex: l.logIndex.Add(1),  // atomic increment
        Op:       store.OpSet,
        Key:      []byte(key),
        Value:    value,
        TTL:      uint64(expiredAt),  // absolute Unix nanoseconds
    })
}
```

`logIndex` is an `atomic.Uint64` — it's incremented atomically so concurrent Set calls don't race. Each replication message gets a unique monotonically increasing index.

Note: `TTL` in the replication message is the **absolute expiry timestamp** (Unix nanoseconds), not a duration. This is essential — if it were a duration, follower clocks slightly behind the leader would compute a different expiry, causing inconsistency.

### broadcast

```go
func (l *Leader) broadcast(msg protocol.Message) {
    wire := encodeFrame(msg)
    l.mu.Lock()
    var dead []string
    for addr, conn := range l.followers {
        if _, err := conn.Write(wire); err != nil {
            dead = append(dead, addr)  // collect failed connections
        }
    }
    for _, addr := range dead {
        l.followers[addr].Close()
        delete(l.followers, addr)
    }
    l.mu.Unlock()
}
```

Failed followers are removed from the map. The leader doesn't retry or buffer missed entries for a reconnecting follower — when a follower reconnects, it starts from "now" and misses entries that happened while it was disconnected (eventual consistency, not strong consistency).

### drainAcks

A goroutine per follower that reads `ReplicationAck` frames. Currently, acks are consumed and discarded (future: track lag). When the connection drops, `drainAcks` exits and calls `removeFollower`.

---

## Follower — sync with leader

**File:** `cluster/follower.go`

```go
func RunFollower(raft *RaftState, db *store.KVStore, nodeAddr string) {
    for {
        if raft.GetRole() == RoleLeader { time.Sleep(100ms); continue }
        leaderAddr := raft.GetLeaderAddr()
        if leaderAddr == "" || leaderAddr == nodeAddr { time.Sleep(100ms); continue }
        err := syncWithLeader(leaderAddr, db, nodeAddr)
        time.Sleep(200ms)  // brief pause before reconnect
    }
}
```

`RunFollower` is always running as a goroutine (started in `Node.Start`). When this node is the leader, it sleeps. When it's a follower with a known leader, it calls `syncWithLeader`.

### syncWithLeader

```go
func syncWithLeader(leaderAddr string, db *store.KVStore, nodeAddr string) error {
    conn, _ := net.Dial("tcp", leaderAddr)
    defer conn.Close()
    sendFrame(conn, &protocol.ReplicationJoin{NodeAddr: nodeAddr})
    // Loop: read ReplicationSync frames → applySync → send ReplicationAck
    for {
        msg := readFrame(conn)
        sync := msg.(*protocol.ReplicationSync)
        applySync(db, sync)
        sendFrame(conn, &protocol.ReplicationAck{LastAppliedIndex: sync.LogIndex})
    }
}
```

The follower connects to the leader, sends `ReplicationJoin` to register itself, then enters a receive loop. It applies every `ReplicationSync` to its local store and sends an ack.

### applySync

```go
func applySync(db *store.KVStore, sync *protocol.ReplicationSync) {
    key := string(sync.Key)
    switch sync.Op {
    case store.OpSet:
        expiredAt := time.Unix(0, int64(sync.TTL))  // absolute timestamp
        db.SetWithExpiry(key, sync.Value, expiredAt)
    case store.OpDelete:
        db.Delete(key)
    }
}
```

`SetWithExpiry` is used (not `Set`) so the follower applies the leader's exact absolute expiry time, not a freshly computed one.

---

## wire.go — shared framing helpers

```go
func sendFrame(conn net.Conn, msg protocol.Message) error {
    wire, _ := protocol.EncodeMessage(msg)
    header := make([]byte, 4)
    binary.BigEndian.PutUint32(header, uint32(len(wire)))
    conn.Write(header)
    conn.Write(wire)
    return nil
}

func readFrame(conn net.Conn) (protocol.Message, error) {
    // read 4-byte length, then that many bytes, then decode
}
```

Identical to the server's `readMessage`/`writeMessage` but packaged as free functions for use within the cluster package (Raft RPCs, follower sync, leader broadcast).

---

## Consistency model

This system provides **eventual consistency** for reads on followers:

- Writes must go to the leader. The leader writes locally and replicates asynchronously.
- Reads can go to any node (leader or follower). A follower read may return slightly stale data if replication lag exists.
- A client reading from a follower immediately after a leader write may not see the new value yet.

**Interview question:** "Is this system strongly consistent?" — No. Replication is asynchronous. To achieve strong consistency, you would need to route all reads through the leader, or implement read quorums (read from a majority of nodes and take the most recent value). The current design trades consistency for read throughput by allowing follower reads.

**Interview question:** "What happens if the leader crashes mid-broadcast?" — The leader writes to its local WAL first, then broadcasts. If it crashes after the WAL write but before broadcasting, the entry exists on the leader's disk. When a new leader is elected (from the remaining nodes), those nodes may not have the entry. This is a data loss scenario. Solving it fully requires Raft log replication (not just election), where a write is not acknowledged to the client until a quorum of nodes have the entry.
