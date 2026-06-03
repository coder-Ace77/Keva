# Technical Reference — Distributed Key-Value Store

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Package Layout](#package-layout)
3. [Protocol Layer](#protocol-layer)
4. [Storage Engine](#storage-engine)
5. [Engine Layer](#engine-layer)
6. [Cluster & Consensus](#cluster--consensus)
7. [Server Layer](#server-layer)
8. [CLI Client](#cli-client)
9. [Configuration Reference](#configuration-reference)
10. [Wire Format](#wire-format)
11. [Data Flow Diagrams](#data-flow-diagrams)

---

## Architecture Overview

The system is a distributed key-value store built in Go. It is structured as five independent layers with clean dependency boundaries:

```
cli  ──►  TCP  ──►  server  ──►  engine  ──►  store
                       │
                    cluster  ──►  protocol
                       │
                     raft
```

**Dependency graph (no cycles):**

| Package | Imports |
|---------|---------|
| `protocol` | stdlib only |
| `store` | stdlib only |
| `config` | stdlib only |
| `engine` | `protocol`, `store` |
| `cluster` | `protocol`, `store` |
| `server` | `engine`, `cluster`, `protocol` |
| `cli` | `protocol` |
| `main` | `server`, `cluster`, `store`, `config` |

---

## Package Layout

```
keyvaluestore/
├── main.go                  Entry point; wires all packages together
├── config/
│   └── config.go            Typed config struct + env-var loader
├── protocol/
│   ├── payload.go           Message interface + registry
│   ├── opcodes.go           All wire opcodes and error codes
│   ├── registry_init.go     init() registers all message types
│   ├── get_payload.go       OpGet (0x01)
│   ├── set_payload.go       OpSet (0x02)
│   ├── delete_payload.go    OpDelete (0x03)
│   ├── get_response.go      OpGetResponse (0x04)
│   ├── response_payload.go  OpSetResponse (0x05), OpDeleteResponse (0x06)
│   ├── topology.go          OpTopologyRequest (0x10), OpTopologyResponse (0x11)
│   ├── replication.go       OpReplicationSync (0x20), OpReplicationAck (0x21)
│   ├── replication_join.go  OpReplicationJoin (0x22)
│   ├── heartbeat.go         OpHeartbeat (0x30), OpHeartbeatAck (0x31)
│   ├── election.go          OpVoteRequest (0x40), OpVoteResponse (0x41)
│   ├── forward.go           OpForwardWrite (0x50), OpForwardWriteResponse (0x51)
│   └── error.go             OpError (0xFE)
├── store/
│   ├── store.go             Sharded hash map + TTL + WAL replay + compaction
│   └── wal.go               Write-ahead log with three sync modes
├── engine/
│   ├── handler.go           Store interface + handler registry + Dispatch
│   ├── get.go               GET handler
│   ├── set.go               SET handler
│   ├── delete.go            DELETE handler
│   └── registry_init.go     Registers all three CRUD handlers
├── cluster/
│   ├── raft.go              Raft state machine (election, heartbeat, vote RPC)
│   ├── node.go              Node struct — wraps store, implements engine.Store
│   ├── leader.go            Leader struct — manages follower conns, broadcasts
│   ├── follower.go          Follower goroutine — syncs with current leader
│   └── wire.go              sendFrame / readFrame TCP helpers
├── server/
│   └── tcp.go               TCP server — first-message routing, client loop
├── cli/
│   └── cli.go               REPL client — topology-aware, per-command connections
└── create_cluster.sh        3-node cluster launcher for local testing
```

Total: ~3,000 lines of Go.

---

## Protocol Layer

### Design

Every message on the wire is typed by its first byte (the opcode). Instead of switch statements scattered across the codebase, the protocol uses a **registry pattern**:

```
Register(opcode, factory) → adds opcode → factory to a sync.RWMutex-protected map
Lookup(opcode)            → returns a fresh Message instance
DecodeMessage([]byte)     → reads opcode, looks up factory, calls msg.Decode()
EncodeMessage(Message)    → calls msg.Encode(), prepends opcode byte
```

Adding a new message type requires: one struct implementing `Message`, one `Register` call in `registry_init.go`. No changes to any dispatch path.

### Message Interface

```go
type Message interface {
    OpCode() byte
    Encode() ([]byte, error)
    Decode(data []byte) error
}
```

### Opcode Table

| Hex | Constant | Direction | Description |
|-----|----------|-----------|-------------|
| 0x01 | `OpGet` | Client → Any | Read a key |
| 0x02 | `OpSet` | Client → Leader | Write a key |
| 0x03 | `OpDelete` | Client → Leader | Delete a key |
| 0x04 | `OpGetResponse` | Node → Client | Value or not-found |
| 0x05 | `OpSetResponse` | Leader → Client | Write result |
| 0x06 | `OpDeleteResponse` | Leader → Client | Delete result |
| 0x10 | `OpTopologyRequest` | Client → Any | Discover cluster |
| 0x11 | `OpTopologyResponse` | Any → Client | Cluster layout |
| 0x20 | `OpReplicationSync` | Leader → Follower | Stream a WAL entry |
| 0x21 | `OpReplicationAck` | Follower → Leader | Progress report |
| 0x22 | `OpReplicationJoin` | Follower → Leader | Register as replica |
| 0x30 | `OpHeartbeat` | Leader → Peer | Liveness ping |
| 0x31 | `OpHeartbeatAck` | Peer → Leader | Liveness ack |
| 0x40 | `OpVoteRequest` | Candidate → Peer | Request vote |
| 0x41 | `OpVoteResponse` | Peer → Candidate | Grant/deny vote |
| 0x50 | `OpForwardWrite` | Follower → Leader | Wrap a client write |
| 0x51 | `OpForwardWriteResponse` | Leader → Follower | Forwarded result |
| 0xFE | `OpError` | Any → Any | Structured error |

### Error Codes

| Code | Constant | Meaning |
|------|----------|---------|
| 1 | `ErrCodeNotLeader` | Write sent to a follower |
| 2 | `ErrCodeKeyNotFound` | Key does not exist |
| 3 | `ErrCodeInternal` | Unexpected server error |
| 4 | `ErrCodeBadRequest` | Malformed message |
| 5 | `ErrCodeTimeout` | Operation timed out |

---

## Storage Engine

### KVStore

`store.KVStore` is an in-memory hash map sharded across `N` independent `Shard` structs (default 32). Each shard has its own `sync.RWMutex`, allowing GET operations on different shards to proceed in parallel.

**Shard selection:**

```
shardIndex = FNV-1a(key) % shardCount
```

**Record structure:**

```go
type Record struct {
    value     []byte
    createdAt time.Time
    expiredAt time.Time
}
```

**TTL enforcement:**
- On `Get`: checks `time.Now().After(record.expiredAt)` — expired records return `ErrKeyNotFound`.
- GC sweep: a background ticker (1-minute interval) removes expired records from all shards.
- Default TTL: 15 minutes (configurable).

### Write-Ahead Log (WAL)

The WAL provides crash recovery. All writes are appended to a binary log **before** the in-memory map is updated. On startup, `Replay()` re-executes every WAL entry to rebuild state.

**WAL record format (binary, big-endian):**

```
[Op 1B][KeyLen 2B][ValLen 4B][ExpiredAt 8B][Key ...][Value ...]
```

- `Op`: 1 = SET, 2 = DELETE
- `ExpiredAt`: Unix nanoseconds (absolute timestamp)
- For DELETE records: `ValLen = 0`, `ExpiredAt = 0`, no value bytes

**Sync modes:**

| Mode | Behaviour | Durability | Throughput |
|------|-----------|------------|------------|
| `always` | `fsync` after every write | Highest | Lowest |
| `interval` | Batch writes, flush every N ms or M entries | Medium | Highest |
| `none` | Write to file, OS decides when to flush | Lowest | Highest |

Default: `interval` with 500 ms flush and 1000 entry batch.

**WAL compaction:**

`KVStore.Compact(filePath)` rewrites the WAL from current memory state, discarding all superseded operations. Steps:
1. Write all live (non-expired) records to a `.tmp` file.
2. Acquire the WAL mutex.
3. Atomic `os.Rename(.tmp → filePath)`.
4. Reopen the file for subsequent appends.

### SetWithExpiry

```go
func (kv *KVStore) SetWithExpiry(key string, value []byte, expiredAt time.Time) error
```

Used exclusively by replication followers. Accepts a caller-supplied absolute expiry so TTL is identical across all nodes in the cluster.

---

## Engine Layer

The engine decouples command dispatch from both the protocol and the storage layers.

### Store Interface

```go
type Store interface {
    Set(key string, value []byte) error
    Get(key string) ([]byte, error)
    Delete(key string) error
}
```

Both `*store.KVStore` (standalone) and `*cluster.Node` (cluster mode) satisfy this interface via Go structural typing — neither imports the `engine` package.

### Handler Registry

Mirrors the protocol registry pattern:

```go
type Handler func(msg protocol.Message, db Store) (protocol.Message, error)

Register(opcode byte, h Handler)   // panics on duplicate
Dispatch(msg Message, db Store)    // looks up and calls handler
```

### Registered Handlers

| Opcode | Handler | Behaviour |
|--------|---------|-----------|
| `OpGet` | `handleGet` | `db.Get(key)` → `GetResponse{Found, Value}` or `GetResponse{Found: false}` |
| `OpSet` | `handleSet` | `db.Set(key, value)` → `SetResponse{Success, Message}` |
| `OpDelete` | `handleDelete` | `db.Delete(key)` → `DeleteResponse{Success, Message}` |

---

## Cluster & Consensus

### Node

`cluster.Node` wraps `*store.KVStore` and adds:
- A `*RaftState` for consensus
- A `*Leader` (non-nil only when this node is the Raft leader)

**Write path (cluster mode):**
1. Calculate `expiredAt = now + defaultTTL`
2. Call `db.SetWithExpiry(key, value, expiredAt)` — writes to WAL + memory
3. If `leader != nil`: call `leader.BroadcastSet(key, value, expiredAt.UnixNano())`

**Standalone mode:** `ForceLeader()` sets `leader = newLeader()` without running Raft.

### Raft State Machine

Implements a simplified Raft leader election. Log replication ordering is handled separately (via `ReplicationSync`); Raft here is used purely for leader election and failure detection.

**Timing constants:**

| Constant | Value | Purpose |
|----------|-------|---------|
| `electionTimeoutMin` | 150 ms | Minimum before starting election |
| `electionTimeoutMax` | 300 ms | Maximum before starting election |
| `heartbeatInterval` | 50 ms | Leader heartbeat cadence |

**State transitions:**

```
         election timeout             majority votes
Follower ──────────────────► Candidate ──────────────► Leader
   ▲                            │                         │
   │     higher-term heartbeat  │                         │
   └────────────────────────────┘                         │
   │                  higher-term heartbeat/vote           │
   └───────────────────────────────────────────────────────┘
```

**Election process (`runCandidate`):**
1. Increment `currentTerm`, vote for self.
2. Send `VoteRequest{Term, CandidateID}` to all other peers in parallel goroutines.
3. Wait for: all responses, or election timeout, or incoming heartbeat.
4. If `votes >= quorum (N/2 + 1)`: become leader, set `leaderAddr = nodeAddr`, invoke `onLeader` callback.
5. Otherwise: revert to follower.

**Vote granting (`HandleVoteRequest`):**
- Grant if `req.Term >= currentTerm` AND `votedFor == ""` or `votedFor == req.CandidateID`.
- A leader in the same term does not grant votes.
- If `req.Term > currentTerm`: always update term, clear vote, step down.

**Heartbeat handling (`HandleHeartbeat`):**
- Ignore if `term < currentTerm`.
- Update `leaderAddr`, set role to follower, signal `heartbeatCh` to reset election timer.
- If the receiving node was a leader (higher-term split-brain scenario): invoke `onFollower` callback.

**RPC transport:** All consensus RPCs use short-lived TCP connections:
- Dial with 200 ms timeout (votes) / 50 ms timeout (heartbeats)
- Send one message, read one response, close.
- Heartbeats are effectively fire-and-forget (ack is drained but not acted on).

### Leader

`cluster.Leader` manages the set of replication connections from followers.

- `AddFollower(nodeAddr, conn)`: stores the conn, starts `drainAcks` goroutine.
- `BroadcastSet` / `BroadcastDelete`: builds a `ReplicationSync` frame, writes to all follower conns under a mutex, removes dead connections.
- `drainAcks`: reads `ReplicationAck` messages from the follower. On EOF, calls `removeFollower` which closes the conn and deletes from the map.
- Log index uses `sync/atomic.Uint64` for lock-free increment.

### Follower Replication

`cluster.RunFollower` runs in a background goroutine:

```
loop:
  if role == Leader → sleep 100ms, continue
  leaderAddr = raft.GetLeaderAddr()
  if leaderAddr == "" or == self → sleep 100ms, continue
  err = syncWithLeader(leaderAddr, db, nodeAddr)
  sleep 200ms  ← gives Raft time to elect a new leader
```

`syncWithLeader`:
1. Dial leader.
2. Send `ReplicationJoin{NodeAddr}`.
3. Read `ReplicationSync` frames in a loop.
4. For SET: call `db.SetWithExpiry(key, value, time.Unix(0, sync.TTL))`.
5. For DELETE: call `db.Delete(key)`, ignore `ErrKeyNotFound`.
6. Send `ReplicationAck{LastAppliedIndex: sync.LogIndex}`.

---

## Server Layer

`server.TCPServer` listens on a single TCP port and serves all connection types.

### Connection Routing

The **first message** opcode determines the connection type. No separate ports are used.

```
accept conn
  │
  ├─ OpReplicationJoin  → leader.AddFollower(conn)   [conn ownership transferred]
  ├─ OpHeartbeat        → node.HandleHeartbeat() → HeartbeatAck  [close]
  ├─ OpVoteRequest      → node.HandleVoteRequest() → VoteResponse  [close]
  ├─ OpTopologyRequest  → node.GetTopology() → TopologyResponse  [close]
  └─ anything else      → client CRUD loop until EOF  [this goroutine closes]
```

**Client loop:**
1. `readMessage`: read 4-byte length, check against `maxPayloadBytes`, read frame, `DecodeMessage`.
2. `engine.Dispatch(msg, node)` → response.
3. `writeMessage`: encode response, write 4-byte length + wire bytes.
4. Repeat until EOF or error.

### Frame Format

```
[Length 4B big-endian][OpCode 1B][Payload ...]
```

The length field covers `OpCode + Payload` (not including the 4-byte length header itself).

---

## CLI Client

### kvClient

The CLI maintains cluster topology state and routes each command to the appropriate node.

```go
type kvClient struct {
    knownAddrs []string  // seed addr + all addresses from topology
    leaderAddr string    // current Raft leader
    followers  []string  // all non-leader nodes
}
```

### Startup

1. Connect to the seed node (`--url` / `KV_ADDR` / `localhost:6379`).
2. Send `TopologyRequest`, receive `TopologyResponse`.
3. Populate `leaderAddr` and `followers`.
4. Display cluster state to user.

### Per-Command Routing

A **new TCP connection** is made for every command (no persistent connection):

| Command | Target | Fallback |
|---------|--------|----------|
| `SET`, `DEL` | `leaderAddr` | Re-discover topology, retry up to 8× |
| `GET` | Random `followers[i]` | `leaderAddr` if no followers |

### Failure & Retry

On connection error:
1. Clear `leaderAddr`.
2. Poll all `knownAddrs` for a new topology every 300 ms, up to 15 attempts (~4.5 s).
3. This window exceeds the maximum Raft election time (300 ms timeout + 200 ms RPC + 50 ms heartbeat propagation ≈ 550 ms).

### Built-in Commands

| Input | Action |
|-------|--------|
| `GET <key>` | Read key from a follower |
| `SET <key> <value>` | Write key to the leader |
| `DEL <key>` | Delete key via the leader |
| `topology` | Print current leader + follower addresses |
| `exit` / `quit` | Disconnect |

---

## Configuration Reference

All configuration is loaded via environment variables. The process starts with `config.Default()` and applies any set variables on top.

| Variable | Default | Description |
|----------|---------|-------------|
| `KV_PORT` | `:6379` | TCP listen address |
| `KV_MAX_PAYLOAD_BYTES` | `10485760` | Per-message size limit (bytes) |
| `KV_SHARD_COUNT` | `32` | Number of hash shards |
| `KV_DEFAULT_TTL` | `15m` | Key TTL (Go duration string, e.g. `30m`, `1h`) |
| `KV_WAL_PATH` | `production.wal` | WAL file path |
| `KV_WAL_MODE` | `interval` | Sync mode: `always` / `interval` / `none` |
| `KV_WAL_FLUSH_INTERVAL` | `500ms` | Flush interval (only in `interval` mode) |
| `KV_WAL_BATCH_SIZE` | `1000` | Max batch size before forced flush |
| `KV_NODE_ADDR` | _(empty)_ | This node's public address for cluster mode |
| `KV_PEERS` | _(empty)_ | Comma-separated list of all cluster nodes |
| `KV_ADDR` | `localhost:6379` | CLI only — default server to connect to |

---

## Wire Format

### TCP Frame

Every message on the wire is preceded by a 4-byte big-endian length prefix:

```
┌──────────────────────────────────────────────────┐
│  Length (4 bytes, big-endian)                    │  ← covers OpCode + Payload
├──────────────────────────────────────────────────┤
│  OpCode (1 byte)                                 │
├──────────────────────────────────────────────────┤
│  Payload (variable)                              │
└──────────────────────────────────────────────────┘
```

### Per-Message Payload Layouts

**GetPayload (0x01)**
```
[KeyLen 2B][Key ...]
```

**SetPayload (0x02)**
```
[KeyLen 2B][ValLen 4B][TTL 8B][Key ...][Value ...]
TTL: nanoseconds, 0 = use server default
```

**DeletePayload (0x03)**
```
[KeyLen 2B][Key ...]
```

**GetResponse (0x04)**
```
[Found 1B][ValLen 4B][Value ...]
Found: 0x01 = exists, 0x00 = not found
```

**SetResponse (0x05) / DeleteResponse (0x06)**
```
[Success 1B][MsgLen 2B][Message ...]
```

**TopologyResponse (0x11)**
```
[LeaderAddrLen 2B][LeaderAddr ...]
[NodeCount 2B]
  for each node:
    [AddrLen 2B][Addr ...][Role 1B][NodeIDLen 2B][NodeID ...]
Role: 0x00 = Follower, 0x01 = Leader
```

**ReplicationSync (0x20)**
```
[LogIndex 8B][Op 1B][KeyLen 2B][ValLen 4B][TTL 8B][Key ...][Value ...]
Op: 1 = SET, 2 = DELETE
TTL: absolute Unix nanoseconds for SET; 0 for DELETE
```

**ReplicationJoin (0x22)**
```
[AddrLen 2B][Addr ...]
```

**Heartbeat (0x30)**
```
[Term 8B][LeaderIDLen 2B][LeaderID ...]
```

**VoteRequest (0x40)**
```
[Term 8B][CandidateIDLen 2B][CandidateID ...][LastLogIndex 8B]
```

**VoteResponse (0x41)**
```
[Term 8B][Granted 1B]
Granted: 0x01 = yes, 0x00 = no
```

**ErrorMessage (0xFE)**
```
[Code 2B][MsgLen 2B][Message ...]
```

---

## Data Flow Diagrams

### Client Write (Cluster Mode)

```
CLI                Leader             Follower1         Follower2
 │                   │                    │                  │
 │─ TopologyReq ─────►│                    │                  │
 │◄─ TopologyResp ────│                    │                  │
 │                   │                    │                  │
 │─ SET key val ─────►│                    │                  │
 │                   │─ SetWithExpiry()   │                  │
 │                   │─ ReplicationSync ──►│                  │
 │                   │─ ReplicationSync ───────────────────►  │
 │                   │                    │─ ReplicationAck ─►│
 │                   │◄─ ReplicationAck ──│                  │
 │◄─ SetResponse ────│                    │                  │
```

### Client Read (Cluster Mode)

```
CLI                  Follower
 │                      │
 │─ TopologyReq ─────►  │
 │◄─ TopologyResp ───── │
 │                      │
 │─ GET key ──────────► │ (reads from local store)
 │◄─ GetResponse ─────  │
```

### Leader Election

```
Node0        Node1 (dead)      Node2
  │                │               │
  │  ← heartbeat timeout ─         │
  │  (150–300 ms)                  │
  │                                │
  │─── VoteRequest(term=2) ────────►│
  │◄── VoteResponse(granted=true) ──│
  │                                │
  │  (self-vote + Node2 vote = 2 ≥ quorum 2)
  │                                │
  │  becomes Leader                │
  │─── Heartbeat(term=2) ──────────►│
  │                                │ (leaderAddr updated)
```

### Follower Reconnection After Election

```
Follower          Dead Leader     New Leader
    │                  │               │
    │  sync loop breaks (EOF)          │
    │                  ×               │
    │  sleep 200ms                     │
    │                                  │
    │  ← Heartbeat(term=2, leader=NewLeader)
    │  leaderAddr = NewLeader          │
    │                                  │
    │─── ReplicationJoin ──────────────►│
    │◄── ReplicationSync stream ────────│
```
