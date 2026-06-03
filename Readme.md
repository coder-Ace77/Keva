# Product Overview — Distributed Key-Value Store

## What Is This?

A distributed key-value store is a database that stores data as simple **key → value** pairs and runs across multiple servers simultaneously. This project is a production-grade implementation built from scratch in Go, with a custom binary protocol, write-ahead logging, automatic leader election, and topology-aware clients.

Think of it as a self-hosted alternative to Redis or Memcached — with built-in clustering and automatic failover.

---

## Key Capabilities

### Core Storage
- **Set, Get, Delete** any UTF-8 key with an arbitrary binary value.
- **Automatic TTL** — every key expires after a configurable duration (default 15 minutes). No manual cleanup needed.
- **Crash recovery** — all writes go to a Write-Ahead Log on disk before memory. Restart the server and your data is back exactly as you left it.

### Clustering
- **Multi-node replication** — start two or more nodes; every write to the leader is automatically propagated to all followers within milliseconds.
- **Read scaling** — GET requests are served from followers, keeping the leader free for writes.
- **Automatic leader election** — if the leader node goes down, the remaining nodes elect a new leader within 300–550 ms. Clients reconnect to the new leader transparently.
- **No single point of failure** — a 3-node cluster tolerates the loss of any 1 node and continues serving reads and writes.

### Operational
- **Zero-dependency deployment** — a single Go binary, no runtime dependencies.
- **Environment-variable configuration** — every parameter tunable without recompiling.
- **WAL compaction** — compact the write-ahead log at any time to reclaim disk space.
- **Interactive CLI** — connect to any node in the cluster; the client discovers the topology automatically.

---

## Use Cases

| Use Case | How It Fits |
|----------|-------------|
| **Session storage** | Store user sessions with automatic TTL expiry. No separate cleanup job needed. |
| **Rate limiting** | Increment counters per user/IP with short TTLs. Distributed counters stay consistent across nodes. |
| **Feature flags / config cache** | Store runtime configuration that changes infrequently. All nodes stay in sync. |
| **Leaderboard / ephemeral rankings** | Fast in-memory lookups with persistence — survives restarts unlike pure in-memory caches. |
| **Service discovery** | Nodes register themselves with a short TTL; expired registrations vanish automatically. |
| **Development & testing** | Lightweight Redis alternative for local development without Docker or external services. |

---

## Getting Started

### Prerequisites
- Go 1.21 or later
- Linux or macOS

### Build

```bash
git clone <repo>
cd keyvaluestore
go build -o kvstore .
```

### Run a Single Node (Standalone)

```bash
./kvstore
# Server starts on :6379
```

Connect with the CLI:

```bash
go run ./cli
```

### Quick Commands

```
kv-db: SET username alice
OK
kv-db: GET username
"alice"
kv-db: DEL username
OK
kv-db: GET username
(nil)
kv-db: topology
  Leader:    localhost:6379
  (no followers in standalone mode)
kv-db: exit
```

---

## Running a Cluster

### Using the Provided Script

The easiest way to test clustering locally:

```bash
chmod +x create_cluster.sh
./create_cluster.sh
```

This builds the binary, picks three random ports, starts one cluster, waits for election to complete, and prints the exact CLI commands to connect. The leader is elected automatically — no configuration needed.

**Example output:**
```
┌──────────────────────────────────────────────────────────────┐
│                    Cluster is running                        │
├──────────────────────────────────────────────────────────────┤
│  Node 0 : localhost:23417  (PID 12301)                      │
│  Node 1 : localhost:27842  (PID 12302)                      │
│  Node 2 : localhost:31955  (PID 12303)                      │
├──────────────────────────────────────────────────────────────┤
│  Connect CLI to any node — it discovers the leader itself:  │
│    go run ./cli -u localhost:23417                           │
│    go run ./cli -u localhost:27842                           │
│    go run ./cli -u localhost:31955                           │
│                                                              │
│  Kill a node (e.g. Node 0) to trigger a new election:      │
│    kill 12301                                                │
└──────────────────────────────────────────────────────────────┘
```

### Manual Cluster Setup

Start each node with its own port, WAL file, and the shared peer list. All three nodes must know about each other.

**Node 0 (terminal 1):**
```bash
KV_PORT=":7000" \
KV_NODE_ADDR="localhost:7000" \
KV_PEERS="localhost:7000,localhost:7001,localhost:7002" \
KV_WAL_PATH="/var/data/node0.wal" \
./kvstore
```

**Node 1 (terminal 2):**
```bash
KV_PORT=":7001" \
KV_NODE_ADDR="localhost:7001" \
KV_PEERS="localhost:7000,localhost:7001,localhost:7002" \
KV_WAL_PATH="/var/data/node1.wal" \
./kvstore
```

**Node 2 (terminal 3):**
```bash
KV_PORT=":7002" \
KV_NODE_ADDR="localhost:7002" \
KV_PEERS="localhost:7000,localhost:7001,localhost:7002" \
KV_WAL_PATH="/var/data/node2.wal" \
./kvstore
```

All three nodes start as followers. Within 300 ms one wins the election and becomes the leader.

### Connect the CLI to Any Node

```bash
go run ./cli -u localhost:7001
```

The client asks that node for the cluster topology and discovers the leader automatically. You do not need to know in advance which node is the leader.

---

## CLI Reference

### Starting the CLI

```bash
go run ./cli                          # connects to localhost:6379
go run ./cli -u localhost:7001        # connects to a specific node
go run ./cli --url localhost:7001     # same, long form
KV_ADDR=localhost:7001 go run ./cli   # same, via environment variable
```

### Commands

| Command | Example | Notes |
|---------|---------|-------|
| `SET key value` | `SET user:1 alice` | Value can contain spaces: `SET msg hello world` |
| `GET key` | `GET user:1` | Returns `(nil)` if key not found or expired |
| `DEL key` | `DEL user:1` | Returns yellow `(nil)` if key did not exist |
| `topology` | `topology` | Shows current leader and all follower addresses |
| `exit` / `quit` | `exit` | Disconnect and quit |

### Output Colors

| Color | Meaning |
|-------|---------|
| Green | Success (OK, write accepted) |
| Cyan bold | GET value found |
| Yellow | Key not found (nil) or soft failure |
| Red bold | Error |

### Read vs Write Routing

The CLI automatically routes commands:
- **SET / DEL** → always sent to the **leader**
- **GET** → sent to a random **follower** (load-balanced reads)

If the targeted node is unreachable, the CLI re-discovers the topology and retries silently. You will see a yellow `(discovering cluster...)` message during the brief re-election window.

---

## Failure Scenarios

### Leader Goes Down

1. Client's next write attempt fails.
2. CLI prints `(node X unreachable, re-discovering cluster...)`.
3. Raft election fires within 150–300 ms.
4. New leader elected within ~550 ms total.
5. CLI discovers the new leader and retries the write automatically.
6. The operation succeeds; the user sees no permanent error.

### Follower Goes Down

- Reads that land on the dead follower are retried on another follower.
- Writes to the leader are unaffected.
- When the follower restarts, it reconnects to the current leader and catches up automatically.

### Network Partition (Minority Side)

- The minority partition (fewer than quorum nodes) cannot elect a leader.
- Writes to the minority are rejected with `ERR [1]: not the leader`.
- The majority partition continues operating normally.
- When the network heals, minority nodes rejoin and catch up.

### All Nodes Restart

- Each node replays its WAL on startup to restore memory state.
- A new Raft election happens automatically.
- Data written before the restart is fully restored.

---

## Configuration Reference

All settings are controlled via environment variables. The defaults are production-safe for small to medium workloads.

### Server

| Variable | Default | Description |
|----------|---------|-------------|
| `KV_PORT` | `:6379` | Port the server listens on. Use `:0` to let the OS pick. |
| `KV_MAX_PAYLOAD_BYTES` | `10485760` | Reject messages larger than this (bytes). Prevents memory exhaustion. |

### Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `KV_SHARD_COUNT` | `32` | Number of internal hash table shards. Higher values reduce write lock contention under load. |
| `KV_DEFAULT_TTL` | `15m` | How long keys live before automatic expiry. Accepts Go duration strings: `30s`, `1h`, `24h`. |
| `KV_WAL_PATH` | `production.wal` | Path to the write-ahead log file. Use separate paths for each node. |

### WAL Durability

| Variable | Default | Options | Trade-off |
|----------|---------|---------|-----------|
| `KV_WAL_MODE` | `interval` | `always`, `interval`, `none` | `always` = safest, `none` = fastest |
| `KV_WAL_FLUSH_INTERVAL` | `500ms` | Any Go duration | Lower = safer, higher = faster |
| `KV_WAL_BATCH_SIZE` | `1000` | Any positive integer | Max entries buffered before forced flush |

**Choosing a WAL mode:**
- `always` — Use when data loss is unacceptable. Every write is fsynced before the client receives a response. Throughput drops significantly.
- `interval` *(default)* — Best balance. At most `KV_WAL_FLUSH_INTERVAL` of writes can be lost on hard crash.
- `none` — Use for caches or test environments where durability is not required. No fsync overhead.

### Cluster

| Variable | Default | Description |
|----------|---------|-------------|
| `KV_NODE_ADDR` | _(empty)_ | This node's own address as other nodes see it (e.g. `localhost:7000` or `10.0.0.1:7000`). Required in cluster mode. |
| `KV_PEERS` | _(empty)_ | Comma-separated list of **all** cluster nodes, including this node. Every node must have the same list. |

**Cluster is disabled when `KV_PEERS` or `KV_NODE_ADDR` is empty.** The node starts in standalone mode and immediately acts as leader.

### CLI

| Variable | Default | Description |
|----------|---------|-------------|
| `KV_ADDR` | `localhost:6379` | Default node address for the CLI. Overridden by `--url` / `-u`. |

---

## Sizing Guide

| Cluster Size | Fault Tolerance | Notes |
|-------------|-----------------|-------|
| 1 node | None | Standalone; no election overhead |
| 3 nodes | 1 node failure | Minimum recommended for production |
| 5 nodes | 2 node failures | Higher availability, more write overhead |

A 3-node cluster is the standard recommendation. Adding more nodes improves read capacity (more followers to distribute reads) but does not improve write throughput (all writes still go through the single leader).

---

## Performance Characteristics

| Metric | Behaviour |
|--------|-----------|
| **Read latency** | Sub-millisecond on localhost; scales with network RTT |
| **Write latency** | Determined by WAL mode; `interval` adds at most `KV_WAL_FLUSH_INTERVAL` |
| **Read concurrency** | Linear with `KV_SHARD_COUNT`; each shard has independent read locks |
| **Write concurrency** | Serialised per shard; 32 shards = 32 parallel write streams |
| **Key expiry** | Lazy on read (immediate) + active sweep every 60 s |
| **Replication lag** | Typically < 5 ms on a LAN; bounded by network RTT |
| **Election time** | 150–550 ms from leader failure to new leader operational |

---

## Limitations (Current Version)

- **No log ordering guarantees on replication** — under heavy concurrent writes to the leader, followers may briefly diverge before catching up. Strong consistency is not guaranteed.
- **In-memory only** — all data must fit in RAM. There is no disk-based storage tier.
- **TTL is server-default only** — clients cannot specify per-key TTL at the protocol level (the field exists in the protocol but the server applies the default).
- **No authentication or TLS** — the server trusts all incoming connections. Do not expose the port to untrusted networks without a firewall or proxy.
- **No WAL compaction trigger** — compaction must be called programmatically; there is no automatic background compaction.
- **Write forwarding not implemented** — if a client accidentally sends a write to a follower, the follower writes it locally without replication. The topology-aware CLI prevents this under normal operation.
