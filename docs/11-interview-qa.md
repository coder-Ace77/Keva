# Interview Q&A

Likely questions organized by topic. Each answer is short enough to say in an interview but complete enough to go deeper if pushed.

---

## Architecture / Design

**Q: Walk me through what happens when a client does SET foo bar.**

1. CLI client opens a TCP connection to the leader, sends `OpAuth` if a token is configured.
2. Client encodes `SetPayload{Key:"foo", Value:"bar", TTL:0}` and sends it with a 4-byte length prefix.
3. Server reads the frame, decodes it via the protocol registry, authenticates the connection.
4. `dispatchToEngine` calls `engine.Dispatch(msg, node)`.
5. `handleSet` calls `node.Set("foo", []byte("bar"), 0)`.
6. `Node.Set` computes `expiredAt = now + defaultTTL`, calls `db.SetWithExpiry`.
7. `KVStore.SetWithExpiry` appends a WAL frame first, then acquires the shard write lock, evicts if needed, writes the `Record`, releases the lock.
8. Back in `Node.Set`, `l.BroadcastSet` encodes a `ReplicationSync` and writes it to every connected follower's TCP connection.
9. Each follower receives the frame in its `syncWithLeader` loop, calls `applySync`, which calls `db.SetWithExpiry` with the leader's absolute expiry time.
10. Server sends `SetResponse{Success: true, Message: "OK"}` back to the client.

---

**Q: Why did you use sharding instead of a single global lock?**

A single `sync.RWMutex` over one map serializes concurrent writers. With 32 shards, a write to key A and a write to key B can proceed in parallel as long as they hash to different shards (which they usually do). The number of shards is configurable — 32 is a sweet spot: low enough that the slice of shard pointers is trivial, high enough to reduce contention significantly on multi-core hardware.

---

**Q: How does the WAL ensure durability?**

Before any key is written to the in-memory hash map, a binary frame is appended to the WAL file. On restart, `Replay()` reads the WAL from start to finish and replays each operation. In `always` mode, `file.Sync()` is called after every write — this forces the OS to flush the kernel buffer to the physical storage device, surviving a crash. In `interval` mode (the default), data can be lost in a crash window equal to the flush interval (500 ms by default).

---

**Q: What's WAL compaction and when would you use it?**

Every SET and DELETE appends to the WAL — it's append-only and grows forever. After many operations on the same keys, there are many redundant entries (old values that were overwritten). Compaction collapses the WAL to a single `OpSet` per currently-live key, discarding all history. You'd trigger it periodically (e.g., when the WAL exceeds a size threshold) or at maintenance windows. The implementation does a file rename (atomic on Linux) to swap the compacted file in place without downtime.

---

**Q: Explain your Raft implementation. Is it complete Raft?**

No, it's Raft leader election only — not Raft log replication. Standard Raft uses the replicated log to guarantee that a write is only acknowledged to the client after a quorum of nodes have durably recorded it. My implementation acknowledges the client as soon as the leader writes locally, then replicates asynchronously. This means a leader crash after acknowledging a write but before replication could lose that write. It's a pragmatic tradeoff for this project — implementing full Raft log replication is significantly more complex (log index tracking, follower catch-up, log truncation on term change). The election mechanism itself (randomized timeouts, vote requests, quorum, term management) is correctly implemented.

---

**Q: What consistency model does this system provide?**

Eventual consistency. Writes go to the leader and are immediately visible on the leader. Followers apply writes asynchronously. A read from a follower immediately after a leader write may return the old value. There is no read-your-writes guarantee unless the client always reads from the leader. Full strong consistency would require either leader-only reads or read quorums.

---

## Concurrency

**Q: How do you handle concurrent reads and writes to the same shard?**

`sync.RWMutex` per shard. Reads acquire `RLock` — multiple readers can proceed concurrently. Writes acquire `Lock` — exclusive, all other readers and writers wait. The one exception is `LRUEvict.OnRead`, which must promote the key in the LRU list — that requires a write lock even on a read path.

---

**Q: Is there a race between eviction and normal writes?**

No. `OnWrite` (which may evict to make room) is called while the shard write lock is held. `OnRemove` (clean up eviction bookkeeping) is also called under the write lock. So eviction and writes on the same shard are serialized. Cross-shard operations are independent — eviction on shard 5 never touches shard 12's lock.

---

**Q: How does the follower receive writes without blocking the leader?**

`Leader.broadcast` holds the leader's follower-map mutex while writing to follower connections. But the write to `shard.data` (in `Node.Set`) releases the shard lock before calling `BroadcastSet`. So the shard is available for other reads/writes while the leader is pushing replication frames to followers. The replication push itself is bounded by network speed to the followers, not by the shard lock.

---

## Protocol / Networking

**Q: Why a custom binary protocol instead of HTTP or gRPC?**

Lower overhead. HTTP adds headers, status lines, and text parsing. gRPC adds protobuf reflection and HTTP/2 framing. A custom binary protocol is the minimum bytes for each operation and parses in a single pass. It also gives complete control over the wire format — for example, the WAL and replication format share the same binary frame layout, which simplifies the code. The tradeoff is that it's harder to debug without purpose-built tooling and requires implementing the codec from scratch.

---

**Q: How do you prevent a client from crashing the server with a huge message?**

`maxPayloadBytes` (default 10 MB) is enforced in `readMessage` before allocating the frame buffer. If the 4-byte length header encodes a value larger than the limit, the server sends an `ErrCodeBadRequest` and closes the connection without allocating the giant buffer.

---

**Q: Why `subtle.ConstantTimeCompare` for token comparison?**

A standard `==` comparison on strings returns early on the first mismatched byte. An attacker sending slightly wrong tokens and measuring response time can learn the correct token one byte at a time (timing attack). `ConstantTimeCompare` always inspects all bytes regardless of where the mismatch is, giving no timing signal.

---

## Eviction

**Q: What are the three eviction modes and when would you use each?**

- **noevict** (default): Reject writes when the memory cap is reached. Use when you'd rather fail writes than silently lose data. Good for session stores where you must never silently evict.
- **relaxed** (sampling): Evict the key with the earliest expiry from a random sample of 5 keys. O(1) per eviction, no extra memory. Good for general caches with high write rates. This is what Redis used before it implemented approximate LRU with clock bits.
- **strict** (LRU): Exact LRU using a doubly-linked list + hash map. Gets are slightly more expensive (write lock on read path). Use when you need true LRU semantics — e.g., when your access pattern has a strong temporal locality that sampling would mishandle.

---

**Q: What's the memory footprint of LRU bookkeeping?**

Per key: one `list.Element` (roughly 3 pointers + the value = ~48 bytes on 64-bit) plus one map entry (`map[string]*list.Element`, roughly 8–16 bytes per bucket). For 1 million keys, that's roughly 64 MB of extra bookkeeping. The sampling approach has zero overhead per key.

---

## Operations

**Q: How do you run a cluster?**

```bash
# Node 1
KV_PORT=:7001 KV_NODE_ADDR=localhost:7001 KV_PEERS=localhost:7001,localhost:7002,localhost:7003 \
KV_WAL_PATH=node1.wal ./keyvaluestore &

# Node 2
KV_PORT=:7002 KV_NODE_ADDR=localhost:7002 KV_PEERS=localhost:7001,localhost:7002,localhost:7003 \
KV_WAL_PATH=node2.wal ./keyvaluestore &

# Node 3
KV_PORT=:7003 KV_NODE_ADDR=localhost:7003 KV_PEERS=localhost:7001,localhost:7002,localhost:7003 \
KV_WAL_PATH=node3.wal ./keyvaluestore &

# Connect client to any node
./cli -u localhost:7001
```

All three nodes share the same `KV_PEERS` list. Each has a unique `KV_NODE_ADDR`, `KV_PORT`, and `KV_WAL_PATH`.

---

**Q: How do you add authentication?**

```bash
# Generate a token
./keyvaluestore --gen-token
# Output: a8f3... (64 hex chars)

# Start server with token
KV_AUTH_TOKEN=a8f3... ./keyvaluestore

# Connect client with token
./cli -token a8f3...
# or
export KV_AUTH_TOKEN=a8f3...
./cli
```

---

**Q: What would you add to make this production-ready?**

1. **Full Raft log replication** — writes acknowledged only after quorum durability.
2. **Persistent term and votedFor** — currently in memory; a crash during an election could violate Raft's one-vote-per-term rule.
3. **Follower catch-up** — reconnecting followers currently miss writes that happened while they were down. A proper implementation would buffer the replication log or implement a snapshot transfer.
4. **Metrics** — expose Prometheus metrics (replication lag, ops/sec, error rate, memory usage).
5. **TLS** — all traffic currently in plaintext.
6. **WAL rotation and automatic compaction** — currently manual.
7. **Cluster membership changes** — Raft joint consensus for safely adding/removing nodes.
