# Package: store

**Files:** `store/store.go`, `store/wal.go`, `store/eviction.go`

This is the core storage engine. Everything else in the project sits on top of this package. It provides three capabilities: a concurrent sharded in-memory hash map, a Write-Ahead Log (WAL) for durability, and pluggable eviction policies.

---

## The Record type

```go
type Record struct {
    value     []byte
    createdAt time.Time
    expiredAt time.Time
}
```

Every stored entry carries its value and an absolute expiry timestamp. There is no `nil` expiry — every key must have a deadline. If the client sends TTL=0, `main.go` fills in `config.DefaultTTL` (default: 15 minutes).

---

## Sharded hash map

### Why sharding?

A single `sync.RWMutex` over one `map[string]Record` would serialize all concurrent reads and writes. With 32 shards, 32 independent locks allow up to 32 concurrent writers (on different shards) and many more concurrent readers.

### How a key is routed to a shard

```go
func (kv *KVStore) getShard(key string) *Shard {
    h := fnv.New32a()
    h.Write([]byte(key))
    shardIndex := h.Sum32() % kv.shardCount
    return kv.shards[shardIndex]
}
```

FNV-1a is a fast non-cryptographic hash. The shard index is the hash modulo the shard count. This is deterministic — the same key always lands on the same shard, which is why you can acquire only that shard's lock for any given operation.

### Shard struct

```go
type Shard struct {
    mu      sync.RWMutex
    data    map[string]Record
    bytes   int64      // sum of len(key)+len(value) for all entries
    budget  int64      // per-shard byte cap (MaxMemoryBytes / ShardCount)
    lru     *list.List         // only populated in strict LRU mode
    lruKeys map[string]*list.Element
}
```

`bytes` is kept up-to-date on every write and delete so the eviction policy can check memory pressure in O(1) without iterating the map.

---

## KVStore operations

### Set

```
1. Append a WAL entry first (durability before in-memory write)
2. Lock the shard (write lock)
3. If key already exists: subtract old footprint from bytes counter, notify eviction
4. Call eviction.OnWrite — policy may free space or reject the write
5. Write the new Record into shard.data
6. Add new footprint to bytes counter
7. Call eviction.AfterWrite — policy updates bookkeeping (e.g. LRU list)
8. Unlock
```

The WAL is written before the in-memory map for the same reason a database writes to the log before modifying pages: if the process crashes after the WAL write but before the map write, the entry is recoverable on restart. If the process crashes before the WAL write, the operation is simply lost — which is the expected behaviour.

### SetWithExpiry

Identical to `Set` but the caller supplies the absolute `expiredAt` time directly. Used by replication followers so they mirror the leader's exact expiry rather than computing a new one from their local clock.

### Get

```
1. Hash key → shard
2. RLock (read lock, allows concurrent readers)
3. Look up record
4. RUnlock
5. If not found OR time.Now().After(record.expiredAt) → return ErrKeyNotFound
6. Copy value bytes (caller gets its own slice; no aliasing into the map)
7. Call eviction.OnRead (may re-acquire write lock internally for LRU)
```

The copy on return is intentional: the caller must not hold a reference into the map's internal slice, because another goroutine could overwrite that key while the caller still holds the pointer.

Note: expired keys are not proactively removed on Get — the GC loop does that. A Get on an expired key just returns "not found" and leaves the stale entry for the GC to collect later.

### Delete

```
1. Append WAL delete entry
2. Write lock the shard
3. Subtract footprint from bytes counter
4. Call eviction.OnRemove (clean up LRU bookkeeping)
5. delete(shard.data, key)
6. Unlock
```

### Background GC (`startGC`)

A goroutine wakes every 1 minute, locks each shard in turn (write lock), and deletes every expired key. This is the only place where TTL expiry actually reclaims memory from the map.

---

## WAL (Write-Ahead Log)

**File:** `store/wal.go`

The WAL is a flat binary log file. Every `Set` and `Delete` appends a fixed-format frame. On startup, `Replay()` reads the file from start to finish and replays each operation into the in-memory map.

### Binary frame format

```
[Op 1B][KeyLen 2B][ValLen 4B][ExpiredAt 8B][Key ...][Value ...]
  Total header: 15 bytes
```

`Op` is `0x01` (OpSet) or `0x02` (OpDelete). For deletes, `ValLen` and `ExpiredAt` are written as zero — `Replay` ignores them for delete entries.

### Three sync modes

| Mode | Behaviour | Durability | Throughput |
|---|---|---|---|
| `always` | `file.Write` then `file.Sync()` on every write | Survives hard crash | Lowest |
| `interval` | Batches frames in a channel; a goroutine flushes at a fixed interval or when batch is full | May lose up to `FlushInterval` of writes on crash | Highest |
| `none` | `file.Write` but never calls `file.Sync()`. OS flushes at its discretion | May lose any unflushed writes | Medium |

**Default is `interval` with 500 ms flush and batch size 1000.** This is a pragmatic tradeoff for a system where sub-second data loss is acceptable.

### Interval flusher goroutine

```go
func (w *WAL) startFlusher() {
    ticker := time.NewTicker(w.options.FlushInterval)
    var batch [][]byte

    flush := func() {
        w.mu.Lock()
        for _, b := range batch { w.file.Write(b) }
        w.file.Sync()
        w.mu.Unlock()
        batch = batch[:0]  // reset without realloc
    }

    for {
        select {
        case <-w.stopCh:   flush(); return
        case data := <-w.writeCh:
            batch = append(batch, data)
            if len(batch) >= w.options.MaxBatchSize { flush() }
        case <-ticker.C:   flush()
        }
    }
}
```

The flusher drains the `writeCh` channel. `AppendSet`/`AppendDelete` just push bytes into the channel and return immediately — the caller's goroutine never blocks on disk I/O.

### WAL Compaction

`Compact(filePath)` collapses a potentially huge append-only WAL into a minimal snapshot:
1. Opens a `.tmp` file.
2. Iterates all shards under `RLock` (reads can still happen concurrently).
3. For each non-expired key, writes a single `OpSet` frame.
4. Acquires the WAL mutex, closes the current WAL file, `os.Rename`s the tmp file over it (atomic on Linux).
5. Reopens the file for future appending.

Compaction is important because an append-only WAL grows without bound. After many Set/Delete cycles for the same keys, the log contains many redundant entries. Compaction reduces it to one entry per live key.

### Replay

```go
func (kv *KVStore) Replay(filepath string) error {
    // reads 15-byte headers in a loop
    // for OpSet: reads key+value, applies only if not yet expired
    // for OpDelete: deletes from in-memory map
}
```

After replay, `rebuildEviction()` recomputes `shard.bytes` and the LRU list from the replayed data, because the eviction callbacks weren't fired during replay.

---

## Eviction policies

**File:** `store/eviction.go`

### The Eviction interface

```go
type Eviction interface {
    InitShard(s *Shard)
    OnWrite(s *Shard, needed int64) bool  // return false = reject write
    AfterWrite(s *Shard, key string)
    OnRemove(s *Shard, key string)
    OnRead(s *Shard, key string)
}
```

The lock contract is documented in the interface comment: `OnRead` is the only method called without the shard lock held — the LRU implementation must acquire the write lock itself.

### NoEvict (default)

When `shard.budget == 0` (unlimited), `OnWrite` always returns true. When a budget is set and adding the new entry would exceed it, `OnWrite` returns false and the caller returns `ErrMemoryFull`. No keys are ever automatically removed.

**Interview question:** "What happens when memory is full with NoEvict?" — The write is rejected with an error. The client gets `ErrMemoryFull`. Existing data is preserved.

### SamplingEvict ("relaxed")

When the shard is over budget, randomly sample up to 5 keys (`sampleN = 5`) and evict the one with the earliest expiry. Repeat until there is room for the new entry. Always returns `true` — never rejects a write.

This is the same approximation Redis used for years. It is O(1) per eviction event (fixed-size sample), requires no extra data structures, and works well in practice because keys with the shortest remaining TTL are the most likely to expire soon anyway.

### LRUEvict ("strict")

Maintains an exact LRU order using:
- `shard.lru` — a `container/list.List` where the front is most-recently-used and the back is least-recently-used.
- `shard.lruKeys` — a `map[string]*list.Element` for O(1) lookup of a key's position in the list.

When eviction is needed, `lruEvictTail` removes the last element (LRU key) and deletes it from `shard.data`.

`OnRead` moves the accessed key to the front of the list to mark it as most recently used. Because a write lock is needed to mutate the list, reads are no longer purely concurrent when LRU is enabled — this is the documented trade-off.

**Interview question:** "Why does LRUEvict make Get more expensive?" — Get normally only needs a read lock. But updating the LRU list position requires a write lock. So with LRUEvict, every successful Get briefly re-acquires an exclusive lock on the shard.
