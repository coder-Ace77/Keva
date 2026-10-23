package store

import (
	"hash/fnv"
	"key_value_store/storage/eviction"
	"key_value_store/storage/wal"
	"time"
)

// Options controls the in-memory storage behaviour.
type Options struct {
	ShardCount     int              // number of hash shards (must be > 0)
	DefaultTTL     time.Duration    // TTL when the client sends TTL == 0
	MaxMemoryBytes int64            // total byte cap across all shards; 0 = unlimited
	NewEviction    eviction.Factory // one policy per shard; nil → NoEvict
}

// KVStore is a sharded in-memory map backed by a write-ahead log. Every write
// goes to the WAL first, then to memory.
type KVStore struct {
	shards     []*Shard
	wal        *wal.WAL
	shardCount uint32
	defaultTTL time.Duration
}

// NewKVStore rebuilds the in-memory state from the WAL at walPath, then opens
// that WAL for appending.
func NewKVStore(walPath string, walOpts wal.Options, opts Options) (*KVStore, error) {
	newPolicy := opts.NewEviction
	if newPolicy == nil {
		newPolicy = eviction.ByName(eviction.NameNoEvict)
	}

	budget := int64(0)
	if opts.MaxMemoryBytes > 0 {
		budget = opts.MaxMemoryBytes / int64(opts.ShardCount)
	}

	kv := &KVStore{
		shards:     make([]*Shard, opts.ShardCount),
		shardCount: uint32(opts.ShardCount),
		defaultTTL: opts.DefaultTTL,
	}
	for i := range kv.shards {
		kv.shards[i] = &Shard{
			data:   make(map[string]Record),
			budget: budget,
			policy: newPolicy(),
		}
	}

	if err := kv.recover(walPath); err != nil {
		return nil, err
	}

	w, err := wal.Open(walPath, walOpts)
	if err != nil {
		return nil, err
	}
	kv.wal = w

	kv.startTTLSweeper()
	return kv, nil
}

func (kv *KVStore) getShard(key string) *Shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return kv.shards[h.Sum32()%kv.shardCount]
}

// ── Writes ──────────────────────────────────────────────────────────────────

// Set stores key with the server's default TTL.
func (kv *KVStore) Set(key string, value []byte) error {
	return kv.SetWithExpiry(key, value, time.Now().Add(kv.defaultTTL))
}

// SetWithExpiry applies a write with a caller-supplied expiry time.
// Used by replication followers to mirror the leader's exact TTL.
func (kv *KVStore) SetWithExpiry(key string, value []byte, expiredAt time.Time) error {
	if err := kv.wal.AppendSet(key, value, expiredAt); err != nil {
		return err
	}

	valCopy := make([]byte, len(value))
	copy(valCopy, value)

	newSize := int64(len(key) + len(value))
	shard := kv.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// An overwrite replaces the old entry: take it out first so the policy
	// never sees (or evicts) the value that is being replaced.
	old, existed := shard.data[key]
	if existed {
		shard.bytes -= int64(len(key) + len(old.value))
		delete(shard.data, key)
	}

	// Let the eviction policy make room (or reject the write).
	if !shard.policy.BeforeWrite(shard, key, newSize) {
		if existed { // rejected: keep the old value
			shard.data[key] = old
			shard.bytes += int64(len(key) + len(old.value))
		}
		return ErrMemoryFull
	}

	shard.data[key] = Record{
		value:     valCopy,
		createdAt: time.Now(),
		expiredAt: expiredAt,
	}
	shard.bytes += newSize
	shard.policy.AfterWrite(key)

	return nil
}

// ── Reads ───────────────────────────────────────────────────────────────────

// Get returns a copy of key's value, or ErrKeyNotFound if it is missing or
// expired. Expired keys are left for the TTL sweeper to remove.
func (kv *KVStore) Get(key string) ([]byte, error) {
	shard := kv.getShard(key)
	shard.policy.BeforeRead(key)

	shard.mu.RLock()
	record, ok := shard.data[key]
	shard.mu.RUnlock()

	if !ok || time.Now().After(record.expiredAt) {
		return nil, ErrKeyNotFound
	}

	valCopy := make([]byte, len(record.value))
	copy(valCopy, record.value)

	// AfterRead runs after releasing the read lock. LRUEvict moves the key
	// to the front under its own mutex; NoEvict and SamplingEvict are no-ops.
	shard.policy.AfterRead(key)

	return valCopy, nil
}

// ── Deletes ─────────────────────────────────────────────────────────────────

// Delete removes key, or returns ErrKeyNotFound if it does not exist. The
// delete is logged to the WAL either way.
func (kv *KVStore) Delete(key string) error {
	if err := kv.wal.AppendDelete(key); err != nil {
		return err
	}

	shard := kv.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	rec, ok := shard.data[key]
	if !ok {
		return ErrKeyNotFound
	}
	shard.policy.BeforeDelete(key)
	shard.bytes -= int64(len(key) + len(rec.value))
	delete(shard.data, key)
	shard.policy.AfterDelete(key)
	return nil
}

// ── Lifecycle ───────────────────────────────────────────────────────────────

// Close flushes and closes the WAL.
func (kv *KVStore) Close() error {
	return kv.wal.Close()
}
