package store

import (
	"container/list"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"sync"
	"time"
)

// This structure is for the value part of hashmaps
type Record struct {
	value     []byte
	createdAt time.Time
	expiredAt time.Time // ttl support
}

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrMemoryFull  = errors.New("memory limit reached: write rejected (noevict policy)")
)

// StoreOptions controls the in-memory storage behaviour.
type StoreOptions struct {
	ShardCount     int           // number of hash shards (must be > 0)
	DefaultTTL     time.Duration // TTL when the client sends TTL == 0
	MaxMemoryBytes int64         // total byte cap across all shards; 0 = unlimited
	Eviction       Eviction      // nil → NoEvict
}

type Shard struct {
	mu     sync.RWMutex
	data   map[string]Record
	bytes  int64                    // current memory: sum of len(key)+len(value)
	budget int64                    // per-shard byte cap (MaxMemoryBytes/ShardCount); 0 = unlimited
	// populated only in strict LRU mode
	lru     *list.List
	lruKeys map[string]*list.Element
}

type KVStore struct {
	shards     []*Shard
	wal        *WAL
	shardCount uint32
	defaultTTL time.Duration
	eviction   Eviction
}

func NewKVStore(filePath string, walOpts WALOptions, storeOpts StoreOptions) (*KVStore, error) {
	wal, err := NewWal(filePath, walOpts)
	if err != nil {
		return nil, err
	}

	ev := storeOpts.Eviction
	if ev == nil {
		ev = NoEvict{}
	}

	budget := int64(0)
	if storeOpts.MaxMemoryBytes > 0 {
		budget = storeOpts.MaxMemoryBytes / int64(storeOpts.ShardCount)
	}

	store := &KVStore{
		shards:     make([]*Shard, storeOpts.ShardCount),
		wal:        wal,
		shardCount: uint32(storeOpts.ShardCount),
		defaultTTL: storeOpts.DefaultTTL,
		eviction:   ev,
	}

	for i := 0; i < storeOpts.ShardCount; i++ {
		s := &Shard{
			data:   make(map[string]Record),
			budget: budget,
		}
		ev.InitShard(s)
		store.shards[i] = s
	}

	if err := store.Replay(filePath); err != nil {
		return nil, err
	}
	// Sync byte counters and eviction bookkeeping with replayed data.
	store.rebuildEviction()

	wal, err = NewWal(filePath, walOpts)
	if err != nil {
		return nil, err
	}
	store.wal = wal

	store.startGC()
	return store, nil
}

func (kv *KVStore) getShard(key string) *Shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	shardIndex := h.Sum32() % kv.shardCount
	return kv.shards[shardIndex]
}

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

	// If the key already exists, subtract the old footprint and remove it
	// from eviction bookkeeping before we overwrite.
	if old, exists := shard.data[key]; exists {
		shard.bytes -= int64(len(key) + len(old.value))
		kv.eviction.OnRemove(shard, key)
	}

	// Let the eviction policy make room (or reject the write).
	if !kv.eviction.OnWrite(shard, newSize) {
		return ErrMemoryFull
	}

	shard.data[key] = Record{
		value:     valCopy,
		createdAt: time.Now(),
		expiredAt: expiredAt,
	}
	shard.bytes += newSize
	kv.eviction.AfterWrite(shard, key)

	return nil
}

func (kv *KVStore) Get(key string) ([]byte, error) {
	shard := kv.getShard(key)

	shard.mu.RLock()
	record, ok := shard.data[key]
	shard.mu.RUnlock()

	if !ok || time.Now().After(record.expiredAt) {
		return nil, ErrKeyNotFound
	}

	valCopy := make([]byte, len(record.value))
	copy(valCopy, record.value)

	// OnRead is called after releasing the read lock.
	// LRUEvict will re-acquire the write lock internally to move the key
	// to the front of the list. NoEvict and SamplingEvict are no-ops.
	kv.eviction.OnRead(shard, key)

	return valCopy, nil
}

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
	shard.bytes -= int64(len(key) + len(rec.value))
	kv.eviction.OnRemove(shard, key)
	delete(shard.data, key)
	return nil
}

func (kv *KVStore) startGC() {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			for _, shard := range kv.shards {
				shard.mu.Lock()
				for key, rec := range shard.data {
					if now.After(rec.expiredAt) {
						shard.bytes -= int64(len(key) + len(rec.value))
						kv.eviction.OnRemove(shard, key)
						delete(shard.data, key)
					}
				}
				shard.mu.Unlock()
			}
		}
	}()
}

// rebuildEviction recomputes byte counters and repopulates eviction
// bookkeeping (e.g. the LRU list) from the data that was just replayed
// from the WAL. Must be called before the store is opened for writes.
func (kv *KVStore) rebuildEviction() {
	for _, shard := range kv.shards {
		shard.mu.Lock()
		for key, rec := range shard.data {
			shard.bytes += int64(len(key) + len(rec.value))
			kv.eviction.AfterWrite(shard, key)
		}
		shard.mu.Unlock()
	}
}

func (kv *KVStore) Replay(filepath string) error {
	file, err := os.Open(filepath)

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	defer file.Close()

	header := make([]byte, 15)

	for {

		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		op := header[0]
		keyLen := binary.BigEndian.Uint16(header[1:3])
		valLen := binary.BigEndian.Uint32(header[3:7])
		expiredUnix := binary.BigEndian.Uint64(header[7:15])
		expiredAt := time.Unix(0, int64(expiredUnix))

		keyBuf := make([]byte, keyLen)

		if _, err := io.ReadFull(file, keyBuf); err != nil {
			return err
		}

		key := string(keyBuf)
		var valBuf []byte

		if op == OpSet {
			valBuf = make([]byte, valLen)
			if _, err := io.ReadFull(file, valBuf); err != nil {
				return err
			}
		}

		shard := kv.getShard(key)
		shard.mu.Lock()

		if op == OpSet {
			// Only load it if it hasn't already expired while the server was off!
			if time.Now().Before(expiredAt) {
				shard.data[key] = Record{
					value:     valBuf,
					createdAt: time.Now(),
					expiredAt: expiredAt,
				}
			}
		} else if op == OpDelete {
			delete(shard.data, key)
		}

		shard.mu.Unlock()
	}

	return nil

}

// Compact takes the current memory state and compresses it into a brand new WAL file,
// discarding all the historical operations we no longer need.
func (kv *KVStore) Compact(filePath string) error {
	tmpPath := filePath + ".tmp"

	// 1. Create a fresh temporary file
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	// 2. Iterate over our 32 shards and dump active keys
	for _, shard := range kv.shards {
		shard.mu.RLock() // Use RLock so users can still GET data while we compact!

		for key, record := range shard.data {
			// Don't save expired keys to the new snapshot
			if time.Now().After(record.expiredAt) {
				continue
			}

			// Re-use our binary framing logic
			keyLen := uint16(len(key))
			valLen := uint32(len(record.value))
			size := 1 + 2 + 4 + 8 + len(key) + len(record.value)
			buf := make([]byte, size)

			buf[0] = OpSet
			binary.BigEndian.PutUint16(buf[1:3], keyLen)
			binary.BigEndian.PutUint32(buf[3:7], valLen)
			binary.BigEndian.PutUint64(buf[7:15], uint64(record.expiredAt.UnixNano()))
			copy(buf[15:], key)
			copy(buf[15+len(key):], record.value)

			tmpFile.Write(buf)
		}

		shard.mu.RUnlock()
	}

	tmpFile.Sync()
	tmpFile.Close()

	// 3. The Critical Swap
	// We must lock the WAL so no new writes happen while we swap the files
	kv.wal.mu.Lock()
	defer kv.wal.mu.Unlock()

	// Close the current active log
	kv.wal.file.Close()

	// Atomically replace the giant old WAL with our tiny new compacted WAL
	if err := os.Rename(tmpPath, filePath); err != nil {
		return err
	}

	// Reopen the newly compacted file for future appending
	newFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	kv.wal.file = newFile

	return nil
}

// Close gracefully shuts down the database and closes the disk file.
func (kv *KVStore) Close() error {
	return kv.wal.Close()
}
