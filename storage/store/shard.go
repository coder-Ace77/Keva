package store

import (
	"key_value_store/storage/eviction"
	"sync"
	"time"
)

// Record is the value part of each shard's hash map.
type Record struct {
	value     []byte
	createdAt time.Time
	expiredAt time.Time // ttl support
}

// Shard is one independently locked slice of the key space.
type Shard struct {
	mu     sync.RWMutex
	data   map[string]Record
	bytes  int64           // current memory: sum of len(key)+len(value)
	budget int64           // per-shard byte cap (MaxMemoryBytes/ShardCount); 0 = unlimited
	policy eviction.Policy // this shard's own eviction policy instance
}

// ── eviction.Shard view — callers must hold s.mu for writing ──────────────

func (s *Shard) Bytes() int64  { return s.bytes }
func (s *Shard) Budget() int64 { return s.budget }
func (s *Shard) Len() int      { return len(s.data) }

func (s *Shard) Range(fn func(key string, expiresAt time.Time) bool) {
	for key, rec := range s.data {
		if !fn(key, rec.expiredAt) {
			return
		}
	}
}

func (s *Shard) Evict(key string) {
	if rec, ok := s.data[key]; ok {
		s.bytes -= int64(len(key) + len(rec.value))
		delete(s.data, key)
	}
}
