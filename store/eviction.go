package store

import (
	"container/list"
	"time"
)

// Eviction is the strategy interface every eviction policy implements.
//
// Call contract (who holds the shard write lock when each method is called):
//
//	InitShard  — called once at startup, no lock
//	OnWrite    — write lock IS held; free space here before the entry lands
//	AfterWrite — write lock IS held; update bookkeeping after the entry lands
//	OnRemove   — write lock IS held; clean up bookkeeping for a removed key
//	OnRead     — write lock is NOT held; policy acquires whatever it needs
type Eviction interface {
	// InitShard allocates per-shard state (e.g. an LRU list).
	InitShard(s *Shard)
	// OnWrite is called before a new entry is inserted.
	// needed = byte size of the incoming key+value.
	// Returns false to reject the write (used by NoEvict when full).
	OnWrite(s *Shard, needed int64) bool
	// AfterWrite is called immediately after the entry is stored.
	AfterWrite(s *Shard, key string)
	// OnRemove is called when a key leaves the shard for any reason
	// (explicit delete, TTL expiry, or eviction).
	OnRemove(s *Shard, key string)
	// OnRead is called after a successful Get. Implementations that update
	// access order (strict LRU) must re-acquire the write lock themselves.
	OnRead(s *Shard, key string)
}

// ── NoEvict ───────────────────────────────────────────────────────────────────

// NoEvict never removes entries to make room. When a memory budget is set and
// the shard is full it rejects the incoming write with ErrMemoryFull instead.
// With no budget (budget == 0) it behaves as if there is no memory limit.
type NoEvict struct{}

func (NoEvict) InitShard(_ *Shard)            {}
func (NoEvict) AfterWrite(_ *Shard, _ string) {}
func (NoEvict) OnRemove(_ *Shard, _ string)   {}
func (NoEvict) OnRead(_ *Shard, _ string)     {}

func (NoEvict) OnWrite(s *Shard, needed int64) bool {
	if s.budget == 0 {
		return true // unlimited
	}
	return s.bytes+needed <= s.budget
}

// ── SamplingEvict (relaxed) ───────────────────────────────────────────────────

// SamplingEvict approximates LRU with no per-key bookkeeping overhead.
// When the shard is over budget it samples up to sampleN random keys and
// evicts whichever one expires the soonest, repeating until there is room.
// This is the same approximation Redis used before it added true LRU clocks.
type SamplingEvict struct{}

const sampleN = 5

func (SamplingEvict) InitShard(_ *Shard)            {}
func (SamplingEvict) AfterWrite(_ *Shard, _ string) {}
func (SamplingEvict) OnRemove(_ *Shard, _ string)   {}
func (SamplingEvict) OnRead(_ *Shard, _ string)     {}

func (SamplingEvict) OnWrite(s *Shard, needed int64) bool {
	if s.budget == 0 {
		return true
	}
	for s.bytes+needed > s.budget && len(s.data) > 0 {
		sampleEvictOne(s)
	}
	return true // always makes room by evicting
}

func sampleEvictOne(s *Shard) {
	var victim string
	var victimExpiry time.Time
	count := 0
	for k, rec := range s.data {
		if count == 0 || rec.expiredAt.Before(victimExpiry) {
			victim = k
			victimExpiry = rec.expiredAt
		}
		count++
		if count >= sampleN {
			break
		}
	}
	if victim == "" {
		return
	}
	rec := s.data[victim]
	s.bytes -= int64(len(victim) + len(rec.value))
	delete(s.data, victim)
}

// ── LRUEvict (strict) ─────────────────────────────────────────────────────────

// LRUEvict maintains exact LRU order per shard using a doubly-linked list and
// a companion map. The tail of the list is always the least-recently-used key.
//
// Trade-off: OnRead must acquire the shard write lock to move the accessed
// key to the front, so Get is slightly more expensive than with the other
// two policies (read path no longer fully concurrent).
type LRUEvict struct{}

func (LRUEvict) InitShard(s *Shard) {
	s.lru = list.New()
	s.lruKeys = make(map[string]*list.Element)
}

func (LRUEvict) OnWrite(s *Shard, needed int64) bool {
	if s.budget == 0 {
		return true
	}
	for s.bytes+needed > s.budget && s.lru.Len() > 0 {
		lruEvictTail(s)
	}
	return true
}

func (LRUEvict) AfterWrite(s *Shard, key string) {
	if elem, ok := s.lruKeys[key]; ok {
		s.lru.MoveToFront(elem)
		return
	}
	elem := s.lru.PushFront(key)
	s.lruKeys[key] = elem
}

func (LRUEvict) OnRemove(s *Shard, key string) {
	if elem, ok := s.lruKeys[key]; ok {
		s.lru.Remove(elem)
		delete(s.lruKeys, key)
	}
}

// OnRead acquires the write lock itself; the caller must NOT hold any lock.
func (LRUEvict) OnRead(s *Shard, key string) {
	s.mu.Lock()
	if elem, ok := s.lruKeys[key]; ok {
		s.lru.MoveToFront(elem)
	}
	s.mu.Unlock()
}

func lruEvictTail(s *Shard) {
	tail := s.lru.Back()
	if tail == nil {
		return
	}
	key := tail.Value.(string)
	if rec, ok := s.data[key]; ok {
		s.bytes -= int64(len(key) + len(rec.value))
		delete(s.data, key)
	}
	s.lru.Remove(tail)
	delete(s.lruKeys, key)
}
