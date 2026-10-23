package eviction

import "time"

// SamplingEvict ("relaxed") approximates LRU with no per-key bookkeeping.
// When the shard is over budget it samples up to sampleN keys and evicts
// whichever one expires the soonest, repeating until there is room.
// This is the same approximation Redis used before it added true LRU clocks.
type SamplingEvict struct{}

const sampleN = 5

func (SamplingEvict) BeforeWrite(s Shard, _ string, needed int64) bool {
	if s.Budget() == 0 {
		return true
	}
	for s.Bytes()+needed > s.Budget() && s.Len() > 0 {
		evictSoonestOfSample(s)
	}
	return true
}

func (SamplingEvict) AfterWrite(_ string)   {}
func (SamplingEvict) BeforeRead(_ string)   {}
func (SamplingEvict) AfterRead(_ string)    {}
func (SamplingEvict) BeforeDelete(_ string) {}
func (SamplingEvict) AfterDelete(_ string)  {}

func evictSoonestOfSample(s Shard) {
	var victim string
	var victimExpiry time.Time
	count := 0
	s.Range(func(key string, expiresAt time.Time) bool {
		if count == 0 || expiresAt.Before(victimExpiry) {
			victim = key
			victimExpiry = expiresAt
		}
		count++
		return count < sampleN
	})
	if count == 0 {
		return
	}
	s.Evict(victim)
}
