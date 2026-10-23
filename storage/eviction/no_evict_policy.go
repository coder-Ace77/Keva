package eviction

// NoEvict never removes entries to make room. When a memory budget is set and
// the shard is full it rejects the incoming write instead (the store turns
// that into ErrMemoryFull). With no budget it behaves as if there is no
// memory limit.
type NoEvict struct{}

func (NoEvict) BeforeWrite(s Shard, _ string, needed int64) bool {
	if s.Budget() == 0 {
		return true // unlimited
	}
	return s.Bytes()+needed <= s.Budget()
}

func (NoEvict) AfterWrite(_ string)   {}
func (NoEvict) BeforeRead(_ string)   {}
func (NoEvict) AfterRead(_ string)    {}
func (NoEvict) BeforeDelete(_ string) {}
func (NoEvict) AfterDelete(_ string)  {}
