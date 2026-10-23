package eviction

const (
	NameNoEvict = "noevict" // NoEvict
	NameRelaxed = "relaxed" // SamplingEvict
	NameStrict  = "strict"  // LRUEvict
)

func ByName(name string) Factory {
	switch name {
	case NameRelaxed:
		return func() Policy { return SamplingEvict{} }
	case NameStrict:
		return func() Policy { return NewLRUEvict() }
	default:
		return func() Policy { return NoEvict{} }
	}
}
