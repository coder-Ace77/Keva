package eviction

import "time"

// The Shard interface lives here, in the package that uses it, rather than in
// store: store imports eviction, so eviction cannot import store back.
// store.Shard satisfies this interface implicitly.

// Shard is the view of one storage shard that a policy may inspect and evict
// from. Every method must be called while the shard's write lock is held,
// which is always true inside Policy.BeforeWrite.
type Shard interface {
	// Bytes is the memory currently used: sum of len(key)+len(value).
	Bytes() int64
	// Budget is this shard's byte cap; 0 means unlimited.
	Budget() int64
	// Len is the number of keys in the shard.
	Len() int
	// Range calls fn for entries in map order until fn returns false.
	Range(fn func(key string, expiresAt time.Time) bool)
	// Evict removes key and its bytes from the shard. It does NOT call back
	// into the policy — the policy updates its own bookkeeping itself.
	Evict(key string)
}
