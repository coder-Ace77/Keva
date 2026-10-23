package eviction

// Policy decides what to drop when a shard runs out of memory. The store
// calls a Before/After hook pair around every write, read and delete; a
// policy implements the hooks it cares about and leaves the rest empty.
//
// such as an LRU list lives inside the policy, not on the shard.
//
// Call contract (who holds the shard write lock when each hook runs):
//
//	BeforeWrite  — write lock IS held; make room (s.Evict) or reject
//	AfterWrite   — write lock IS held; the key has been stored
//	BeforeRead   — no lock held
//	AfterRead    — no lock held; only called when the key was found
//	BeforeDelete — write lock IS held; the key is about to be removed
//	AfterDelete  — write lock IS held; the key has been removed
//
// "Write" includes overwrites: the store takes the old entry out of the shard
// before BeforeWrite, so a policy never sees or evicts the value being
// replaced. "Delete" means an explicit DEL or a TTL expiry; keys the policy
// evicts itself through Shard.Evict do not trigger the delete hooks.
type Policy interface {
	// BeforeWrite is called before key is stored.
	// needed = byte size of the incoming key+value.
	// Returns false to reject the write.
	BeforeWrite(s Shard, key string, needed int64) bool
	// AfterWrite is called immediately after the entry is stored.
	AfterWrite(key string)

	// BeforeRead is called before the key is looked up.
	BeforeRead(key string)
	// AfterRead is called after a successful Get.
	AfterRead(key string)

	// BeforeDelete is called before an existing key is removed.
	BeforeDelete(key string)
	// AfterDelete is called after the key has been removed.
	AfterDelete(key string)
}

// Factory creates a fresh Policy for one shard.
type Factory func() Policy
