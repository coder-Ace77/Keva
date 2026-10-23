package store

import (
	"key_value_store/storage/wal"
	"time"
)

// Background and maintenance work for KVStore: rebuilding from the WAL on
// startup, sweeping expired keys, and compacting the WAL.

// ── Recovery ────────────────────────────────────────────────────────────────

// recover rebuilds the shards from the WAL at walPath, then syncs byte
// counters and eviction bookkeeping with what was loaded. It runs before the
// store is handed out, so it does not lock the shards.
func (kv *KVStore) recover(walPath string) error {
	now := time.Now()

	err := wal.Replay(walPath, func(r wal.Record) {
		shard := kv.getShard(r.Key)
		switch r.Op {
		case wal.OpSet:
			// A value that expired while the server was off is not loaded,
			// and it still replaces whatever was there before it.
			if now.Before(r.ExpiresAt) {
				shard.data[r.Key] = Record{
					value:     r.Value,
					createdAt: now,
					expiredAt: r.ExpiresAt,
				}
			} else {
				delete(shard.data, r.Key)
			}
		case wal.OpDelete:
			delete(shard.data, r.Key)
		}
	})
	if err != nil {
		return err
	}

	for _, shard := range kv.shards {
		for key, rec := range shard.data {
			shard.bytes += int64(len(key) + len(rec.value))
			shard.policy.AfterWrite(key)
		}
	}
	return nil
}

// ── TTL expiry ──────────────────────────────────────────────────────────────

const ttlSweepInterval = 1 * time.Minute

// startTTLSweeper removes expired keys from every shard once per
// ttlSweepInterval. Get already hides expired keys; this reclaims their memory.
func (kv *KVStore) startTTLSweeper() {
	go func() {
		ticker := time.NewTicker(ttlSweepInterval)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			for _, shard := range kv.shards {
				shard.mu.Lock()
				for key, rec := range shard.data {
					if now.After(rec.expiredAt) {
						shard.policy.BeforeDelete(key)
						shard.bytes -= int64(len(key) + len(rec.value))
						delete(shard.data, key)
						shard.policy.AfterDelete(key)
					}
				}
				shard.mu.Unlock()
			}
		}
	}()
}

// ── Compaction ──────────────────────────────────────────────────────────────

// Compact rewrites the WAL so it holds one Set record per live key, dropping
// overwritten, deleted and expired history.
func (kv *KVStore) Compact() error {
	return kv.wal.Compact(func(write func(wal.Record) error) error {
		now := time.Now()
		for _, shard := range kv.shards {
			// RLock so GETs keep working while we compact.
			shard.mu.RLock()
			for key, rec := range shard.data {
				if now.After(rec.expiredAt) {
					continue
				}
				err := write(wal.Record{Op: wal.OpSet, Key: key, Value: rec.value, ExpiresAt: rec.expiredAt})
				if err != nil {
					shard.mu.RUnlock()
					return err
				}
			}
			shard.mu.RUnlock()
		}
		return nil
	})
}
