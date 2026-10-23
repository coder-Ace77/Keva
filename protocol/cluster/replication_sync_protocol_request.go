package cluster

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationSync — Leader streams one write to a follower.
//
// Wire format:
//   [LogIndex 8B] [Op 1B] [KeyLen 2B] [ValueLen 4B] [ExpiresAt 8B] [Key ...] [Value ...]
//
// This mirrors the WAL binary frame format with the LogIndex prepended.
// The follower applies this entry to its local store and WAL.
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpReplicationSync, func() protocol.Message { return &ReplicationSync{} })
}

const replicationSyncHeaderSize = 8 + 1 + 2 + 4 + 8 // logIndex + op + keyLen + valLen + expiresAt = 23 bytes

type ReplicationSync struct {
	LogIndex  uint64
	Op        byte // wal.OpSet or wal.OpDelete — re-uses WAL-level ops
	Key       []byte
	Value     []byte // Empty for deletes
	ExpiresAt uint64 // Absolute expiry, Unix nanoseconds. 0 for deletes.
}

func (r *ReplicationSync) OpCode() byte { return protocol.OpReplicationSync }

func (r *ReplicationSync) Encode() ([]byte, error) {
	keyLen := len(r.Key)
	valLen := len(r.Value)

	buf := make([]byte, replicationSyncHeaderSize+keyLen+valLen)

	binary.BigEndian.PutUint64(buf[0:8], r.LogIndex)
	buf[8] = r.Op
	binary.BigEndian.PutUint16(buf[9:11], uint16(keyLen))
	binary.BigEndian.PutUint32(buf[11:15], uint32(valLen))
	binary.BigEndian.PutUint64(buf[15:23], r.ExpiresAt)

	copy(buf[23:], r.Key)
	copy(buf[23+keyLen:], r.Value)

	return buf, nil
}

func (r *ReplicationSync) Decode(data []byte) error {
	if len(data) < replicationSyncHeaderSize {
		return protocol.ErrPayloadTooShort
	}

	r.LogIndex = binary.BigEndian.Uint64(data[0:8])
	r.Op = data[8]
	keyLen := int(binary.BigEndian.Uint16(data[9:11]))
	valLen := int(binary.BigEndian.Uint32(data[11:15]))
	r.ExpiresAt = binary.BigEndian.Uint64(data[15:23])

	if len(data) < replicationSyncHeaderSize+keyLen+valLen {
		return protocol.ErrPayloadTooShort
	}

	r.Key = make([]byte, keyLen)
	copy(r.Key, data[23:23+keyLen])

	r.Value = make([]byte, valLen)
	copy(r.Value, data[23+keyLen:23+keyLen+valLen])

	return nil
}
