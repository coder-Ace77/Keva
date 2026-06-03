package protocol

import (
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationSync — Leader sends WAL entries to followers.
//
// Wire format:
//   [LogIndex 8B] [Op 1B] [KeyLen 2B] [ValueLen 4B] [TTL 8B] [Key ...] [Value ...]
//
// This mirrors the WAL binary frame format with the LogIndex prepended.
// The follower applies this entry to its local store and WAL.
// ─────────────────────────────────────────────────────────────────────────────

const replicationSyncHeaderSize = 8 + 1 + 2 + 4 + 8 // logIndex + op + keyLen + valLen + ttl = 23 bytes

type ReplicationSync struct {
	LogIndex uint64
	Op       byte   // OpSet (from WAL) or OpDelete (from WAL) — re-uses store-level ops
	Key      []byte
	Value    []byte // Empty for deletes
	TTL      uint64 // Nanoseconds, 0 for deletes
}

func (r *ReplicationSync) OpCode() byte { return OpReplicationSync }

func (r *ReplicationSync) Encode() ([]byte, error) {
	keyLen := len(r.Key)
	valLen := len(r.Value)

	buf := make([]byte, replicationSyncHeaderSize+keyLen+valLen)

	binary.BigEndian.PutUint64(buf[0:8], r.LogIndex)
	buf[8] = r.Op
	binary.BigEndian.PutUint16(buf[9:11], uint16(keyLen))
	binary.BigEndian.PutUint32(buf[11:15], uint32(valLen))
	binary.BigEndian.PutUint64(buf[15:23], r.TTL)

	copy(buf[23:], r.Key)
	copy(buf[23+keyLen:], r.Value)

	return buf, nil
}

func (r *ReplicationSync) Decode(data []byte) error {
	if len(data) < replicationSyncHeaderSize {
		return ErrPayloadTooShort
	}

	r.LogIndex = binary.BigEndian.Uint64(data[0:8])
	r.Op = data[8]
	keyLen := int(binary.BigEndian.Uint16(data[9:11]))
	valLen := int(binary.BigEndian.Uint32(data[11:15]))
	r.TTL = binary.BigEndian.Uint64(data[15:23])

	if len(data) < replicationSyncHeaderSize+keyLen+valLen {
		return ErrPayloadTooShort
	}

	r.Key = make([]byte, keyLen)
	copy(r.Key, data[23:23+keyLen])

	r.Value = make([]byte, valLen)
	copy(r.Value, data[23+keyLen:23+keyLen+valLen])

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationAck — Follower tells the leader how far it's caught up.
//
// Wire format:
//   [LastAppliedIndex 8B]
//
// The leader uses this to track replication lag per follower.
// ─────────────────────────────────────────────────────────────────────────────

type ReplicationAck struct {
	LastAppliedIndex uint64
}

func (r *ReplicationAck) OpCode() byte { return OpReplicationAck }

func (r *ReplicationAck) Encode() ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:8], r.LastAppliedIndex)
	return buf, nil
}

func (r *ReplicationAck) Decode(data []byte) error {
	if len(data) < 8 {
		return ErrPayloadTooShort
	}
	r.LastAppliedIndex = binary.BigEndian.Uint64(data[0:8])
	return nil
}
