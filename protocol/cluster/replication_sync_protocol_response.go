package cluster

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationAck — Follower tells the leader how far it's caught up.
//
// Wire format:
//   [LastAppliedIndex 8B]
//
// Sent once per applied ReplicationSync.
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpReplicationAck, func() protocol.Message { return &ReplicationAck{} })
}

type ReplicationAck struct {
	LastAppliedIndex uint64
}

func (r *ReplicationAck) OpCode() byte { return protocol.OpReplicationAck }

func (r *ReplicationAck) Encode() ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:8], r.LastAppliedIndex)
	return buf, nil
}

func (r *ReplicationAck) Decode(data []byte) error {
	if len(data) < 8 {
		return protocol.ErrPayloadTooShort
	}
	r.LastAppliedIndex = binary.BigEndian.Uint64(data[0:8])
	return nil
}
