package cluster

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// HeartbeatAck — Follower responds with term and replication progress.
//
// Wire: [Term 8B] [LastAppliedIndex 8B]
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpHeartbeatAck, func() protocol.Message { return &HeartbeatAck{} })
}

type HeartbeatAck struct {
	Term             uint64
	LastAppliedIndex uint64
}

func (h *HeartbeatAck) OpCode() byte { return protocol.OpHeartbeatAck }

func (h *HeartbeatAck) Encode() ([]byte, error) {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], h.Term)
	binary.BigEndian.PutUint64(buf[8:16], h.LastAppliedIndex)
	return buf, nil
}

func (h *HeartbeatAck) Decode(data []byte) error {
	if len(data) < 16 {
		return protocol.ErrPayloadTooShort
	}
	h.Term = binary.BigEndian.Uint64(data[0:8])
	h.LastAppliedIndex = binary.BigEndian.Uint64(data[8:16])
	return nil
}
