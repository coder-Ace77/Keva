package protocol

import (
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// Heartbeat — Leader periodically pings all followers.
//
// Wire: [Term 8B] [LeaderIDLen 2B] [LeaderID ...]
// ─────────────────────────────────────────────────────────────────────────────

type Heartbeat struct {
	Term     uint64
	LeaderID string
}

func (h *Heartbeat) OpCode() byte { return OpHeartbeat }

func (h *Heartbeat) Encode() ([]byte, error) {
	idBytes := []byte(h.LeaderID)
	buf := make([]byte, 8+2+len(idBytes))
	binary.BigEndian.PutUint64(buf[0:8], h.Term)
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(idBytes)))
	copy(buf[10:], idBytes)
	return buf, nil
}

func (h *Heartbeat) Decode(data []byte) error {
	if len(data) < 10 {
		return ErrPayloadTooShort
	}
	h.Term = binary.BigEndian.Uint64(data[0:8])
	idLen := int(binary.BigEndian.Uint16(data[8:10]))
	if len(data) < 10+idLen {
		return ErrPayloadTooShort
	}
	h.LeaderID = string(data[10 : 10+idLen])
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// HeartbeatAck — Follower responds with term and replication progress.
//
// Wire: [Term 8B] [LastAppliedIndex 8B]
// ─────────────────────────────────────────────────────────────────────────────

type HeartbeatAck struct {
	Term             uint64
	LastAppliedIndex uint64
}

func (h *HeartbeatAck) OpCode() byte { return OpHeartbeatAck }

func (h *HeartbeatAck) Encode() ([]byte, error) {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], h.Term)
	binary.BigEndian.PutUint64(buf[8:16], h.LastAppliedIndex)
	return buf, nil
}

func (h *HeartbeatAck) Decode(data []byte) error {
	if len(data) < 16 {
		return ErrPayloadTooShort
	}
	h.Term = binary.BigEndian.Uint64(data[0:8])
	h.LastAppliedIndex = binary.BigEndian.Uint64(data[8:16])
	return nil
}
