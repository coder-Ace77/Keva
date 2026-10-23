package cluster

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// Heartbeat — Leader periodically pings all followers.
//
// Wire: [Term 8B] [LeaderIDLen 2B] [LeaderID ...]
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpHeartbeat, func() protocol.Message { return &Heartbeat{} })
}

type Heartbeat struct {
	Term     uint64
	LeaderID string
}

func (h *Heartbeat) OpCode() byte { return protocol.OpHeartbeat }

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
		return protocol.ErrPayloadTooShort
	}
	h.Term = binary.BigEndian.Uint64(data[0:8])
	idLen := int(binary.BigEndian.Uint16(data[8:10]))
	if len(data) < 10+idLen {
		return protocol.ErrPayloadTooShort
	}
	h.LeaderID = string(data[10 : 10+idLen])
	return nil
}
