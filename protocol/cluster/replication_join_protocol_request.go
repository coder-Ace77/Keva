package cluster

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationJoin — Follower sends this as its first message to the leader.
//
// Wire format:
//   [AddrLen 2B] [Addr ...]
//
// The leader uses NodeAddr as the key for tracking this follower. There is no
// response: the leader keeps the connection and starts streaming
// ReplicationSync messages on it.
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpReplicationJoin, func() protocol.Message { return &ReplicationJoin{} })
}

type ReplicationJoin struct {
	NodeAddr string // e.g. "localhost:7001"
}

func (r *ReplicationJoin) OpCode() byte { return protocol.OpReplicationJoin }

func (r *ReplicationJoin) Encode() ([]byte, error) {
	addrBytes := []byte(r.NodeAddr)
	buf := make([]byte, 2+len(addrBytes))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(addrBytes)))
	copy(buf[2:], addrBytes)
	return buf, nil
}

func (r *ReplicationJoin) Decode(data []byte) error {
	if len(data) < 2 {
		return protocol.ErrPayloadTooShort
	}
	addrLen := int(binary.BigEndian.Uint16(data[0:2]))
	if len(data) < 2+addrLen {
		return protocol.ErrPayloadTooShort
	}
	r.NodeAddr = string(data[2 : 2+addrLen])
	return nil
}
