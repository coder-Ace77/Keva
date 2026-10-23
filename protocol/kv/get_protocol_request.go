package kv

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// GetRequest — Client sends this to any node to read a key.
//
// Wire format:
//   [KeyLen 2B] [Key ...]
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpGet, func() protocol.Message { return &GetRequest{} })
}

type GetRequest struct {
	Key []byte
}

func (g *GetRequest) OpCode() byte { return protocol.OpGet }

func (g *GetRequest) Encode() ([]byte, error) {
	keyLen := len(g.Key)
	buf := make([]byte, 2+keyLen)
	binary.BigEndian.PutUint16(buf[0:2], uint16(keyLen))
	copy(buf[2:], g.Key)
	return buf, nil
}

func (g *GetRequest) Decode(data []byte) error {
	if len(data) < 2 {
		return protocol.ErrPayloadTooShort
	}
	keyLen := binary.BigEndian.Uint16(data[0:2])
	if len(data) < 2+int(keyLen) {
		return protocol.ErrPayloadTooShort
	}
	g.Key = make([]byte, keyLen)
	copy(g.Key, data[2:2+keyLen])
	return nil
}
