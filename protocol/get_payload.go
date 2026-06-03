package protocol

import (
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// GetPayload — Client sends this to any node to read a key.
//
// Wire format:
//   [KeyLen 2B] [Key ...]
// ─────────────────────────────────────────────────────────────────────────────

type GetPayload struct {
	Key []byte
}

func (g *GetPayload) OpCode() byte { return OpGet }

func (g *GetPayload) Encode() ([]byte, error) {
	keyLen := len(g.Key)
	buf := make([]byte, 2+keyLen)
	binary.BigEndian.PutUint16(buf[0:2], uint16(keyLen))
	copy(buf[2:], g.Key)
	return buf, nil
}

func (g *GetPayload) Decode(data []byte) error {
	if len(data) < 2 {
		return ErrPayloadTooShort
	}
	keyLen := binary.BigEndian.Uint16(data[0:2])
	if len(data) < 2+int(keyLen) {
		return ErrPayloadTooShort
	}
	g.Key = make([]byte, keyLen)
	copy(g.Key, data[2:2+keyLen])
	return nil
}
