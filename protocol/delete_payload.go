package protocol

import (
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// DeletePayload — Client sends this to the leader to delete a key.
//
// Wire format:
//   [KeyLen 2B] [Key ...]
// ─────────────────────────────────────────────────────────────────────────────

type DeletePayload struct {
	Key []byte
}

func (d *DeletePayload) OpCode() byte { return OpDelete }

func (d *DeletePayload) Encode() ([]byte, error) {
	keyLen := len(d.Key)
	buf := make([]byte, 2+keyLen)
	binary.BigEndian.PutUint16(buf[0:2], uint16(keyLen))
	copy(buf[2:], d.Key)
	return buf, nil
}

func (d *DeletePayload) Decode(data []byte) error {
	if len(data) < 2 {
		return ErrPayloadTooShort
	}
	keyLen := binary.BigEndian.Uint16(data[0:2])
	if len(data) < 2+int(keyLen) {
		return ErrPayloadTooShort
	}
	d.Key = make([]byte, keyLen)
	copy(d.Key, data[2:2+keyLen])
	return nil
}
