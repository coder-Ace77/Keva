package kv

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// DeleteRequest — Client sends this to the leader to delete a key.
//
// Wire format:
//   [KeyLen 2B] [Key ...]
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpDelete, func() protocol.Message { return &DeleteRequest{} })
}

type DeleteRequest struct {
	Key []byte
}

func (d *DeleteRequest) OpCode() byte { return protocol.OpDelete }

func (d *DeleteRequest) Encode() ([]byte, error) {
	keyLen := len(d.Key)
	buf := make([]byte, 2+keyLen)
	binary.BigEndian.PutUint16(buf[0:2], uint16(keyLen))
	copy(buf[2:], d.Key)
	return buf, nil
}

func (d *DeleteRequest) Decode(data []byte) error {
	if len(data) < 2 {
		return protocol.ErrPayloadTooShort
	}
	keyLen := binary.BigEndian.Uint16(data[0:2])
	if len(data) < 2+int(keyLen) {
		return protocol.ErrPayloadTooShort
	}
	d.Key = make([]byte, keyLen)
	copy(d.Key, data[2:2+keyLen])
	return nil
}
