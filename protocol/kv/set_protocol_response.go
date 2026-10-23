package kv

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// SetResponse — Leader replies to a SET.
//
// Wire format:
//   [Success 1B] [MsgLen 2B] [Message ...]
//
// Success = 0x01 for OK, 0x00 for failure.
// Message contains a human-readable string (e.g., "OK" or an error reason).
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpSetResponse, func() protocol.Message { return &SetResponse{} })
}

type SetResponse struct {
	Success bool
	Message string
}

func (r *SetResponse) OpCode() byte { return protocol.OpSetResponse }

func (r *SetResponse) Encode() ([]byte, error) {
	msgBytes := []byte(r.Message)
	buf := make([]byte, 1+2+len(msgBytes))

	if r.Success {
		buf[0] = 0x01
	}
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(msgBytes)))
	copy(buf[3:], msgBytes)

	return buf, nil
}

func (r *SetResponse) Decode(data []byte) error {
	if len(data) < 3 {
		return protocol.ErrPayloadTooShort
	}
	r.Success = data[0] == 0x01
	msgLen := binary.BigEndian.Uint16(data[1:3])
	if len(data) < 3+int(msgLen) {
		return protocol.ErrPayloadTooShort
	}
	r.Message = string(data[3 : 3+msgLen])
	return nil
}
