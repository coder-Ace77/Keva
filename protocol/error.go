package protocol

import (
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// ErrorMessage — Structured error that any node can send.
//
// Wire: [Code 2B] [MsgLen 2B] [Message ...]
//
// Standard error codes are defined in opcodes.go (ErrCodeNotLeader, etc.)
// ─────────────────────────────────────────────────────────────────────────────

type ErrorMessage struct {
	Code    uint16
	Message string
}

func (e *ErrorMessage) OpCode() byte { return OpError }

func (e *ErrorMessage) Encode() ([]byte, error) {
	msgBytes := []byte(e.Message)
	buf := make([]byte, 2+2+len(msgBytes))
	binary.BigEndian.PutUint16(buf[0:2], e.Code)
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(msgBytes)))
	copy(buf[4:], msgBytes)
	return buf, nil
}

func (e *ErrorMessage) Decode(data []byte) error {
	if len(data) < 4 {
		return ErrPayloadTooShort
	}
	e.Code = binary.BigEndian.Uint16(data[0:2])
	msgLen := int(binary.BigEndian.Uint16(data[2:4]))
	if len(data) < 4+msgLen {
		return ErrPayloadTooShort
	}
	e.Message = string(data[4 : 4+msgLen])
	return nil
}
