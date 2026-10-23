package protocol

import (
	"encoding/binary"
	"errors"
)

// ─────────────────────────────────────────────────────────────────────────────
// Decode errors — returned by Decode / DecodeMessage / ReadFrame.
// ─────────────────────────────────────────────────────────────────────────────

var (
	ErrUnknownOpCode    = errors.New("protocol: unknown opcode")
	ErrPayloadTooShort  = errors.New("protocol: payload too short")
	ErrFrameTooLarge    = errors.New("protocol: frame too large")
	ErrMalformedMessage = errors.New("protocol: malformed message")
)

// ─────────────────────────────────────────────────────────────────────────────
// Well-known error codes carried inside ErrorMessage.
// ─────────────────────────────────────────────────────────────────────────────

const (
	ErrCodeNotLeader   uint16 = 1 // Write was sent to a follower
	ErrCodeKeyNotFound uint16 = 2 // Key does not exist
	ErrCodeInternal    uint16 = 3 // Unexpected server error
	ErrCodeBadRequest  uint16 = 4 // Malformed message
	ErrCodeTimeout     uint16 = 5 // Operation timed out
)

// ─────────────────────────────────────────────────────────────────────────────
// ErrorMessage — Structured error that any node can send.
//
// Wire: [Code 2B] [MsgLen 2B] [Message ...]
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	Register(OpError, func() Message { return &ErrorMessage{} })
}

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
