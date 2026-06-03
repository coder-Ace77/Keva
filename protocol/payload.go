package protocol

import (
	"errors"
	"fmt"
	"sync"
)

// ─────────────────────────────────────────────────────────────────────────────
// Message Interface
//
// Every protocol message (GET, SET, Heartbeat, ReplicationSync, etc.)
// implements this interface. The OpCode identifies the type on the wire,
// Encode serializes the payload, and Decode deserializes it.
// ─────────────────────────────────────────────────────────────────────────────

type Message interface {
	OpCode() byte
	Encode() ([]byte, error)
	Decode(data []byte) error
}

// ─────────────────────────────────────────────────────────────────────────────
// Registry — The core of the pattern.
//
// Instead of a giant switch statement, we register each message type once.
// Decoding becomes: read the opcode → look up the factory → construct → decode.
// Adding a new message type = one Register() call + a struct. Zero changes to
// the decode path.
// ─────────────────────────────────────────────────────────────────────────────

var (
	registryMu sync.RWMutex
	registry   = make(map[byte]func() Message)
)

var (
	ErrUnknownOpCode  = errors.New("protocol: unknown opcode")
	ErrPayloadTooShort = errors.New("protocol: payload too short")
	ErrInvalidPayload  = errors.New("protocol: invalid payload")
)

// Register maps an opcode to a factory function that creates an empty Message
// of that type. Panics on duplicate registration to catch bugs at init time.
func Register(opcode byte, factory func() Message) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[opcode]; exists {
		panic(fmt.Sprintf("protocol: duplicate registration for opcode 0x%02X", opcode))
	}
	registry[opcode] = factory
}

// Lookup returns a fresh Message instance for the given opcode.
func Lookup(opcode byte) (Message, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	factory, ok := registry[opcode]
	if !ok {
		return nil, false
	}
	return factory(), true
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire helpers
//
// EncodeMessage: [OpCode 1B] [Payload ...]
// DecodeMessage: reads opcode, looks up factory, decodes payload.
//
// The caller is responsible for wrapping this in the TCP length-prefix frame:
//   [TotalLength 4B] [OpCode 1B] [Payload ...]
// ─────────────────────────────────────────────────────────────────────────────

// EncodeMessage serializes a Message to wire format: [OpCode 1B][Payload...].
func EncodeMessage(msg Message) ([]byte, error) {
	payload, err := msg.Encode()
	if err != nil {
		return nil, fmt.Errorf("protocol: encode error for opcode 0x%02X: %w", msg.OpCode(), err)
	}

	// 1 byte for opcode + payload
	wire := make([]byte, 1+len(payload))
	wire[0] = msg.OpCode()
	copy(wire[1:], payload)
	return wire, nil
}

// DecodeMessage deserializes wire bytes back into a typed Message.
// Input must be [OpCode 1B][Payload...] (no length prefix).
func DecodeMessage(data []byte) (Message, error) {
	if len(data) < 1 {
		return nil, ErrPayloadTooShort
	}

	opcode := data[0]
	msg, ok := Lookup(opcode)
	if !ok {
		return nil, fmt.Errorf("%w: 0x%02X", ErrUnknownOpCode, opcode)
	}

	if err := msg.Decode(data[1:]); err != nil {
		return nil, fmt.Errorf("protocol: decode error for opcode 0x%02X: %w", opcode, err)
	}

	return msg, nil
}
