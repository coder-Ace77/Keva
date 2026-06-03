package protocol

import (
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// ForwardWrite — Follower wraps a client's write and sends it to the leader.
//
// Wire: [OriginalOpCode 1B] [PayloadLen 4B] [OriginalPayload ...]
//
// The leader unwraps this, processes the original SET/DELETE, and responds.
// ─────────────────────────────────────────────────────────────────────────────

type ForwardWrite struct {
	OriginalOpCode byte
	Payload        []byte // The raw encoded payload of the original message
}

func (f *ForwardWrite) OpCode() byte { return OpForwardWrite }

func (f *ForwardWrite) Encode() ([]byte, error) {
	buf := make([]byte, 1+4+len(f.Payload))
	buf[0] = f.OriginalOpCode
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(f.Payload)))
	copy(buf[5:], f.Payload)
	return buf, nil
}

func (f *ForwardWrite) Decode(data []byte) error {
	if len(data) < 5 {
		return ErrPayloadTooShort
	}
	f.OriginalOpCode = data[0]
	payloadLen := int(binary.BigEndian.Uint32(data[1:5]))
	if len(data) < 5+payloadLen {
		return ErrPayloadTooShort
	}
	f.Payload = make([]byte, payloadLen)
	copy(f.Payload, data[5:5+payloadLen])
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ForwardWriteResponse — Leader responds to a forwarded write.
//
// Wire: [Success 1B] [ResponseOpCode 1B] [ResponseLen 4B] [ResponsePayload ...]
// ─────────────────────────────────────────────────────────────────────────────

type ForwardWriteResponse struct {
	Success        bool
	ResponseOpCode byte
	Payload        []byte // The raw encoded response payload
}

func (f *ForwardWriteResponse) OpCode() byte { return OpForwardWriteResponse }

func (f *ForwardWriteResponse) Encode() ([]byte, error) {
	buf := make([]byte, 1+1+4+len(f.Payload))
	if f.Success {
		buf[0] = 0x01
	} else {
		buf[0] = 0x00
	}
	buf[1] = f.ResponseOpCode
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(f.Payload)))
	copy(buf[6:], f.Payload)
	return buf, nil
}

func (f *ForwardWriteResponse) Decode(data []byte) error {
	if len(data) < 6 {
		return ErrPayloadTooShort
	}
	f.Success = data[0] == 0x01
	f.ResponseOpCode = data[1]
	payloadLen := int(binary.BigEndian.Uint32(data[2:6]))
	if len(data) < 6+payloadLen {
		return ErrPayloadTooShort
	}
	f.Payload = make([]byte, payloadLen)
	copy(f.Payload, data[6:6+payloadLen])
	return nil
}
