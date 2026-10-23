package kv

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// SetRequest — Client sends this to the leader to write a key-value pair.
//
// Wire format:
//   [KeyLen 2B] [ValueLen 4B] [TTL 8B] [Key ...] [Value ...]
//
// TTL is in nanoseconds (matches Go's time.Duration). A TTL of 0 means
// "use the server default".
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpSet, func() protocol.Message { return &SetRequest{} })
}

const setHeaderSize = 2 + 4 + 8 // keyLen + valLen + TTL = 14 bytes

type SetRequest struct {
	Key   []byte
	Value []byte
	TTL   uint64 // Nanoseconds. 0 = server default.
}

func (s *SetRequest) OpCode() byte { return protocol.OpSet }

func (s *SetRequest) Encode() ([]byte, error) {
	keyLen := len(s.Key)
	valLen := len(s.Value)

	buf := make([]byte, setHeaderSize+keyLen+valLen)

	binary.BigEndian.PutUint16(buf[0:2], uint16(keyLen))
	binary.BigEndian.PutUint32(buf[2:6], uint32(valLen))
	binary.BigEndian.PutUint64(buf[6:14], s.TTL)

	copy(buf[14:], s.Key)
	copy(buf[14+keyLen:], s.Value)

	return buf, nil
}

func (s *SetRequest) Decode(data []byte) error {
	if len(data) < setHeaderSize {
		return protocol.ErrPayloadTooShort
	}

	keyLen := binary.BigEndian.Uint16(data[0:2])
	valLen := binary.BigEndian.Uint32(data[2:6])
	s.TTL = binary.BigEndian.Uint64(data[6:14])

	if len(data) < setHeaderSize+int(keyLen)+int(valLen) {
		return protocol.ErrPayloadTooShort
	}

	s.Key = make([]byte, keyLen)
	copy(s.Key, data[14:14+keyLen])

	s.Value = make([]byte, valLen)
	copy(s.Value, data[14+keyLen:14+int(keyLen)+int(valLen)])

	return nil
}
