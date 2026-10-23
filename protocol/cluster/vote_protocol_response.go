package cluster

import (
	"encoding/binary"
	"key_value_store/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// VoteResponse — Peer replies to a vote request.
//
// Wire: [Term 8B] [Granted 1B]
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpVoteResponse, func() protocol.Message { return &VoteResponse{} })
}

type VoteResponse struct {
	Term    uint64
	Granted bool
}

func (v *VoteResponse) OpCode() byte { return protocol.OpVoteResponse }

func (v *VoteResponse) Encode() ([]byte, error) {
	buf := make([]byte, 9)
	binary.BigEndian.PutUint64(buf[0:8], v.Term)
	if v.Granted {
		buf[8] = 0x01
	}
	return buf, nil
}

func (v *VoteResponse) Decode(data []byte) error {
	if len(data) < 9 {
		return protocol.ErrPayloadTooShort
	}
	v.Term = binary.BigEndian.Uint64(data[0:8])
	v.Granted = data[8] == 0x01
	return nil
}
