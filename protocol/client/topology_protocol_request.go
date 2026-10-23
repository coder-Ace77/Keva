package client

import "key_value_store/protocol"

// ─────────────────────────────────────────────────────────────────────────────
// TopologyRequest — Client asks any node for the cluster layout.
//
// Wire format: (empty — the opcode alone is sufficient)
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	protocol.Register(protocol.OpTopologyRequest, func() protocol.Message { return &TopologyRequest{} })
}

type TopologyRequest struct{}

func (t *TopologyRequest) OpCode() byte             { return protocol.OpTopologyRequest }
func (t *TopologyRequest) Encode() ([]byte, error)  { return []byte{}, nil }
func (t *TopologyRequest) Decode(data []byte) error { return nil }
