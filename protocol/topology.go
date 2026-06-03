package protocol

import (
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// TopologyRequest — Client asks any node for the cluster layout.
//
// Wire format: (empty — the opcode alone is sufficient)
// ─────────────────────────────────────────────────────────────────────────────

type TopologyRequest struct{}

func (t *TopologyRequest) OpCode() byte         { return OpTopologyRequest }
func (t *TopologyRequest) Encode() ([]byte, error) { return []byte{}, nil }
func (t *TopologyRequest) Decode(data []byte) error { return nil }

// ─────────────────────────────────────────────────────────────────────────────
// TopologyResponse — Node replies with the full cluster map.
//
// Wire format:
//   [LeaderAddrLen 2B] [LeaderAddr ...]
//   [NodeCount 2B]
//   For each node:
//     [AddrLen 2B] [Addr ...]
//     [Role 1B]               (0x00 = Follower, 0x01 = Leader)
//     [NodeIDLen 2B] [NodeID ...]
//
// The client uses this to:
//   1. Direct all writes to the leader address
//   2. Pick a random follower for reads
// ─────────────────────────────────────────────────────────────────────────────

const (
	TopologyRoleFollower byte = 0x00
	TopologyRoleLeader   byte = 0x01
)

type NodeInfo struct {
	Address string
	Role    byte   // TopologyRoleFollower or TopologyRoleLeader
	NodeID  string
}

type TopologyResponse struct {
	LeaderAddr string
	Nodes      []NodeInfo
}

func (t *TopologyResponse) OpCode() byte { return OpTopologyResponse }

func (t *TopologyResponse) Encode() ([]byte, error) {
	// Calculate total size
	leaderBytes := []byte(t.LeaderAddr)
	size := 2 + len(leaderBytes) + 2 // leaderAddrLen + leaderAddr + nodeCount

	for _, n := range t.Nodes {
		addrBytes := []byte(n.Address)
		idBytes := []byte(n.NodeID)
		size += 2 + len(addrBytes) + 1 + 2 + len(idBytes) // addrLen + addr + role + idLen + id
	}

	buf := make([]byte, size)
	offset := 0

	// Leader address
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(leaderBytes)))
	offset += 2
	copy(buf[offset:], leaderBytes)
	offset += len(leaderBytes)

	// Node count
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(t.Nodes)))
	offset += 2

	// Each node
	for _, n := range t.Nodes {
		addrBytes := []byte(n.Address)
		idBytes := []byte(n.NodeID)

		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(addrBytes)))
		offset += 2
		copy(buf[offset:], addrBytes)
		offset += len(addrBytes)

		buf[offset] = n.Role
		offset++

		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(idBytes)))
		offset += 2
		copy(buf[offset:], idBytes)
		offset += len(idBytes)
	}

	return buf, nil
}

func (t *TopologyResponse) Decode(data []byte) error {
	if len(data) < 4 {
		return ErrPayloadTooShort
	}

	offset := 0

	// Leader address
	leaderLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if len(data) < offset+leaderLen {
		return ErrPayloadTooShort
	}
	t.LeaderAddr = string(data[offset : offset+leaderLen])
	offset += leaderLen

	// Node count
	if len(data) < offset+2 {
		return ErrPayloadTooShort
	}
	nodeCount := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	t.Nodes = make([]NodeInfo, 0, nodeCount)

	for i := 0; i < nodeCount; i++ {
		// Address
		if len(data) < offset+2 {
			return ErrPayloadTooShort
		}
		addrLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if len(data) < offset+addrLen {
			return ErrPayloadTooShort
		}
		addr := string(data[offset : offset+addrLen])
		offset += addrLen

		// Role
		if len(data) < offset+1 {
			return ErrPayloadTooShort
		}
		role := data[offset]
		offset++

		// NodeID
		if len(data) < offset+2 {
			return ErrPayloadTooShort
		}
		idLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if len(data) < offset+idLen {
			return ErrPayloadTooShort
		}
		nodeID := string(data[offset : offset+idLen])
		offset += idLen

		t.Nodes = append(t.Nodes, NodeInfo{
			Address: addr,
			Role:    role,
			NodeID:  nodeID,
		})
	}

	return nil
}
