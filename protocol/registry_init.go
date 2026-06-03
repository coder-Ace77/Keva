package protocol

// ─────────────────────────────────────────────────────────────────────────────
// Registry Init — All message types are registered here via init().
//
// This file is the single source of truth for the opcode ↔ type mapping.
// Adding a new message type? Add one line here.
// ─────────────────────────────────────────────────────────────────────────────

func init() {
	// Client ↔ Node: CRUD
	Register(OpGet, func() Message { return &GetPayload{} })
	Register(OpSet, func() Message { return &SetPayload{} })
	Register(OpDelete, func() Message { return &DeletePayload{} })
	Register(OpGetResponse, func() Message { return &GetResponse{} })
	Register(OpSetResponse, func() Message { return &SetResponse{} })
	Register(OpDeleteResponse, func() Message { return &DeleteResponse{} })

	// Topology discovery
	Register(OpTopologyRequest, func() Message { return &TopologyRequest{} })
	Register(OpTopologyResponse, func() Message { return &TopologyResponse{} })

	// Replication
	Register(OpReplicationSync, func() Message { return &ReplicationSync{} })
	Register(OpReplicationAck, func() Message { return &ReplicationAck{} })
	Register(OpReplicationJoin, func() Message { return &ReplicationJoin{} })

	// Heartbeat
	Register(OpHeartbeat, func() Message { return &Heartbeat{} })
	Register(OpHeartbeatAck, func() Message { return &HeartbeatAck{} })

	// Election
	Register(OpVoteRequest, func() Message { return &VoteRequest{} })
	Register(OpVoteResponse, func() Message { return &VoteResponse{} })

	// Write forwarding
	Register(OpForwardWrite, func() Message { return &ForwardWrite{} })
	Register(OpForwardWriteResponse, func() Message { return &ForwardWriteResponse{} })

	// Auth
	Register(OpAuth, func() Message { return &AuthMessage{} })
	Register(OpAuthResponse, func() Message { return &AuthResponse{} })

	// Errors
	Register(OpError, func() Message { return &ErrorMessage{} })
}
