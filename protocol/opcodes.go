package protocol

// ─────────────────────────────────────────────────────────────────────────────
// OpCodes — Every message on the wire starts with one of these bytes.
// Grouped by functional area for clarity.
// ─────────────────────────────────────────────────────────────────────────────

// Client ↔ Node: CRUD operations
const (
	OpGet            byte = 0x01 // Client → Node      : Read a key
	OpSet            byte = 0x02 // Client → Leader     : Write a key
	OpDelete         byte = 0x03 // Client → Leader     : Delete a key
	OpGetResponse    byte = 0x04 // Node → Client       : Response to GET
	OpSetResponse    byte = 0x05 // Leader → Client     : Response to SET
	OpDeleteResponse byte = 0x06 // Leader → Client     : Response to DEL
)

// Cluster topology discovery
const (
	OpTopologyRequest  byte = 0x10 // Client → Any Node   : "Who's the leader? Give me the map."
	OpTopologyResponse byte = 0x11 // Node → Client        : Full cluster layout
)

// Leader ↔ Follower: Replication
const (
	OpReplicationSync byte = 0x20 // Leader → Follower   : Stream a WAL entry
	OpReplicationAck  byte = 0x21 // Follower → Leader   : "I applied up to index X"
	OpReplicationJoin byte = 0x22 // Follower → Leader   : Register as a replication peer
)

// Leader ↔ Follower: Heartbeat / Liveness
const (
	OpHeartbeat    byte = 0x30 // Leader → Follower   : "I'm alive, here's my term"
	OpHeartbeatAck byte = 0x31 // Follower → Leader   : "Ack, here's my progress"
)

// Election (Raft-like)
const (
	OpVoteRequest  byte = 0x40 // Candidate → Peer    : "Vote for me"
	OpVoteResponse byte = 0x41 // Peer → Candidate    : "Yes/No"
)

// Write forwarding: Follower → Leader
const (
	OpForwardWrite         byte = 0x50 // Follower → Leader   : Wrapped client write
	OpForwardWriteResponse byte = 0x51 // Leader → Follower   : Result of forwarded write
)

// Errors
const (
	OpError byte = 0xFE // Any → Any : Structured error
)

// ─────────────────────────────────────────────────────────────────────────────
// Well-known error codes used in ErrorMessage
// ─────────────────────────────────────────────────────────────────────────────
const (
	ErrCodeNotLeader   uint16 = 1 // Write was sent to a follower
	ErrCodeKeyNotFound uint16 = 2 // Key does not exist
	ErrCodeInternal    uint16 = 3 // Unexpected server error
	ErrCodeBadRequest  uint16 = 4 // Malformed message
	ErrCodeTimeout     uint16 = 5 // Operation timed out
)
