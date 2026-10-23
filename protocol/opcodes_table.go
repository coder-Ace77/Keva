package protocol

// ─────────────────────────────────────────────────────────────────────────────
// Opcodes Table — every message on the wire starts with one of these bytes.
//
// This is the single place opcodes are assigned, so collisions are visible at
// a glance. The message types themselves live in the sub-packages:
//   0x0_  kv/       client CRUD
//   0x1_  client/   topology discovery
//   0x2_  cluster/  replication
//   0x3_  cluster/  heartbeat
//   0x4_  cluster/  election
//   0xFE  errors.go structured error
// ─────────────────────────────────────────────────────────────────────────────

// Client ↔ Node: CRUD operations (kv/)
const (
	OpGet            byte = 0x01 // Client → Node      : Read a key
	OpSet            byte = 0x02 // Client → Leader     : Write a key
	OpDelete         byte = 0x03 // Client → Leader     : Delete a key
	OpGetResponse    byte = 0x04 // Node → Client       : Response to GET
	OpSetResponse    byte = 0x05 // Leader → Client     : Response to SET
	OpDeleteResponse byte = 0x06 // Leader → Client     : Response to DEL
)

// Cluster topology discovery (client/)
const (
	OpTopologyRequest  byte = 0x10 // Client → Any Node   : "Who's the leader? Give me the map."
	OpTopologyResponse byte = 0x11 // Node → Client        : Full cluster layout
)

// Leader ↔ Follower: Replication (cluster/)
const (
	OpReplicationSync byte = 0x20 // Leader → Follower   : Stream a WAL entry
	OpReplicationAck  byte = 0x21 // Follower → Leader   : "I applied up to index X"
	OpReplicationJoin byte = 0x22 // Follower → Leader   : Register as a replication peer
)

// Leader ↔ Follower: Heartbeat / Liveness (cluster/)
const (
	OpHeartbeat    byte = 0x30 // Leader → Follower   : "I'm alive, here's my term"
	OpHeartbeatAck byte = 0x31 // Follower → Leader   : "Ack, here's my progress"
)

// Election, Raft-like (cluster/)
const (
	OpVoteRequest  byte = 0x40 // Candidate → Peer    : "Vote for me"
	OpVoteResponse byte = 0x41 // Peer → Candidate    : "Yes/No"
)

// Errors (errors.go)
const (
	OpError byte = 0xFE // Any → Any : Structured error
)
