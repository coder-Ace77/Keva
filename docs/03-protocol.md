# Package: protocol

**Files:** `protocol/payload.go`, `protocol/opcodes.go`, `protocol/registry_init.go`, and one file per message type.

This package defines everything that goes on the wire: the opcodes, the binary encoding of each message type, and the registry-based codec system.

---

## Why a custom binary protocol?

- **No overhead:** No HTTP headers, JSON encoding/decoding, or protobuf reflection. Each message is the minimum bytes needed.
- **Interview answer:** "I wanted to understand how databases like Redis design their wire protocols. Redis uses RESP (a text-based protocol). I designed a binary protocol so I could learn the tradeoffs: binary is faster to parse, harder to debug manually, but unambiguous and compact."

---

## TCP framing

The TCP byte stream is broken into discrete frames using a 4-byte length prefix:

```
[TotalLength 4B big-endian] [OpCode 1B] [Payload ...]
```

The server reads 4 bytes to learn the total message length, then reads exactly that many bytes. This is called "length-prefixed framing" and solves the fundamental problem of knowing where one message ends and the next begins on a stream transport.

The maximum payload size is enforced at the server level (`maxPayloadBytes`, default 10 MB) to prevent a malicious or buggy client from sending a giant frame and causing an out-of-memory allocation.

---

## The Message interface

```go
type Message interface {
    OpCode() byte
    Encode() ([]byte, error)
    Decode(data []byte) error
}
```

Every message on the wire implements this interface. `OpCode()` returns the one-byte identifier. `Encode()` serializes the fields to bytes. `Decode(data)` deserializes from bytes (the data slice excludes the opcode byte — the registry already consumed it).

---

## The registry pattern

Instead of a giant `switch opcode { case 0x01: ... case 0x02: ... }` in the decode path, the protocol uses a registry of factory functions:

```go
var registry = make(map[byte]func() Message)

func Register(opcode byte, factory func() Message) {
    // panics on duplicate — catches bugs at init time
    registry[opcode] = factory
}

func DecodeMessage(data []byte) (Message, error) {
    opcode := data[0]
    msg, ok := Lookup(opcode)   // calls factory() to get a fresh empty struct
    msg.Decode(data[1:])        // populate from the remaining bytes
    return msg, nil
}
```

All registrations happen in `registry_init.go`'s `init()` function, which Go calls automatically before `main()`. Adding a new message type means:
1. Write a struct with `OpCode()`, `Encode()`, `Decode()`.
2. Add one `Register(...)` line in `registry_init.go`.

Nothing else changes. No switches to update anywhere.

**Interview question:** "How does the server know what struct to deserialize a message into?" — It reads the first byte (opcode), looks it up in the registry, the registry calls the factory function to produce an empty struct of the right type, then calls `Decode` on that struct with the remaining bytes.

---

## Opcodes

```
0x01 OpGet              Client → any node: read a key
0x02 OpSet              Client → leader: write a key
0x03 OpDelete           Client → leader: delete a key
0x04 OpGetResponse      Node → client: value or not-found
0x05 OpSetResponse      Node → client: success/fail
0x06 OpDeleteResponse   Node → client: success/fail

0x10 OpTopologyRequest  Client → any node: ask for cluster map
0x11 OpTopologyResponse Node → client: leader + all node addresses

0x20 OpReplicationSync  Leader → follower: a WAL entry to apply
0x21 OpReplicationAck   Follower → leader: "I applied up to index N"
0x22 OpReplicationJoin  Follower → leader: "register me as a replication peer"

0x30 OpHeartbeat        Leader → follower: "I'm alive, term T"
0x31 OpHeartbeatAck     Follower → leader: "acknowledged, my term is T"

0x40 OpVoteRequest      Candidate → peer: "vote for me in term T"
0x41 OpVoteResponse     Peer → candidate: "yes/no, my term is T"

0x50 OpForwardWrite     Follower → leader: proxy a client write
0x51 OpForwardWriteResponse Leader → follower: result of proxied write

0x60 OpAuth             Client → node: present the pre-shared token
0x61 OpAuthResponse     Node → client: accepted or rejected

0xFE OpError            Any direction: structured error
```

Error codes:
```
1  ErrCodeNotLeader    — write sent to a follower
2  ErrCodeKeyNotFound  — key does not exist
3  ErrCodeInternal     — unexpected server error
4  ErrCodeBadRequest   — malformed message
5  ErrCodeTimeout      — operation timed out
6  ErrCodeUnauthorized — missing or invalid auth token
```

---

## Message binary formats (all big-endian)

### SetPayload (OpSet)
```
[KeyLen 2B][ValLen 4B][TTL 8B][Key ...][Value ...]
```
TTL is in nanoseconds (Go's `time.Duration` unit). 0 = use server default.

### GetPayload (OpGet)
```
[KeyLen 2B][Key ...]
```

### DeletePayload (OpDelete)
```
[KeyLen 2B][Key ...]
```

### GetResponse (OpGetResponse)
```
[Found 1B][ValLen 4B (if Found)][Value ... (if Found)]
```
`Found = 0x01` means the key exists; `0x00` means not found.

### SetResponse / DeleteResponse
```
[Success 1B][MsgLen 2B][Message string ...]
```

### ReplicationSync (OpReplicationSync)
```
[LogIndex 8B][Op 1B][KeyLen 2B][ValLen 4B][TTL 8B][Key ...][Value ...]
```
`Op` reuses the WAL constants (`0x01` = Set, `0x02` = Delete). `LogIndex` is a monotonically increasing counter from the leader.

### ReplicationAck (OpReplicationAck)
```
[LastAppliedIndex 8B]
```

### ReplicationJoin (OpReplicationJoin)
```
[AddrLen 2B][NodeAddr string ...]
```
Sent by a follower when it connects to the leader to start receiving replication.

### Heartbeat / HeartbeatAck
```
Heartbeat:    [Term 8B][LeaderIDLen 2B][LeaderID ...]
HeartbeatAck: [Term 8B]
```

### VoteRequest / VoteResponse
```
VoteRequest:  [Term 8B][CandidateIDLen 2B][CandidateID ...]
VoteResponse: [Term 8B][Granted 1B]
```

### AuthMessage / AuthResponse
```
AuthMessage:  [TokenLen 2B][Token ...]
AuthResponse: [Success 1B]
```

### ErrorMessage (OpError)
```
[Code 2B][MsgLen 2B][Message ...]
```

### TopologyRequest
```
(empty payload)
```

### TopologyResponse
```
[LeaderAddrLen 2B][LeaderAddr ...]
[NodeCount 2B]
[per node: AddrLen 2B][Addr ...][Role 1B][NodeIDLen 2B][NodeID ...]]
```

---

## EncodeMessage / DecodeMessage

```go
func EncodeMessage(msg Message) ([]byte, error) {
    payload, _ := msg.Encode()
    wire := make([]byte, 1+len(payload))
    wire[0] = msg.OpCode()      // opcode first
    copy(wire[1:], payload)
    return wire, nil
}

func DecodeMessage(data []byte) (Message, error) {
    opcode := data[0]
    msg, ok := Lookup(opcode)   // fresh empty struct from registry
    msg.Decode(data[1:])
    return msg, nil
}
```

The 4-byte length prefix is handled by the TCP layer (`server/tcp.go` and `cluster/wire.go`), not here. This separation keeps the protocol package free of transport concerns.
