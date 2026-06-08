# Package: server

**File:** `server/tcp.go`

The server package contains the TCP accept loop and connection handler. It is the entry point for all network traffic — both client commands and cluster-internal messages.

---

## TCPServer struct

```go
type TCPServer struct {
    node            *cluster.Node
    listener        net.Listener
    maxPayloadBytes uint32
    authToken       string
}
```

- `node` — the cluster node (implements the `engine.Store` interface and holds Raft state).
- `maxPayloadBytes` — hard limit on incoming frame size, default 10 MB.
- `authToken` — if non-empty, every client connection must authenticate before sending commands.

---

## Start (accept loop)

```go
func (s *TCPServer) Start(address string) error {
    s.listener, _ = net.Listen("tcp", address)
    for {
        conn, _ := s.listener.Accept()
        go s.handleConnection(conn)   // one goroutine per connection
    }
}
```

Every accepted connection gets its own goroutine. This is the classical "goroutine-per-connection" model. In Go, goroutines are cheap (~2 KB stack, grows on demand), so this scales to thousands of connections without a thread-pool bottleneck.

---

## handleConnection — the routing logic

This is the most important function in the server package. It reads the **first message only**, then dispatches based on opcode to determine what kind of connection this is:

```go
func (s *TCPServer) handleConnection(conn net.Conn) {
    firstMsg, _ := s.readMessage(conn)

    switch firstMsg.OpCode() {
    case protocol.OpReplicationJoin:
        s.handleReplicationPeer(conn, firstMsg)
        // NOTE: conn is NOT closed here — it stays open for streaming

    case protocol.OpHeartbeat:
        defer conn.Close()
        hb := firstMsg.(*protocol.Heartbeat)
        s.node.HandleHeartbeat(hb.Term, hb.LeaderID)
        s.writeMessage(conn, &protocol.HeartbeatAck{Term: s.node.GetTerm()})

    case protocol.OpVoteRequest:
        defer conn.Close()
        req := firstMsg.(*protocol.VoteRequest)
        granted, term := s.node.HandleVoteRequest(req)
        s.writeMessage(conn, &protocol.VoteResponse{Term: term, Granted: granted})

    default:
        defer conn.Close()
        // Auth gate: must authenticate before any command
        if s.authToken != "" {
            firstMsg, _ = s.authenticate(conn, firstMsg)
        }
        s.dispatchClient(conn, firstMsg)
        // Keep-alive loop for subsequent commands on same connection
        for {
            msg, _ := s.readMessage(conn)
            s.dispatchClient(conn, msg)
        }
    }
}
```

### Key design decisions

**1. Cluster messages bypass auth.** Heartbeats and vote requests are node-to-node, not client-to-server. They use a different port range conceptually and are not authenticated by the token system. In production you would typically isolate this with a separate internal port or mutual TLS.

**2. Replication connections are long-lived.** When a follower sends `OpReplicationJoin`, the server calls `AddFollower(conn)` and returns — the connection stays open indefinitely. The leader pushes `ReplicationSync` frames down this connection. No `defer conn.Close()` is set for this case.

**3. Client connections are multiplexed.** After the first message is dispatched, a `for` loop reads subsequent messages on the same connection. Clients do not need to reconnect for every command.

---

## Authentication

```go
func (s *TCPServer) authenticate(conn net.Conn, firstMsg protocol.Message) (protocol.Message, error) {
    if firstMsg.OpCode() != protocol.OpAuth {
        s.writeError(conn, protocol.ErrCodeUnauthorized, "authentication required")
        return nil, fmt.Errorf("auth required")
    }
    auth := firstMsg.(*protocol.AuthMessage)
    tokenMatch := subtle.ConstantTimeCompare(auth.Token, []byte(s.authToken)) == 1
    if !tokenMatch {
        s.writeMessage(conn, &protocol.AuthResponse{Success: false})
        return nil, fmt.Errorf("invalid token")
    }
    s.writeMessage(conn, &protocol.AuthResponse{Success: true})
    // Read the FIRST REAL command now that auth passed
    msg, _ := s.readMessage(conn)
    return msg, nil
}
```

`subtle.ConstantTimeCompare` is critical here. A naive `string(auth.Token) == s.authToken` comparison returns early on the first mismatched byte, which leaks timing information. An attacker could measure response times to gradually discover the token one byte at a time (timing attack). `ConstantTimeCompare` always compares all bytes regardless of where the mismatch is.

After a successful auth the function reads and returns the first real command so the caller can dispatch it normally — auth is a one-time handshake per connection, not repeated per command.

---

## dispatchClient

```go
func (s *TCPServer) dispatchClient(conn net.Conn, msg protocol.Message) {
    if msg.OpCode() == protocol.OpTopologyRequest {
        s.writeMessage(conn, s.node.GetTopology())
        return
    }
    s.dispatchToEngine(conn, msg)
}
```

Topology requests are handled directly by the server (they don't go through the command engine) because topology is a cluster concept, not a store operation.

All other client messages go to the engine via `dispatchToEngine`:

```go
func (s *TCPServer) dispatchToEngine(conn net.Conn, msg protocol.Message) {
    resp, err := engine.Dispatch(msg, s.node)
    // write response or error
}
```

`s.node` implements `engine.Store` (it has `Set`, `Get`, `Delete` methods), so the engine operates on it without knowing about clusters or Raft.

---

## Wire framing (readMessage / writeMessage)

```go
func (s *TCPServer) readMessage(conn net.Conn) (protocol.Message, error) {
    header := make([]byte, 4)
    io.ReadFull(conn, header)            // read exactly 4 bytes
    msgLen := binary.BigEndian.Uint32(header)
    if msgLen > s.maxPayloadBytes { ... } // safety check
    frame := make([]byte, msgLen)
    io.ReadFull(conn, frame)             // read exactly msgLen bytes
    return protocol.DecodeMessage(frame)
}
```

`io.ReadFull` blocks until the exact number of bytes have been received or an error occurs. This handles the fact that TCP is a stream — a single `conn.Read` call may return fewer bytes than requested due to network segmentation.

`writeMessage` does the inverse: `EncodeMessage` → prepend 4-byte length → write both to the connection.

---

## Error responses

Any error causes `writeError`, which sends an `ErrorMessage` frame back to the client before the connection is closed. Clients can always parse the response and read a structured error code.
