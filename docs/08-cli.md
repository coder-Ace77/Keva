# Package: cli

**File:** `cli/cli.go`

The CLI is an interactive REPL (Read-Eval-Print Loop) client. It understands the cluster topology and routes commands to the correct node — writes to the leader, reads to any follower for load distribution.

---

## kvClient struct

```go
type kvClient struct {
    knownAddrs []string  // grows as topology reveals more nodes
    leaderAddr string
    followers  []string
    authToken  string
}
```

The client maintains its own view of the cluster topology. It starts with a single seed address and discovers the rest from the `OpTopologyResponse`.

---

## Startup sequence

```
1. Parse -url / -u flag (or KV_ADDR env, or default localhost:6379)
2. Parse -token / -t flag (or KV_AUTH_TOKEN env)
3. newClient(seed, token)
4. client.waitForLeader() — retries topology discovery for up to 4.5 seconds
5. Print: Leader + Followers
6. Enter REPL loop
```

`waitForLeader` retries up to 15 times with 300 ms between attempts (4.5 seconds total). This gives the cluster time to elect a leader if it was just started.

---

## Topology discovery

```go
func (c *kvClient) connect() error {
    for _, addr := range c.knownAddrs {
        topo, err := c.fetchTopology(addr)
        if err != nil { continue }
        c.leaderAddr = topo.LeaderAddr
        c.followers = nil
        for _, n := range topo.Nodes {
            c.addKnown(n.Address)  // expand known addresses
            if n.Address != topo.LeaderAddr {
                c.followers = append(c.followers, n.Address)
            }
        }
        if c.leaderAddr != "" { return nil }
    }
    return fmt.Errorf("no reachable node returned a topology with a leader")
}
```

`fetchTopology` opens a short-lived connection to one node, authenticates if needed, sends `OpTopologyRequest`, reads `OpTopologyResponse`, and closes the connection. The response contains the leader address and all node addresses.

`addKnown` deduplicates — if topology reveals addresses not in the initial seed list, they're added for future reconnect attempts.

---

## Command routing

```go
func (c *kvClient) execute(msg protocol.Message) (protocol.Message, error) {
    isWrite := msg.OpCode() == protocol.OpSet || msg.OpCode() == protocol.OpDelete

    for attempt := 0; attempt < 8; attempt++ {
        target := c.pickTarget(isWrite)
        resp, err := c.sendToNode(target, msg)
        if err == nil { return resp, nil }
        // node unreachable: re-discover cluster and retry
        c.leaderAddr = ""
        time.Sleep(300ms)
        c.waitForLeader()
    }
    return nil, fmt.Errorf("cluster unavailable after retries")
}

func (c *kvClient) pickTarget(isWrite bool) string {
    if isWrite { return c.leaderAddr }
    if len(c.followers) > 0 { return c.followers[rand.Intn(len(c.followers))] }
    return c.leaderAddr  // no followers: read from leader
}
```

**Writes always go to the leader.** The leader is the only node that accepts writes. If a write is sent to a follower, the follower returns `ErrCodeNotLeader`.

**Reads are randomly distributed across followers.** This is client-side load balancing. With 3 followers, each follower handles ~33% of reads. If there are no followers, reads go to the leader.

The retry loop handles leader failover: if the target is unreachable, the client clears `leaderAddr` and re-discovers the cluster (which will have a new leader after Raft election).

---

## Per-command connection model

Each command opens a new TCP connection, authenticates, sends the command, reads the response, and closes the connection. This is simple and avoids connection lifecycle complexity, at the cost of TCP handshake overhead per command.

**Interview question:** "Is this efficient?" — For an interactive REPL with human-speed input, the handshake overhead is invisible. For high-throughput automation, you'd want persistent connections (see `bench/client`). This is a tradeoff that fits the use case.

---

## Authentication per connection

```go
func (c *kvClient) authConn(conn net.Conn) error {
    if c.authToken == "" { return nil }
    sendMessage(conn, &protocol.AuthMessage{Token: []byte(c.authToken)})
    resp, _ := receiveMessage(conn)
    authResp := resp.(*protocol.AuthResponse)
    if !authResp.Success { return fmt.Errorf("authentication failed: invalid token") }
    return nil
}
```

Called in `fetchTopology` and `sendToNode` before any command is sent. If no token is configured, this is a no-op.

---

## REPL commands

```
GET <key>              → sends OpGet, prints value or (nil)
SET <key> <value>      → sends OpSet, prints OK or error
DEL <key>              → sends OpDelete, prints OK or "key not found"
topology               → prints leader + followers (no network call)
exit / quit            → exits
```

Parsing is simple whitespace splitting. `SET key hello world` joins everything after the key as the value (`"hello world"`).

---

## Output colours

ANSI escape codes for a better terminal experience:
- Cyan: prompt, GET values
- Green: successful SET/DEL
- Yellow: not found (nil)
- Red: errors
- Bold: key values, error prefixes

These are not conditionalized on whether the terminal supports them (no `isatty` check) — a minor limitation for piped output.
