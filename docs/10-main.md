# Entry Point: main.go

**File:** `main.go`

`main.go` is the wiring layer. It reads configuration, constructs all major subsystems, and connects them together. It contains no business logic.

---

## Startup sequence

```
1. Parse --gen-token flag (if set: generate 32-byte hex token, print, exit)
2. config.Load()            — read environment variables
3. store.NewKVStore(...)    — open WAL, replay, start GC
4. cluster.NewNode(...)     — create node with Raft state
5. if standalone: node.ForceLeader()
   if cluster:   node.Start()   — launch Raft + follower goroutines
6. server.NewTCPServer(node, ...) — create TCP server
7. tcpServer.Start(port)    — begin accepting connections (blocks forever)
```

---

## --gen-token flag

```go
genToken := flag.Bool("gen-token", false, "generate a random auth token, print it, and exit")
flag.Parse()
if *genToken {
    token, _ := generateToken()
    fmt.Println(token)
    fmt.Fprintf(os.Stderr, "Set KV_AUTH_TOKEN=%s on both server and client.\n", token)
    os.Exit(0)
}
```

`generateToken` reads 32 bytes from `crypto/rand` (OS entropy source) and hex-encodes them → 64-character hex string. This is cryptographically secure. The helper message on stderr reminds the operator to set the same token on clients.

---

## Standalone vs cluster decision

```go
peers := cfg.Cluster.Peers
nodeAddr := cfg.Cluster.NodeAddr

if nodeAddr == "" || len(peers) == 0 {
    // Standalone mode
    node := cluster.NewNode(db, cfg.Store.DefaultTTL, "", nil)
    node.ForceLeader()
    // ...
    return
}

// Cluster mode
node := cluster.NewNode(db, cfg.Store.DefaultTTL, nodeAddr, peers)
node.Start()
```

The distinction is purely configuration-driven. The rest of the server startup is identical — `TCPServer` wraps `node` in both cases, and `engine.Dispatch` operates on `node` via the `Store` interface.

---

## evictionPolicy helper

```go
func evictionPolicy(name string) store.Eviction {
    switch name {
    case "relaxed": return store.SamplingEvict{}
    case "strict":  return store.LRUEvict{}
    default:        return store.NoEvict{}  // "noevict" or empty
    }
}
```

Maps the string from the config to the concrete eviction type. `default` covers both `"noevict"` and any typo — silent fallback to the safest policy.

---

## defer db.Close()

```go
db, err := store.NewKVStore(...)
defer db.Close()
```

`Close()` on `KVStore` calls `WAL.Close()`, which:
- In `interval` mode: signals the flusher goroutine to stop, waits for the final flush to complete, then closes the file.
- In other modes: calls `file.Sync()` then `file.Close()`.

This ensures no in-flight WAL writes are lost when the process exits cleanly (e.g., SIGINT). It does not protect against hard kills (`kill -9`).

---

## Log output at startup

Standalone mode:
```
mode=standalone  port=:6379  wal=production.wal  auth=false  eviction=noevict  max_memory=unlimited
```

Cluster mode:
```
mode=cluster  node=localhost:7001  peers=[localhost:7001 localhost:7002 localhost:7003]
              port=:7001  wal=node1.wal  auth=false  eviction=noevict  max_memory=unlimited
```

These log lines are designed to be grepped in production: every key parameter is present in a `key=value` format.
