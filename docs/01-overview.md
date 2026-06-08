# Project Overview

## What is this project?

A from-scratch distributed key-value store written in Go. It is roughly analogous to a stripped-down Redis but built on a custom binary TCP protocol and Raft-based leader election instead of relying on any external framework.

## Key properties

| Property | Implementation |
|---|---|
| Data model | String key → binary value (arbitrary bytes) |
| Persistence | Write-Ahead Log (WAL) on every write |
| Replication | Leader–follower streaming replication |
| Consensus | Simplified Raft for leader election |
| Concurrency | Sharded hash map with per-shard `sync.RWMutex` |
| TTL | Per-key expiry with background garbage collection |
| Eviction | Three pluggable policies: none / sampling / strict LRU |
| Auth | Pre-shared token handshake (`crypto/subtle` constant-time compare) |
| Transport | Custom binary framing over raw TCP (no HTTP, no gRPC) |

## Package map

```
main.go              Entry point: wires all packages together, standalone vs cluster mode
├── config/          Environment-variable driven configuration
├── store/           In-memory sharded hash map + WAL + eviction policies
├── protocol/        Binary wire format: opcodes, message codec, registry
├── server/          TCP accept loop, auth gate, message routing
├── engine/          Command dispatcher (GET/SET/DEL handlers)
├── cluster/         Raft state machine, leader replication, follower sync
├── cli/             Interactive REPL client with cluster topology awareness
└── bench/
    ├── client/      Persistent-connection benchmark client library
    └── loadgen/     Open-loop load generator with HDR histogram reporting
```

## Data flow for a client SET in cluster mode

```
CLI client
  │  [OpAuth] → authenticated connection
  │  [OpSet key value ttl]
  ▼
TCP Server (server/tcp.go)
  │  readMessage → frame decode
  │  authenticate (if token configured)
  │  dispatchClient → dispatchToEngine
  ▼
Engine (engine/handler.go)
  │  Dispatch(OpSet) → handleSet
  ▼
Node.Set (cluster/node.go)
  │  db.SetWithExpiry → WAL append + in-memory write
  │  l.BroadcastSet  → push ReplicationSync to all followers
  ▼
Follower nodes (cluster/follower.go)
     applySync → db.SetWithExpiry (mirrors leader's exact expiry)
     ReplicationAck → leader (progress tracking)
```

## Running modes

**Standalone** — `KV_NODE_ADDR` and `KV_PEERS` are both empty. The node calls `ForceLeader()` and skips Raft entirely. Good for single-machine development.

**Cluster** — Set `KV_NODE_ADDR=<this node's address>` and `KV_PEERS=addr1,addr2,addr3`. All nodes in the cluster must share the same `KV_PEERS` list. Raft elects one leader; the others become followers and establish replication streams to the leader.
