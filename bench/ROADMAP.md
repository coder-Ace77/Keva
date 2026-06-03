# Benchmarking Roadmap

**Goal:** produce four concrete, resume-grade numbers for a distributed KV store built from scratch in Go with a custom Raft-based leader election, WAL, and binary TCP protocol.

---

## 1. The Four Metrics That Matter

| Metric | Why reviewers care | Your measurement tool |
|---|---|---|
| **Sustained writes/s + reads/s** | Baseline throughput signals implementation quality | `bench/loadgen` |
| **Write p99 latency** | Write path is the interesting one: WAL flush + async replication broadcast | `bench/loadgen` |
| **Leader failover window (p50 + p95)** | This is the money metric — how long writes are unavailable after a `kill -9` | `bench/failover` |
| **Linearizability under load** | "Verified with Porcupine" is a rare, elite-tier signal on a junior resume | `bench/linearcheck` |

---

## 2. Architecture Notes (context for honest resume framing)

- **Raft parameters:** heartbeat 50 ms, election timeout 150–300 ms. Theoretical failover floor ≈ 200–350 ms.
- **Replication model:** leader applies the write locally (WAL + memory), then broadcasts to followers *fire-and-forget* — no quorum ack is awaited. This means **write latency is NOT quorum-write latency**; it is WAL-append + async fan-out latency. State this clearly.
- **Read routing:** any node accepts reads (local KV lookup). Follower reads are **eventually consistent**, not linearizable. The linearizability check therefore routes **all ops to the leader**.
- **Write routing:** writes must reach the leader. The CLI and bench tools discover the leader via topology. A write sent to a follower will apply locally only (known limitation).

---

## 3. Tool Reference

### 3.1 Load Generator (`bench/loadgen`)

```bash
go run ./bench/loadgen \
  -addr   "localhost:7001,localhost:7002,localhost:7003" \
  -rate   10000 \
  -duration 60s \
  -workers  64 \
  -read-ratio 0.5 \
  -keys   1000 \
  -value-size 64
```

**Key flags:**

| Flag | Default | Meaning |
|---|---|---|
| `-rate` | 5000 | Open-loop target ops/s |
| `-workers` | 64 | Persistent TCP connections |
| `-read-ratio` | 0.5 | Fraction of ops that are GETs |
| `-keys` | 1000 | Keyspace size (uniform random) |
| `-value-size` | 64 | Bytes per value |

**Sample output:**

```
=== Load Generator Results ===
Target node:           localhost:7001
Duration:              60.0s
Workers:               64
Target rate:           10000 ops/s
Actual rate:           9843 ops/s  (590580 total)
Errors:                18 (0.003%)
Dropped tokens:        0

Write latency (µs)  [295290 ops]
  p50=    312   p95=    892   p99=   1843   p999=   8120   max=  18432

Read  latency (µs)  [295290 ops]
  p50=    145   p95=    412   p99=    723   p999=   2100   max=   9800
```

**Recommended runs:**
1. Warm-up: 30 s at 5 k ops/s (discarded)
2. Measurement: 60 s at 5 k ops/s → note steady-state numbers
3. Measurement: 60 s at 20 k ops/s → note where p99 degrades

---

### 3.2 Failover Measurer (`bench/failover`)

```bash
go run ./bench/failover \
  -addr "localhost:7001,localhost:7002,localhost:7003" \
  -runs 30
```

The tool writes a probe key every 10 ms and timestamps every outcome. When you `kill -9` the leader from another terminal, it records:

```
window = first_success_on_new_leader − last_success_on_old_leader
```

This is the **write-unavailability window** — what a user actually experiences.

**Sample output (30 runs):**

```
=== Failover Window Statistics  (30 samples) ===
  min:     183ms
  p50:     241ms
  mean:    258ms
  p95:     380ms
  p99:     410ms
  max:     432ms

Resume bullet template:
  Leader failover: median 241ms, p95 380ms  (30 kill-9 trials, 3-node cluster)
```

**Notes:**
- The theoretical floor is ~200–350 ms (one missed heartbeat → election). Your measured p50 should land in that band on a local cluster.
- On a real LAN (EC2 same-AZ), expect similar numbers since Raft timing is CPU-bound, not network-bound at those timescales.
- Run from a separate terminal; keep the failover tool's output visible while you kill nodes.

**How to kill the leader:**

```bash
# Find leader address from the tool's output, e.g. localhost:7001
lsof -t -i:7001 | xargs kill -9
```

---

### 3.3 Linearizability Checker (`bench/linearcheck`)

```bash
go run ./bench/linearcheck \
  -addr "localhost:7001,localhost:7002,localhost:7003" \
  -workers 10 \
  -keys 5 \
  -duration 20s
```

Runs 10 goroutines doing mixed GET/SET on 5 keys for 20 s, all routed to the leader. At the end, feeds the full operation history (with nanosecond timestamps) to [Porcupine](https://github.com/anishathalye/porcupine).

**Expected output (no partition):**

```
collected 3240 operations — checking linearizability per key...

  key "lc:0"  : ✓ LINEARIZABLE  (648 ops)
  key "lc:1"  : ✓ LINEARIZABLE  (652 ops)
  key "lc:2"  : ✓ LINEARIZABLE  (644 ops)
  key "lc:3"  : ✓ LINEARIZABLE  (649 ops)
  key "lc:4"  : ✓ LINEARIZABLE  (647 ops)

RESULT  : All keys linearizable ✓
```

**With a network partition (advanced):**

Use [Toxiproxy](https://github.com/Shopify/toxiproxy) to cut traffic between nodes mid-run, then restore it. Any reads that return stale values during the partition will surface as violations.

```bash
# Install toxiproxy-server and toxiproxy-cli, then:
toxiproxy-server &
toxiproxy-cli create kv01-to-kv02 -l localhost:17001 -u localhost:7002
toxiproxy-cli toxic add kv01-to-kv02 -t timeout -a timeout=10000   # partition
# ... run linearcheck ...
toxiproxy-cli toxic remove kv01-to-kv02 -n timeout_1               # heal
```

---

## 4. Local 3-Node Cluster Setup

The repo ships with `create_cluster.sh`. Use it for local benchmarking:

```bash
go build -o ./kvstore .
./create_cluster.sh
```

The script starts three nodes on random ports and prints their PIDs. Note the ports printed — pass them to the bench tools as `-addr`.

For the failover tool you need the PIDs. The script prints them; alternatively:

```bash
lsof -t -i:<PORT>   # finds the PID listening on a port
```

---

## 5. EC2 One-Hour Plan

You only have ~60 minutes. Run this sequence to capture all four numbers before the instances terminate.

### Instance setup

- **Count:** 3 nodes + 1 client/benchmark machine (4 instances total)
- **Type:** `t3.medium` (2 vCPU, 4 GB) — enough headroom; upgrade to `t3.large` if you want to push past 20 k ops/s without CPU saturation
- **AZ:** same availability zone (same-AZ traffic is < 1 ms RTT — keeps election timing realistic)
- **OS:** Amazon Linux 2023 or Ubuntu 22.04

### Timeline

| Time | Action |
|---|---|
| **0:00–0:10** | Launch 4 instances. SSH in. Install Go (`wget` the tarball), copy binary or `go build` on each node. |
| **0:10–0:15** | Start the 3-node cluster. Use fixed ports (e.g. 7001/7002/7003). Verify with the CLI that a leader is elected. |
| **0:15–0:20** | **Warm-up run:** `loadgen -rate 3000 -duration 30s` — discard the numbers, let the OS page caches warm. |
| **0:20–0:35** | **Throughput runs:** two 3-min runs at 5 k and 20 k ops/s. Note actual rate and write p99. |
| **0:35–0:52** | **Failover runs:** `failover -runs 30`. From a separate SSH session, kill the leader after the first "steady" line appears. Repeat 30 times. |
| **0:52–0:57** | **Linearizability check:** `linearcheck -workers 10 -keys 5 -duration 20s`. Capture the PASS/FAIL verdict. |
| **0:57–1:00** | Copy all terminal output to a local file. `terraform destroy` / terminate instances. |

### Quick EC2 cluster launch script

```bash
#!/usr/bin/env bash
# Run on each of the 3 node machines (set NODE_ADDR and PEERS before running)
export KV_PORT=":7001"
export KV_NODE_ADDR="$NODE_ADDR"    # e.g. "10.0.0.1:7001"
export KV_PEERS="$PEERS"            # e.g. "10.0.0.1:7001,10.0.0.2:7002,10.0.0.3:7003"
export KV_WAL_PATH="/tmp/node.wal"
export KV_WAL_MODE="interval"
./kvstore
```

---

## 6. Resume Bullet Template

Fill in your measured numbers:

```
Key-Value Store (Go) — github.com/yourhandle/keyvaluestore
• Custom binary TCP protocol, sharded in-memory store, WAL with configurable
  durability (always / interval / none)
• Raft-based leader election (150–300 ms timeout, 50 ms heartbeat)
• Sustained ____k writes/s + ____k reads/s on a 3-node cluster
  (3× t3.medium, same-AZ); write p99 = ____ ms
• Leader failover: median ____ ms, p95 ____ ms across 30 kill-9 trials
• Verified linearizable under concurrent load with Porcupine checker
```

---

## 7. Honest Caveats to Know (for interview Q&A)

| What an interviewer might ask | Accurate answer |
|---|---|
| "Is this a quorum write?" | No — the leader appends to WAL and broadcasts async. Writes are acknowledged after local commit only. Replication lag can exist on followers. |
| "Are follower reads linearizable?" | No — followers may serve stale data. Linearizability holds only for leader-routed reads. |
| "What's your replication guarantee?" | At-least-once delivery to followers; followers apply in order. No epoch-fenced log (simplified Raft). |
| "What happens to in-flight writes when the leader dies?" | They fail with a network error. The client must retry. The tool measures this window. |
