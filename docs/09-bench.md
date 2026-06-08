# Package: bench

**Files:** `bench/client/client.go`, `bench/loadgen/main.go`

The bench directory contains two subpackages: a reusable benchmark client library and a full open-loop load generator.

---

## bench/client — persistent-connection client

**File:** `bench/client/client.go`

Unlike the CLI (which opens a new TCP connection per command), the benchmark client keeps connections open across many requests.

### Conn struct

```go
type Conn struct {
    conn net.Conn
}
```

A single authenticated persistent TCP connection to one KV node.

### Dial

```go
func Dial(addr, authToken string, timeout time.Duration) (*Conn, error) {
    raw, _ := net.DialTimeout("tcp", addr, timeout)
    c := &Conn{conn: raw}
    if authToken != "" { c.doAuth(authToken) }
    return c, nil
}
```

Connects, authenticates once, and returns. All subsequent `Do` calls reuse the same connection.

### Do

```go
func (c *Conn) Do(req protocol.Message, opTimeout time.Duration) (protocol.Message, error) {
    c.conn.SetDeadline(time.Now().Add(opTimeout))
    c.Send(req)
    return c.Recv()
}
```

`SetDeadline` applies to both the write and the read (it's a single deadline on the entire connection, not per-operation). If either the send or receive takes longer than `opTimeout`, the connection returns an error.

### Topology helpers

```go
func FetchTopology(addr, authToken string) (*protocol.TopologyResponse, error)
func DiscoverLeader(seeds []string, authToken string) (leaderAddr string, peers []string, err error)
func WaitForLeader(seeds []string, authToken string, maxWait time.Duration) (string, []string, error)
```

`WaitForLeader` polls `DiscoverLeader` until a leader is found or `maxWait` elapses (100 ms between attempts). Used by the load generator at startup to wait for the cluster to elect a leader.

---

## bench/loadgen — open-loop load generator

**File:** `bench/loadgen/main.go`

### What "open-loop" means

A **closed-loop** generator sends a request, waits for a response, then sends the next request. Throughput is bounded by latency: 1 ms latency = max 1000 ops/sec per connection.

An **open-loop** generator sends requests at a fixed rate regardless of how long responses take. It simulates a real workload where arrivals are independent of service time. This reveals queue buildup and tail latency under load.

The load generator uses a ticker to inject work tokens at the target rate:
```go
batchSize    := max(1, *rate/1000)      // avoid 1000 Hz ticker jitter
tickInterval := time.Duration(int64(time.Second) * int64(batchSize) / int64(*rate))

for {
    <-ticker.C
    for i := 0; i < batchSize; i++ {
        select {
        case workCh <- rand.Float64() >= readRatio:  // true = write
        default:
            atomic.AddInt64(&droppedTokens, 1)       // workers can't keep up
        }
    }
}
```

`droppedTokens` counts how many work items were dropped because the `workCh` was full. A non-zero drop count means the cluster can't sustain the target rate.

### Worker goroutines

```go
func runWorker(id int, ...) {
    reconnect := func() {
        // discover leader, dial a persistent connection
    }
    reconnect()
    defer conn.Close()

    for isWrite := range workCh {
        key := pickKey()
        start := time.Now()
        if isWrite {
            resp, err = conn.Do(&protocol.SetPayload{...}, 5*time.Second)
        } else {
            resp, err = conn.Do(&protocol.GetPayload{...}, 5*time.Second)
        }
        latUs := time.Since(start).Microseconds()
        // record in histogram
    }
}
```

Each worker maintains one persistent connection. If `conn.Do` fails (network error), the worker reconnects before continuing. Reconnect always targets the current leader (re-discovers via topology).

### Key distribution

```go
func pickKey() []byte {
    if hotKeys > 0 && rng.Float64() < hotRatio {
        return keys[rng.Intn(hotKeys)]   // hot pool
    }
    return keys[hotKeys + rng.Intn(len(keys)-hotKeys)]  // cold pool
}
```

The `--hot-keys N --hot-ratio 0.8` flags model skewed access patterns (Zipfian-like): 80% of requests hit the top N keys. This is realistic for cache workloads where a small number of "hot" keys account for most traffic.

### HDR Histograms

```go
writeHist := hdrhistogram.New(1, maxLatencyMicros, histSigFigs)
// ...
writeHist.RecordValue(latUs)
// ...
fmt.Printf("p50=%d  p95=%d  p99=%d  p999=%d  max=%d\n",
    h.ValueAtQuantile(50), h.ValueAtQuantile(95), ...)
```

HDR (High Dynamic Range) histograms track latency distribution across a wide range (1 µs to 30 s) with configurable precision (3 significant figures). They're more accurate than average/stddev for latency analysis because latency distributions are typically non-normal — there's a long tail.

Per-worker histograms are merged at the end:
```go
totalWrite.Merge(r.writeHist)
```

This is safe because workers have stopped by the time merging happens (after `wg.Wait()`).

### CSV output

```
label, timestamp, target_rate, actual_rate, duration_s, workers,
read_ratio_pct, value_bytes, key_count, hot_keys, hot_ratio_pct,
write_ops, read_ops, errors, dropped_tokens,
wp50_us, wp95_us, wp99_us, wp999_us, wmax_us,
rp50_us, rp95_us, rp99_us, rp999_us, rmax_us
```

The CSV file is append-only. If the file doesn't exist, a header row is written first. Each run appends one data row. This allows running multiple scenarios and comparing results in a spreadsheet.

### CLI flags

```
-addr       "localhost:7001,localhost:7002,localhost:7003"  seed nodes
-label      "write-heavy-10k"                               scenario name
-rate       10000                                           target ops/sec
-duration   60s                                             test length
-workers    64                                              concurrent connections
-read-ratio 0.5                                             fraction that are reads
-keys       5000                                            key-space size
-value-size 64                                              value payload bytes
-hot-keys   100                                             size of hot pool
-hot-ratio  0.8                                             fraction hitting hot pool
-csv        results/results.csv                             output file
-token      ""                                              auth token
```

**Interview question:** "How did you measure the system's performance?" — "I built an open-loop load generator that fires requests at a configurable rate using persistent connections. It uses HDR histograms to capture latency distribution at p50/p95/p99/p999 and saves results to CSV. I can run scenarios with different rates, read/write ratios, and hot-key distributions and compare results across runs."
