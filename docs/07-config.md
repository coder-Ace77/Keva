# Package: config

**File:** `config/config.go`

The config package provides a single `Config` struct that drives all tuneable behaviour in the system. Configuration is loaded exclusively from environment variables — there is no config file.

---

## Config struct hierarchy

```
Config
├── ServerConfig
│   ├── Port            string         (default: ":6379")
│   ├── MaxPayloadBytes uint32         (default: 10 MB)
│   └── AuthToken       string         (default: "" = no auth)
│
├── StoreConfig
│   ├── ShardCount      int            (default: 32)
│   ├── DefaultTTL      time.Duration  (default: 15 minutes)
│   ├── WALPath         string         (default: "production.wal")
│   ├── MaxMemoryBytes  int64          (default: 0 = unlimited)
│   └── EvictionPolicy string         (default: "noevict")
│
├── WALConfig
│   ├── Mode            string         (default: "interval")
│   ├── FlushInterval   time.Duration  (default: 500 ms)
│   └── MaxBatchSize    int            (default: 1000)
│
└── ClusterConfig
    ├── NodeAddr        string         (default: "" = standalone)
    └── Peers           []string       (default: nil = standalone)
```

---

## Environment variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `KV_PORT` | string | `:6379` | TCP listen address (same default as Redis) |
| `KV_MAX_PAYLOAD_BYTES` | uint32 | 10485760 | Max incoming message size |
| `KV_SHARD_COUNT` | int | 32 | Number of hash map shards |
| `KV_DEFAULT_TTL` | duration | `15m` | TTL when client sends TTL=0 |
| `KV_WAL_PATH` | string | `production.wal` | Path to WAL file |
| `KV_WAL_MODE` | string | `interval` | `always`, `interval`, or `none` |
| `KV_WAL_FLUSH_INTERVAL` | duration | `500ms` | Flush period for interval mode |
| `KV_WAL_BATCH_SIZE` | int | 1000 | Max batch size before forced flush |
| `KV_AUTH_TOKEN` | string | `` | Pre-shared auth token (empty = no auth) |
| `KV_NODE_ADDR` | string | `` | This node's own address (required for cluster) |
| `KV_PEERS` | string | `` | Comma-separated peer addresses |
| `KV_MAX_MEMORY` | string | `` | Memory limit, e.g. `512MB`, `2GB` |
| `KV_EVICTION_POLICY` | string | `noevict` | `noevict`, `relaxed`, `strict` |

---

## Load()

```go
func Load() Config {
    cfg := Default()           // start with all defaults
    if v := os.Getenv("KV_PORT"); v != "" {
        cfg.Server.Port = v
    }
    // ... override each field if env var is set
    return cfg
}
```

`Default()` returns a fully populated `Config` with production-sensible values. `Load()` applies env overrides on top. Any unset variable keeps the default — you only need to set what you want to change.

---

## parseMemoryBytes

```go
func parseMemoryBytes(s string) (int64, error) {
    // Handles: "512MB", "2GB", "256KB", "1073741824"
    // Case-insensitive suffixes: KB/K, MB/M, GB/G, TB/T
}
```

Uses bit shifts rather than multiplication to avoid float arithmetic:
- KB = 1 << 10 (1024)
- MB = 1 << 20 (1,048,576)
- GB = 1 << 30
- TB = 1 << 40

**Interview question:** "Why use bit shifts instead of multiplying by 1024?" — It's idiomatic for binary byte sizes, avoids floating-point rounding, and makes the intent obvious to other systems programmers.

---

## FormatMemoryBytes

The inverse of `parseMemoryBytes`, used in startup log messages:
```
0         → "unlimited"
1073741824 → "1GB"
536870912  → "512MB"
```

---

## Design choice: environment variables only

No config file, no flags (except `--gen-token`). Environment variables integrate naturally with:
- Docker: `docker run -e KV_PORT=:7001 ...`
- Kubernetes: `env:` in pod spec
- Shell scripts: `export KV_PEERS=...` then `./keyvaluestore`

The `cluster.sh` and `create_cluster.sh` scripts use this to launch multiple nodes with different `KV_NODE_ADDR`, `KV_PORT`, and `KV_WAL_PATH` values in a single shell session.
