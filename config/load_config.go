package config

import (
	"key_value_store/utils"
	"os"
	"strconv"
	"strings"
	"time"
)

// Default returns a Config with production-grade sensible defaults.
func Default() Config {
	return Config{
		Server: ServerConfig{
			Port:            ":6379",
			MaxPayloadBytes: 10 * 1024 * 1024, // 10 MB
		},
		Store: StoreConfig{
			ShardCount:     32,
			DefaultTTL:     15 * time.Minute,
			WALPath:        "production.wal",
			MaxMemoryBytes: 0, // unlimited
			EvictionPolicy: "noevict",
		},
		WAL: WALConfig{
			Mode:          "interval",
			FlushInterval: 500 * time.Millisecond,
			MaxBatchSize:  1000,
		},
	}
}

// KV_PORT                 Server listen address       (default: ":6379")
// KV_MAX_PAYLOAD_BYTES    Max message size in bytes   (default: 10485760)
// KV_SHARD_COUNT          Hash map shard count        (default: 32)
// KV_DEFAULT_TTL          Key TTL as duration string  (default: "15m")
// KV_WAL_PATH             WAL file path               (default: "production.wal")
// KV_WAL_MODE             WAL sync mode               (default: "interval")
// KV_WAL_FLUSH_INTERVAL   WAL flush interval          (default: "500ms")
// KV_WAL_BATCH_SIZE       WAL max batch size          (default: 1000)
// KV_NODE_ADDR            This node's own address     (e.g. "localhost:7001")
// KV_PEERS               Comma-separated peer list   (e.g. "localhost:7000,localhost:7001,localhost:7002")
func Load() Config {
	cfg := Default()

	if v := os.Getenv("KV_PORT"); v != "" {
		cfg.Server.Port = v
	}
	if v := os.Getenv("KV_MAX_PAYLOAD_BYTES"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			cfg.Server.MaxPayloadBytes = uint32(n)
		}
	}
	if v := os.Getenv("KV_SHARD_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Store.ShardCount = n
		}
	}
	if v := os.Getenv("KV_DEFAULT_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.Store.DefaultTTL = d
		}
	}
	if v := os.Getenv("KV_WAL_PATH"); v != "" {
		cfg.Store.WALPath = v
	}
	if v := os.Getenv("KV_WAL_MODE"); v != "" {
		cfg.WAL.Mode = v
	}
	if v := os.Getenv("KV_WAL_FLUSH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.WAL.FlushInterval = d
		}
	}
	if v := os.Getenv("KV_WAL_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.WAL.MaxBatchSize = n
		}
	}
	if v := os.Getenv("KV_NODE_ADDR"); v != "" {
		cfg.Cluster.NodeAddr = v
	}
	if v := os.Getenv("KV_PEERS"); v != "" {
		cfg.Cluster.Peers = strings.Split(v, ",")
	}
	if v := os.Getenv("KV_MAX_MEMORY"); v != "" {
		if n, err := utils.ParseMemoryBytes(v); err == nil && n > 0 {
			cfg.Store.MaxMemoryBytes = n
		}
	}
	if v := os.Getenv("KV_EVICTION_POLICY"); v != "" {
		cfg.Store.EvictionPolicy = strings.ToLower(strings.TrimSpace(v))
	}

	return cfg
}
