package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Server  ServerConfig
	Store   StoreConfig
	WAL     WALConfig
	Cluster ClusterConfig
}

type ClusterConfig struct {
	// NodeAddr is this node's own public address (e.g. "localhost:7001").
	// Required for cluster mode.
	NodeAddr string
	// Peers is the full list of cluster node addresses, including this node.
	// All nodes must share the same list.
	Peers []string
}

type ServerConfig struct {
	Port            string
	MaxPayloadBytes uint32
	// AuthToken is the pre-shared secret clients must present before any command.
	// Empty string disables auth (useful for local dev / testing).
	AuthToken string
}

type StoreConfig struct {
	ShardCount int
	DefaultTTL time.Duration
	WALPath    string
}

type WALConfig struct {
	// Mode controls durability vs throughput: "always" | "interval" | "none"
	Mode          string
	FlushInterval time.Duration
	MaxBatchSize  int
}

// Default returns a Config with production-grade sensible defaults.
func Default() Config {
	return Config{
		Server: ServerConfig{
			Port:            ":6379",
			MaxPayloadBytes: 10 * 1024 * 1024, // 10 MB
		},
		Store: StoreConfig{
			ShardCount: 32,
			DefaultTTL: 15 * time.Minute,
			WALPath:    "production.wal",
		},
		WAL: WALConfig{
			Mode:          "interval",
			FlushInterval: 500 * time.Millisecond,
			MaxBatchSize:  1000,
		},
	}
}

// Load starts from Default() and applies environment variable overrides.
//
// Environment variables:
//
//	KV_PORT                 Server listen address       (default: ":6379")
//	KV_MAX_PAYLOAD_BYTES    Max message size in bytes   (default: 10485760)
//	KV_SHARD_COUNT          Hash map shard count        (default: 32)
//	KV_DEFAULT_TTL          Key TTL as duration string  (default: "15m")
//	KV_WAL_PATH             WAL file path               (default: "production.wal")
//	KV_WAL_MODE             WAL sync mode               (default: "interval")
//	KV_WAL_FLUSH_INTERVAL   WAL flush interval          (default: "500ms")
//	KV_WAL_BATCH_SIZE       WAL max batch size          (default: 1000)
//	KV_NODE_ADDR            This node's own address     (e.g. "localhost:7001")
//	KV_PEERS               Comma-separated peer list   (e.g. "localhost:7000,localhost:7001,localhost:7002")
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
	if v := os.Getenv("KV_AUTH_TOKEN"); v != "" {
		cfg.Server.AuthToken = v
	}
	if v := os.Getenv("KV_NODE_ADDR"); v != "" {
		cfg.Cluster.NodeAddr = v
	}
	if v := os.Getenv("KV_PEERS"); v != "" {
		cfg.Cluster.Peers = strings.Split(v, ",")
	}

	return cfg
}
