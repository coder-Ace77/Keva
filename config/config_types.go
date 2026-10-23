package config

import "time"

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
}

type StoreConfig struct {
	ShardCount     int
	DefaultTTL     time.Duration
	WALPath        string
	MaxMemoryBytes int64  // 0 = unlimited; set via KV_MAX_MEMORY (e.g. "512MB")
	EvictionPolicy string // "noevict" | "relaxed" | "strict"  (default: "noevict")
}

type WALConfig struct {
	// Mode controls durability vs throughput: "always" | "interval" | "none"
	Mode          string
	FlushInterval time.Duration
	MaxBatchSize  int
}
