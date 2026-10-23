package main

import (
	"key_value_store/cluster"
	"key_value_store/config"
	"key_value_store/server"
	"key_value_store/storage/eviction"
	"key_value_store/storage/store"
	"key_value_store/storage/wal"
	"key_value_store/utils"
	"log"
)

func main() {
	cfg := config.Load()

	walOpts := wal.Options{
		Mode:          wal.SyncMode(cfg.WAL.Mode),
		FlushInterval: cfg.WAL.FlushInterval,
		MaxBatchSize:  cfg.WAL.MaxBatchSize,
	}

	storeOpts := store.Options{
		ShardCount:     cfg.Store.ShardCount,
		DefaultTTL:     cfg.Store.DefaultTTL,
		MaxMemoryBytes: cfg.Store.MaxMemoryBytes,
		NewEviction:    eviction.ByName(cfg.Store.EvictionPolicy),
	}

	db, err := store.NewKVStore(cfg.Store.WALPath, walOpts, storeOpts)
	if err != nil {
		log.Fatalf("failed to start store: %v", err)
	}
	defer db.Close()

	peers := cfg.Cluster.Peers
	nodeAddr := cfg.Cluster.NodeAddr

	// standalone mode
	if nodeAddr == "" || len(peers) == 0 {
		log.Printf("mode=standalone  port=%s  wal=%s  eviction=%s  max_memory=%s",
			cfg.Server.Port, cfg.Store.WALPath,
			cfg.Store.EvictionPolicy, utils.FormatMemoryBytes(cfg.Store.MaxMemoryBytes))
		node := cluster.NewNode(db, cfg.Store.DefaultTTL, "", nil)
		node.ForceLeader()
		tcpServer := server.NewTCPServer(node, cfg.Server.MaxPayloadBytes)
		if err := tcpServer.Start(cfg.Server.Port); err != nil {
			log.Fatalf("network error: %v", err)
		}
		return
	}

	log.Printf("mode=cluster  node=%s  peers=%v  port=%s  wal=%s  eviction=%s  max_memory=%s",
		nodeAddr, peers, cfg.Server.Port, cfg.Store.WALPath,
		cfg.Store.EvictionPolicy, utils.FormatMemoryBytes(cfg.Store.MaxMemoryBytes))

	node := cluster.NewNode(db, cfg.Store.DefaultTTL, nodeAddr, peers)
	node.Start()

	tcpServer := server.NewTCPServer(node, cfg.Server.MaxPayloadBytes)
	if err := tcpServer.Start(cfg.Server.Port); err != nil {
		log.Fatalf("network error: %v", err)
	}
}
