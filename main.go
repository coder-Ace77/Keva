package main

import (
	"key_value_store/cluster"
	"key_value_store/config"
	"key_value_store/server"
	"key_value_store/store"
	"log"
)

func main() {
	cfg := config.Load()

	walOpts := store.WALOptions{
		Mode:          store.SyncMode(cfg.WAL.Mode),
		FlushInterval: cfg.WAL.FlushInterval,
		MaxBatchSize:  cfg.WAL.MaxBatchSize,
	}
	storeOpts := store.StoreOptions{
		ShardCount: cfg.Store.ShardCount,
		DefaultTTL: cfg.Store.DefaultTTL,
	}

	db, err := store.NewKVStore(cfg.Store.WALPath, walOpts, storeOpts)
	if err != nil {
		log.Fatalf("failed to start store: %v", err)
	}
	defer db.Close()

	peers := cfg.Cluster.Peers
	nodeAddr := cfg.Cluster.NodeAddr

	if nodeAddr == "" || len(peers) == 0 {
		// Standalone mode — single node, no Raft.
		log.Printf("mode=standalone  port=%s  wal=%s", cfg.Server.Port, cfg.Store.WALPath)
		node := cluster.NewNode(db, cfg.Store.DefaultTTL, "", nil)
		// Immediately promote to leader so writes are accepted.
		node.ForceLeader()
		tcpServer := server.NewTCPServer(node, cfg.Server.MaxPayloadBytes)
		if err := tcpServer.Start(cfg.Server.Port); err != nil {
			log.Fatalf("network error: %v", err)
		}
		return
	}

	log.Printf("mode=cluster  node=%s  peers=%v  port=%s  wal=%s",
		nodeAddr, peers, cfg.Server.Port, cfg.Store.WALPath)

	node := cluster.NewNode(db, cfg.Store.DefaultTTL, nodeAddr, peers)
	node.Start()

	tcpServer := server.NewTCPServer(node, cfg.Server.MaxPayloadBytes)
	if err := tcpServer.Start(cfg.Server.Port); err != nil {
		log.Fatalf("network error: %v", err)
	}
}
