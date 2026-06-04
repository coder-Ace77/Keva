package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"key_value_store/cluster"
	"key_value_store/config"
	"key_value_store/server"
	"key_value_store/store"
	"log"
	"os"
)

func main() {
	genToken := flag.Bool("gen-token", false, "generate a random auth token, print it, and exit")
	flag.Parse()

	if *genToken {
		token, err := generateToken()
		if err != nil {
			log.Fatalf("failed to generate token: %v", err)
		}
		fmt.Println(token)
		fmt.Fprintf(os.Stderr, "Set KV_AUTH_TOKEN=%s on both server and client.\n", token)
		os.Exit(0)
	}

	cfg := config.Load()

	walOpts := store.WALOptions{
		Mode:          store.SyncMode(cfg.WAL.Mode),
		FlushInterval: cfg.WAL.FlushInterval,
		MaxBatchSize:  cfg.WAL.MaxBatchSize,
	}

	eviction := evictionPolicy(cfg.Store.EvictionPolicy)

	storeOpts := store.StoreOptions{
		ShardCount:     cfg.Store.ShardCount,
		DefaultTTL:     cfg.Store.DefaultTTL,
		MaxMemoryBytes: cfg.Store.MaxMemoryBytes,
		Eviction:       eviction,
	}

	db, err := store.NewKVStore(cfg.Store.WALPath, walOpts, storeOpts)
	if err != nil {
		log.Fatalf("failed to start store: %v", err)
	}
	defer db.Close()

	peers := cfg.Cluster.Peers
	nodeAddr := cfg.Cluster.NodeAddr

	if nodeAddr == "" || len(peers) == 0 {
		log.Printf("mode=standalone  port=%s  wal=%s  auth=%v  eviction=%s  max_memory=%s",
			cfg.Server.Port, cfg.Store.WALPath, cfg.Server.AuthToken != "",
			cfg.Store.EvictionPolicy, config.FormatMemoryBytes(cfg.Store.MaxMemoryBytes))
		node := cluster.NewNode(db, cfg.Store.DefaultTTL, "", nil)
		node.ForceLeader()
		tcpServer := server.NewTCPServer(node, cfg.Server.MaxPayloadBytes, cfg.Server.AuthToken)
		if err := tcpServer.Start(cfg.Server.Port); err != nil {
			log.Fatalf("network error: %v", err)
		}
		return
	}

	log.Printf("mode=cluster  node=%s  peers=%v  port=%s  wal=%s  auth=%v  eviction=%s  max_memory=%s",
		nodeAddr, peers, cfg.Server.Port, cfg.Store.WALPath, cfg.Server.AuthToken != "",
		cfg.Store.EvictionPolicy, config.FormatMemoryBytes(cfg.Store.MaxMemoryBytes))

	node := cluster.NewNode(db, cfg.Store.DefaultTTL, nodeAddr, peers)
	node.Start()

	tcpServer := server.NewTCPServer(node, cfg.Server.MaxPayloadBytes, cfg.Server.AuthToken)
	if err := tcpServer.Start(cfg.Server.Port); err != nil {
		log.Fatalf("network error: %v", err)
	}
}

func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func evictionPolicy(name string) store.Eviction {
	switch name {
	case "relaxed":
		return store.SamplingEvict{}
	case "strict":
		return store.LRUEvict{}
	default: // "noevict" or empty
		return store.NoEvict{}
	}
}
