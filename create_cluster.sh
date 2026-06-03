#!/usr/bin/env bash
set -euo pipefail

# ─── Build ────────────────────────────────────────────────────────────────────
echo "Building..."
go build -o ./kvstore .
echo "Build OK"

# ─── Random ports (non-overlapping ranges) ────────────────────────────────────
NODE0_PORT=$(( (RANDOM % 5000) + 20000 ))
NODE1_PORT=$(( (RANDOM % 5000) + 25000 ))
NODE2_PORT=$(( (RANDOM % 5000) + 30000 ))

PEERS="localhost:${NODE0_PORT},localhost:${NODE1_PORT},localhost:${NODE2_PORT}"

# ─── Temp WAL files ───────────────────────────────────────────────────────────
NODE0_WAL=$(mktemp /tmp/kv_node0_XXXXXX.wal)
NODE1_WAL=$(mktemp /tmp/kv_node1_XXXXXX.wal)
NODE2_WAL=$(mktemp /tmp/kv_node2_XXXXXX.wal)

cleanup() {
    echo ""
    echo "Stopping cluster..."
    kill "$NODE0_PID" "$NODE1_PID" "$NODE2_PID" 2>/dev/null || true
    wait "$NODE0_PID" "$NODE1_PID" "$NODE2_PID" 2>/dev/null || true
    rm -f "$NODE0_WAL" "$NODE1_WAL" "$NODE2_WAL" ./kvstore
    echo "Done."
}
trap cleanup EXIT INT TERM

# ─── Start all three nodes simultaneously ────────────────────────────────────
# All start as followers; Raft elects the leader automatically.

KV_PORT=":${NODE0_PORT}" \
KV_NODE_ADDR="localhost:${NODE0_PORT}" \
KV_PEERS="${PEERS}" \
KV_WAL_PATH="${NODE0_WAL}" \
    ./kvstore &
NODE0_PID=$!

KV_PORT=":${NODE1_PORT}" \
KV_NODE_ADDR="localhost:${NODE1_PORT}" \
KV_PEERS="${PEERS}" \
KV_WAL_PATH="${NODE1_WAL}" \
    ./kvstore &
NODE1_PID=$!

KV_PORT=":${NODE2_PORT}" \
KV_NODE_ADDR="localhost:${NODE2_PORT}" \
KV_PEERS="${PEERS}" \
KV_WAL_PATH="${NODE2_WAL}" \
    ./kvstore &
NODE2_PID=$!

# Brief pause for election to complete (~300ms).
sleep 0.6

echo ""
echo "┌──────────────────────────────────────────────────────────────┐"
echo "│                    Cluster is running                        │"
echo "├──────────────────────────────────────────────────────────────┤"
printf "│  Node 0 : localhost:%-5s  (PID %-6s)                    │\n" "${NODE0_PORT}" "${NODE0_PID}"
printf "│  Node 1 : localhost:%-5s  (PID %-6s)                    │\n" "${NODE1_PORT}" "${NODE1_PID}"
printf "│  Node 2 : localhost:%-5s  (PID %-6s)                    │\n" "${NODE2_PORT}" "${NODE2_PID}"
echo "├──────────────────────────────────────────────────────────────┤"
echo "│  Connect CLI to any node — it discovers the leader itself:  │"
printf "│    go run ./cli -u localhost:%-5s\n" "${NODE0_PORT}"
printf "│    go run ./cli -u localhost:%-5s\n" "${NODE1_PORT}"
printf "│    go run ./cli -u localhost:%-5s\n" "${NODE2_PORT}"
echo "│                                                              │"
echo "│  Kill a node (e.g. Node 0) to trigger a new election:      │"
printf "│    kill %s\n" "${NODE0_PID}"
echo "│                                                              │"
echo "│  Press Ctrl+C to stop all nodes.                            │"
echo "└──────────────────────────────────────────────────────────────┘"

wait
