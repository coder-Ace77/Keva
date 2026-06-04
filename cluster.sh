#!/usr/bin/env bash
# cluster.sh — manage a 3-node local KV cluster
#
# COMMANDS
#   up     build binary, start 3 nodes, wait for leader election
#   down   kill the running cluster and clean up
#   bench  run the full load-test sweep against the running cluster
#   status show which nodes are alive
#
# USAGE
#   ./cluster.sh up [--max-memory SIZE] [--eviction POLICY] [--token TOKEN]
#   ./cluster.sh bench [output.csv]
#   ./cluster.sh down
#   ./cluster.sh status
#
# OPTIONS (for 'up')
#   --max-memory SIZE    e.g. 256MB, 1GB, 512KB  (default: unlimited)
#   --eviction POLICY    noevict | relaxed | strict  (default: noevict)
#   --token TOKEN        shared auth token (default: none)
#
# EXAMPLES
#   ./cluster.sh up
#   ./cluster.sh up --max-memory 256MB --eviction relaxed
#   ./cluster.sh bench
#   ./cluster.sh bench results/eviction_relaxed.csv
#   ./cluster.sh down

set -euo pipefail

# ── fixed topology ────────────────────────────────────────────────────────────
NODE0_PORT=7001
NODE1_PORT=7002
NODE2_PORT=7003
PEERS="localhost:${NODE0_PORT},localhost:${NODE1_PORT},localhost:${NODE2_PORT}"

# ── state files (shared between up/down/status) ───────────────────────────────
STATE_DIR="/tmp/kv_cluster"
PID_FILE="${STATE_DIR}/pids"
WAL0="${STATE_DIR}/node0.wal"
WAL1="${STATE_DIR}/node1.wal"
WAL2="${STATE_DIR}/node2.wal"
BINARY="${STATE_DIR}/kvstore"
LOG0="${STATE_DIR}/node0.log"
LOG1="${STATE_DIR}/node1.log"
LOG2="${STATE_DIR}/node2.log"

# ─────────────────────────────────────────────────────────────────────────────

cmd_up() {
    local MAX_MEMORY=""
    local EVICTION="noevict"
    local TOKEN=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --max-memory) MAX_MEMORY="$2"; shift 2 ;;
            --eviction)   EVICTION="$2";   shift 2 ;;
            --token)      TOKEN="$2";      shift 2 ;;
            *) echo "Unknown option: $1"; exit 1 ;;
        esac
    done

    if [[ -f "$PID_FILE" ]]; then
        echo "Cluster already running. Run './cluster.sh down' first."
        exit 1
    fi

    mkdir -p "$STATE_DIR"

    echo "Building..."
    go build -o "$BINARY" .
    echo "Build OK"
    echo ""

    local COMMON_ENV=(
        KV_PEERS="${PEERS}"
        KV_WAL_MODE="interval"
        KV_EVICTION_POLICY="${EVICTION}"
    )
    [[ -n "$MAX_MEMORY" ]] && COMMON_ENV+=(KV_MAX_MEMORY="${MAX_MEMORY}")
    [[ -n "$TOKEN" ]]      && COMMON_ENV+=(KV_AUTH_TOKEN="${TOKEN}")

    env "${COMMON_ENV[@]}" \
        KV_PORT=":${NODE0_PORT}" \
        KV_NODE_ADDR="localhost:${NODE0_PORT}" \
        KV_WAL_PATH="${WAL0}" \
        "$BINARY" >"$LOG0" 2>&1 &
    local P0=$!

    env "${COMMON_ENV[@]}" \
        KV_PORT=":${NODE1_PORT}" \
        KV_NODE_ADDR="localhost:${NODE1_PORT}" \
        KV_WAL_PATH="${WAL1}" \
        "$BINARY" >"$LOG1" 2>&1 &
    local P1=$!

    env "${COMMON_ENV[@]}" \
        KV_PORT=":${NODE2_PORT}" \
        KV_NODE_ADDR="localhost:${NODE2_PORT}" \
        KV_WAL_PATH="${WAL2}" \
        "$BINARY" >"$LOG2" 2>&1 &
    local P2=$!

    echo "$P0 $P1 $P2" > "$PID_FILE"

    # Wait for leader election (~300 ms, give it 2 s to be safe).
    printf "Waiting for leader election"
    local elected=0
    for i in $(seq 1 20); do
        sleep 0.1
        if grep -q "role=leader" "$LOG0" "$LOG1" "$LOG2" 2>/dev/null; then
            elected=1
            break
        fi
        printf "."
    done
    echo ""

    if [[ $elected -eq 0 ]]; then
        echo "Warning: leader not detected yet (logs may lag). Cluster should be ready shortly."
    fi

    echo ""
    echo "┌──────────────────────────────────────────────────────────────┐"
    echo "│                  Cluster is running                          │"
    echo "├──────────────────────────────────────────────────────────────┤"
    printf "│  Node 0 : localhost:%-5s  (PID %-6s)                    │\n" "${NODE0_PORT}" "${P0}"
    printf "│  Node 1 : localhost:%-5s  (PID %-6s)                    │\n" "${NODE1_PORT}" "${P1}"
    printf "│  Node 2 : localhost:%-5s  (PID %-6s)                    │\n" "${NODE2_PORT}" "${P2}"
    echo "├──────────────────────────────────────────────────────────────┤"
    printf "│  eviction=%-10s  max_memory=%-20s      │\n" "${EVICTION}" "${MAX_MEMORY:-unlimited}"
    echo "├──────────────────────────────────────────────────────────────┤"
    echo "│  Logs: /tmp/kv_cluster/node{0,1,2}.log                      │"
    echo "│  Next steps:                                                 │"
    echo "│    ./cluster.sh bench                                        │"
    echo "│    ./cluster.sh bench results/run1.csv                       │"
    echo "│    ./cluster.sh down                                         │"
    echo "└──────────────────────────────────────────────────────────────┘"
}

cmd_down() {
    if [[ ! -f "$PID_FILE" ]]; then
        echo "No cluster PID file found. Nothing to stop."
        exit 0
    fi

    read -r P0 P1 P2 < "$PID_FILE"

    echo "Stopping nodes (PIDs: $P0 $P1 $P2)..."
    kill "$P0" "$P1" "$P2" 2>/dev/null || true
    # wait a moment for graceful shutdown
    sleep 0.5
    kill -9 "$P0" "$P1" "$P2" 2>/dev/null || true

    rm -f "$PID_FILE" "$WAL0" "$WAL1" "$WAL2" "$BINARY" \
          "$LOG0" "$LOG1" "$LOG2"
    rmdir "$STATE_DIR" 2>/dev/null || true

    echo "Cluster stopped and cleaned up."
}

cmd_bench() {
    local CSV="${1:-bench/results/results.csv}"

    if [[ ! -f "$PID_FILE" ]]; then
        echo "No cluster is running. Start one with: ./cluster.sh up"
        exit 1
    fi

    exec ./bench/sweep.sh \
        "localhost:${NODE0_PORT},localhost:${NODE1_PORT},localhost:${NODE2_PORT}" \
        "$CSV"
}

cmd_status() {
    if [[ ! -f "$PID_FILE" ]]; then
        echo "No cluster is running."
        exit 0
    fi

    read -r P0 P1 P2 < "$PID_FILE"
    echo "PID file: $PID_FILE"
    for port pid in "${NODE0_PORT}" "${P0}" "${NODE1_PORT}" "${P1}" "${NODE2_PORT}" "${P2}"; do
        :
    done

    local pairs=("${NODE0_PORT}:${P0}" "${NODE1_PORT}:${P1}" "${NODE2_PORT}:${P2}")
    for pair in "${pairs[@]}"; do
        local port="${pair%%:*}"
        local pid="${pair##*:}"
        if kill -0 "$pid" 2>/dev/null; then
            local role="follower"
            if grep -q "role=leader" "${STATE_DIR}/node${port##700}.log" 2>/dev/null; then
                role="leader"
            fi
            echo "  localhost:${port}  PID=${pid}  alive  (${role})"
        else
            echo "  localhost:${port}  PID=${pid}  DEAD"
        fi
    done
}

# ── dispatch ──────────────────────────────────────────────────────────────────

COMMAND="${1:-help}"
shift || true

case "$COMMAND" in
    up)     cmd_up "$@" ;;
    down)   cmd_down ;;
    bench)  cmd_bench "$@" ;;
    status) cmd_status ;;
    *)
        echo "Usage: $0 {up|down|bench|status}"
        echo ""
        echo "  up [--max-memory SIZE] [--eviction noevict|relaxed|strict] [--token TOKEN]"
        echo "  bench [output.csv]"
        echo "  down"
        echo "  status"
        exit 1
        ;;
esac
