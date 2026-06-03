#!/usr/bin/env bash
# sweep.sh — runs all load scenarios and writes every result to one CSV file.
#
# Usage (on the EC2 machine, from the repo root):
#   ./bench/sweep.sh [cluster-addr] [output-csv]
#
# Defaults:
#   cluster-addr : localhost:7001,localhost:7002,localhost:7003
#   output-csv   : bench/results/results.csv
#
# Total run time ≈ 30–35 min with 60s per scenario.
# Cut to 30s by changing DURATION below if you're short on EC2 time.

set -euo pipefail

ADDR="${1:-localhost:7001,localhost:7002,localhost:7003}"
CSV="${2:-bench/results/results.csv}"
DURATION="60s"
WORKERS=64
COOLDOWN=8   # seconds between runs (lets the leader flush and GC)

mkdir -p "$(dirname "$CSV")"

# Build a fresh binary
echo "Building loadgen..."
go build -o bench/bin/loadgen ./bench/loadgen/
echo "Binary ready."
echo "Results will be written to: $CSV"
echo ""

LG="bench/bin/loadgen"

# ─── helper: run one scenario ─────────────────────────────────────────────────
run() {
    local LABEL="$1"; shift
    echo "────────────────────────────────────────────"
    echo "  $LABEL"
    echo "────────────────────────────────────────────"
    "$LG" -addr "$ADDR" -csv "$CSV" -label "$LABEL" \
          -duration "$DURATION" -workers $WORKERS "$@"
    echo "  (cooling down ${COOLDOWN}s)"
    sleep "$COOLDOWN"
    echo ""
}

# ═════════════════════════════════════════════════════════════
# 1. THROUGHPUT SWEEP
# Fixed: 50/50 read-write, 64-byte values, 5000 keys.
# Purpose: find the saturation knee and build the
#          throughput-vs-latency curve for the resume.
# ═════════════════════════════════════════════════════════════
echo "═══ 1. THROUGHPUT SWEEP (50/50, 64 B) ═══"

run "throughput-1k"   -rate   1000 -read-ratio 0.5 -keys 5000 -value-size 64
run "throughput-5k"   -rate   5000 -read-ratio 0.5 -keys 5000 -value-size 64
run "throughput-10k"  -rate  10000 -read-ratio 0.5 -keys 5000 -value-size 64
run "throughput-20k"  -rate  20000 -read-ratio 0.5 -keys 5000 -value-size 64
run "throughput-50k"  -rate  50000 -read-ratio 0.5 -keys 5000 -value-size 64
run "throughput-100k" -rate 100000 -read-ratio 0.5 -keys 5000 -value-size 64

# ═════════════════════════════════════════════════════════════
# 2. READ / WRITE RATIO
# Fixed: 10k ops/s, 64-byte values, 5000 keys.
# Purpose: show what happens when reads dominate (cache-like)
#          vs writes dominate (write-log-like).
# ═════════════════════════════════════════════════════════════
echo "═══ 2. READ / WRITE RATIO (10k ops/s, 64 B) ═══"

run "ratio-read-only"   -rate 10000 -read-ratio 1.0  -keys 5000 -value-size 64
run "ratio-read-heavy"  -rate 10000 -read-ratio 0.9  -keys 5000 -value-size 64
run "ratio-balanced"    -rate 10000 -read-ratio 0.5  -keys 5000 -value-size 64
run "ratio-write-heavy" -rate 10000 -read-ratio 0.1  -keys 5000 -value-size 64
run "ratio-write-only"  -rate 10000 -read-ratio 0.0  -keys 5000 -value-size 64

# ═════════════════════════════════════════════════════════════
# 3. VALUE SIZE (PAYLOAD) SWEEP
# Fixed: 10k ops/s, 50/50, 1000 keys.
# Purpose: show where serialisation + TCP bandwidth starts
#          hurting — typically above 4–16 KB.
# ═════════════════════════════════════════════════════════════
echo "═══ 3. PAYLOAD SIZE (10k ops/s, 50/50) ═══"

run "payload-64b"   -rate 10000 -read-ratio 0.5 -keys 1000 -value-size 64
run "payload-256b"  -rate 10000 -read-ratio 0.5 -keys 1000 -value-size 256
run "payload-1kb"   -rate 10000 -read-ratio 0.5 -keys 1000 -value-size 1024
run "payload-4kb"   -rate 10000 -read-ratio 0.5 -keys 1000 -value-size 4096
run "payload-16kb"  -rate 10000 -read-ratio 0.5 -keys 1000 -value-size 16384
run "payload-64kb"  -rate 10000 -read-ratio 0.5 -keys 1000 -value-size 65536

# ═════════════════════════════════════════════════════════════
# 4. KEY SPACE SIZE
# Fixed: 10k ops/s, 50/50, 64-byte values.
# Purpose: verify that a large or tiny key space doesn't
#          change latency (hash-map lookup is O(1)).
#          Tiny key space = maximum shard lock contention.
# ═════════════════════════════════════════════════════════════
echo "═══ 4. KEY SPACE SIZE (10k ops/s, 50/50, 64 B) ═══"

run "keys-10"    -rate 10000 -read-ratio 0.5 -keys 10     -value-size 64
run "keys-1k"    -rate 10000 -read-ratio 0.5 -keys 1000   -value-size 64
run "keys-100k"  -rate 10000 -read-ratio 0.5 -keys 100000 -value-size 64
run "keys-1m"    -rate 10000 -read-ratio 0.5 -keys 1000000 -value-size 64

# ═════════════════════════════════════════════════════════════
# 5. HOT KEY (SKEWED ACCESS)
# Fixed: 10k ops/s, 50/50, 5000 total keys, 64-byte values.
# Purpose: simulate a "celebrity key" / Zipfian workload
#          where a small set of keys gets most of the traffic.
#          Stresses one shard's mutex disproportionately.
# ═════════════════════════════════════════════════════════════
echo "═══ 5. HOT KEY SKEW (10k ops/s, 50/50, 64 B) ═══"

run "hotkey-uniform"  -rate 10000 -read-ratio 0.5 -keys 5000 \
                      -hot-keys 0                   -value-size 64

run "hotkey-skew-50"  -rate 10000 -read-ratio 0.5 -keys 5000 \
                      -hot-keys 10 -hot-ratio 0.5   -value-size 64

run "hotkey-skew-80"  -rate 10000 -read-ratio 0.5 -keys 5000 \
                      -hot-keys 10 -hot-ratio 0.8   -value-size 64

run "hotkey-skew-95"  -rate 10000 -read-ratio 0.5 -keys 5000 \
                      -hot-keys 5  -hot-ratio 0.95  -value-size 64

# ═════════════════════════════════════════════════════════════
echo ""
echo "All scenarios complete."
echo "CSV → $CSV"
echo ""
echo "First few rows:"
head -5 "$CSV"
