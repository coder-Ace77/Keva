#!/usr/bin/env bash
# evict_sweep.sh — compare eviction policies under memory pressure.
#
# Starts and tears down the cluster automatically for each policy,
# then writes all results to one CSV so you can compare rows by label.
#
# Usage (from repo root):
#   ./bench/evict_sweep.sh [output.csv] [max-memory]
#
# Defaults:
#   output.csv  : bench/results/eviction.csv
#   max-memory  : 64MB   (small enough to trigger eviction at 10k ops/s)
#
# What it measures
#   - noevict  : writes rejected when full → errors spike, latency stays flat
#   - relaxed  : random-sample eviction   → errors=0, slight latency bump
#   - strict   : exact LRU eviction       → errors=0, slightly higher write p99
#
# Each policy runs 3 load points (low / medium / high) for 45 s each.
# Total runtime ≈ 15 min.

set -euo pipefail

CSV="${1:-bench/results/eviction.csv}"
MAX_MEM="${2:-64MB}"
DURATION="45s"
WORKERS=32
COOLDOWN=5

mkdir -p "$(dirname "$CSV")"

echo "Building loadgen..."
go build -o bench/bin/loadgen ./bench/loadgen/
echo "Build OK"
echo ""
echo "Eviction comparison: noevict vs relaxed vs strict"
echo "Memory cap: ${MAX_MEM}   Output: ${CSV}"
echo ""

LG="bench/bin/loadgen"
ADDR="localhost:7001,localhost:7002,localhost:7003"

# run_scenario LABEL RATE READ_RATIO
run_scenario() {
    local LABEL="$1" RATE="$2" RATIO="$3"
    echo "  → ${LABEL}"
    "$LG" -addr "$ADDR" -csv "$CSV" -label "$LABEL" \
          -duration "$DURATION" -workers $WORKERS \
          -rate "$RATE" -read-ratio "$RATIO" \
          -keys 5000 -value-size 512
    sleep "$COOLDOWN"
}

for POLICY in noevict relaxed strict; do
    echo "══════════════════════════════════════════════"
    echo "  POLICY: ${POLICY}   MAX_MEMORY: ${MAX_MEM}"
    echo "══════════════════════════════════════════════"

    # Start cluster with this eviction policy
    ./cluster.sh up --max-memory "$MAX_MEM" --eviction "$POLICY"
    echo ""

    # Low load — should be no pressure regardless of policy
    run_scenario "${POLICY}-low-2k"    2000  0.5

    # Medium load — memory fills up; eviction decisions start mattering
    run_scenario "${POLICY}-medium-8k" 8000  0.5

    # Write-heavy at medium rate — more evictions triggered
    run_scenario "${POLICY}-writes-8k" 8000  0.1

    # Tear down before next policy
    echo ""
    ./cluster.sh down
    echo ""
    sleep 2  # let OS release ports
done

echo "══════════════════════════════════════════════"
echo "All policies done."
echo "CSV → ${CSV}"
echo ""
echo "Quick peek at results:"
echo ""
column -t -s',' "$CSV" | head -20
