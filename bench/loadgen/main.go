// loadgen: open-loop load generator for the KV cluster.
//
// Fires GET and SET requests at a fixed rate using persistent TCP connections,
// records per-op latency in HDR histograms, prints a human-readable summary,
// and optionally appends one row to a CSV file for later analysis.
//
// Usage:
//
//	go run ./bench/loadgen \
//	    -addr      localhost:7001,localhost:7002,localhost:7003 \
//	    -label     balanced-10k \
//	    -rate      10000 \
//	    -duration  60s \
//	    -read-ratio 0.5 \
//	    -csv       results/results.csv
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"

	"key_value_store/bench/client"
	"key_value_store/protocol"
)

const (
	maxLatencyMicros = 30_000_000
	histSigFigs      = 3
)

type workerResult struct {
	writeHist *hdrhistogram.Histogram
	readHist  *hdrhistogram.Histogram
	writeOps  int64
	readOps   int64
	errors    int64
}

func main() {
	addr      := flag.String("addr",       "localhost:6379", "comma-separated seed node addresses")
	label     := flag.String("label",      "",               "scenario name (appears in output and CSV)")
	csvPath   := flag.String("csv",        "",               "append one result row to this CSV file")
	rate      := flag.Int("rate",          5000,             "target ops/sec (open-loop)")
	dur       := flag.Duration("duration", 60*time.Second,   "test duration")
	workers   := flag.Int("workers",       64,               "number of persistent connections")
	readRatio := flag.Float64("read-ratio",0.5,              "fraction of ops that are reads [0,1]")
	numKeys   := flag.Int("keys",          5000,             "key-space size")
	valSize   := flag.Int("value-size",    64,               "value payload size in bytes")
	hotKeys   := flag.Int("hot-keys",      0,                "first N keys form the hot pool (0 = uniform)")
	hotRatio  := flag.Float64("hot-ratio", 0.8,              "fraction of ops hitting the hot pool")
	token     := flag.String("token",      os.Getenv("KV_AUTH_TOKEN"), "auth token")
	flag.Parse()

	seeds := strings.Split(*addr, ",")

	fmt.Printf("discovering cluster via %v ...\n", seeds)
	leaderAddr, peers, err := client.WaitForLeader(seeds, *token, 10*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	allSeeds := mergeSeeds(seeds, peers)
	fmt.Printf("leader : %s\n", leaderAddr)

	value := make([]byte, *valSize)
	for i := range value {
		value[i] = 'x'
	}

	keys := make([][]byte, *numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench:k:%07d", i))
	}

	hk := *hotKeys
	if hk >= len(keys) {
		hk = 0
	}

	workCh := make(chan bool, *workers*8)

	wResults := make([]*workerResult, *workers)
	for i := range wResults {
		wResults[i] = &workerResult{
			writeHist: hdrhistogram.New(1, maxLatencyMicros, histSigFigs),
			readHist:  hdrhistogram.New(1, maxLatencyMicros, histSigFigs),
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go runWorker(i, allSeeds, *token, keys, hk, *hotRatio, value, workCh, wResults[i], &wg)
	}

	// Open-loop coordinator: batch tokens so we tick at most 1000 Hz
	batchSize    := max(1, *rate/1000)
	tickInterval := time.Duration(int64(time.Second) * int64(batchSize) / int64(*rate))

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sig:
			cancel()
		case <-ctx.Done():
		}
	}()

	var droppedTokens int64
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	rr    := *readRatio
	start := time.Now()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			for i := 0; i < batchSize; i++ {
				select {
				case workCh <- rand.Float64() >= rr: // true = write
				default:
					atomic.AddInt64(&droppedTokens, 1)
				}
			}
		}
	}

	close(workCh)
	wg.Wait()
	elapsed := time.Since(start)

	// Merge per-worker histograms
	totalWrite := hdrhistogram.New(1, maxLatencyMicros, histSigFigs)
	totalRead  := hdrhistogram.New(1, maxLatencyMicros, histSigFigs)
	var totalWriteOps, totalReadOps, totalErrors int64
	for _, r := range wResults {
		totalWrite.Merge(r.writeHist)
		totalRead.Merge(r.readHist)
		totalWriteOps += atomic.LoadInt64(&r.writeOps)
		totalReadOps  += atomic.LoadInt64(&r.readOps)
		totalErrors   += atomic.LoadInt64(&r.errors)
	}

	totalOps   := totalWriteOps + totalReadOps
	actualRate := float64(totalOps) / elapsed.Seconds()

	// ── human-readable output ─────────────────────────────────────────────────
	heading := "Load Generator Results"
	if *label != "" {
		heading += " — " + *label
	}
	fmt.Printf("\n=== %s ===\n", heading)
	fmt.Printf("%-24s %s\n",      "Scenario:",       orDash(*label))
	fmt.Printf("%-24s %s\n",      "Target node:",    leaderAddr)
	fmt.Printf("%-24s %.1fs\n",   "Duration:",       elapsed.Seconds())
	fmt.Printf("%-24s %d\n",      "Workers:",        *workers)
	fmt.Printf("%-24s %d ops/s\n","Target rate:",    *rate)
	fmt.Printf("%-24s %.0f ops/s  (%d total)\n", "Actual rate:", actualRate, totalOps)
	fmt.Printf("%-24s %d (%.3f%%)\n", "Errors:",
		totalErrors, 100*pct(totalErrors, totalOps+totalErrors))
	fmt.Printf("%-24s %d\n",      "Dropped tokens:", droppedTokens)
	fmt.Printf("%-24s %.1f%%\n",  "Read ratio:",     *readRatio*100)
	fmt.Printf("%-24s %d bytes\n","Value size:",     *valSize)
	fmt.Println()
	printHist("Write latency (µs)", totalWrite, totalWriteOps)
	printHist("Read  latency (µs)", totalRead,  totalReadOps)

	// ── CSV row ───────────────────────────────────────────────────────────────
	if *csvPath != "" {
		if err := appendCSV(*csvPath, csvRow{
			Label:        *label,
			Timestamp:    time.Now().Format("2006-01-02 15:04:05"),
			TargetRate:   *rate,
			ActualRate:   int(actualRate),
			DurationSec:  elapsed.Seconds(),
			Workers:      *workers,
			ReadRatioPct: *readRatio * 100,
			ValueBytes:   *valSize,
			KeyCount:     *numKeys,
			HotKeys:      hk,
			HotRatioPct:  *hotRatio * 100,
			WriteOps:     totalWriteOps,
			ReadOps:      totalReadOps,
			Errors:       totalErrors,
			Dropped:      droppedTokens,
			Wp50:  totalWrite.ValueAtQuantile(50),
			Wp95:  totalWrite.ValueAtQuantile(95),
			Wp99:  totalWrite.ValueAtQuantile(99),
			Wp999: totalWrite.ValueAtQuantile(99.9),
			Wmax:  totalWrite.Max(),
			Rp50:  totalRead.ValueAtQuantile(50),
			Rp95:  totalRead.ValueAtQuantile(95),
			Rp99:  totalRead.ValueAtQuantile(99),
			Rp999: totalRead.ValueAtQuantile(99.9),
			Rmax:  totalRead.Max(),
		}); err != nil {
			fmt.Fprintf(os.Stderr, "csv write error: %v\n", err)
		} else {
			fmt.Printf("\nCSV row appended → %s\n", *csvPath)
		}
	}
}

// ── worker ────────────────────────────────────────────────────────────────────

func runWorker(
	id int,
	seeds []string,
	token string,
	keys [][]byte,
	hotKeys int,
	hotRatio float64,
	value []byte,
	workCh <-chan bool,
	r *workerResult,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	var conn *client.Conn
	reconnect := func() {
		if conn != nil {
			conn.Close()
		}
		for {
			la, _, err := client.DiscoverLeader(seeds, token)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			c, err := client.Dial(la, token, 3*time.Second)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			conn = c
			return
		}
	}
	reconnect()
	defer conn.Close()

	rng := rand.New(rand.NewSource(int64(id) * 0x9e3779b9))

	pickKey := func() []byte {
		if hotKeys > 0 && rng.Float64() < hotRatio {
			return keys[rng.Intn(hotKeys)]
		}
		if hotKeys > 0 && len(keys) > hotKeys {
			return keys[hotKeys+rng.Intn(len(keys)-hotKeys)]
		}
		return keys[rng.Intn(len(keys))]
	}

	for isWrite := range workCh {
		key   := pickKey()
		start := time.Now()

		var resp protocol.Message
		var opErr error
		if isWrite {
			resp, opErr = conn.Do(&protocol.SetPayload{Key: key, Value: value}, 5*time.Second)
		} else {
			resp, opErr = conn.Do(&protocol.GetPayload{Key: key}, 5*time.Second)
		}
		latUs := time.Since(start).Microseconds()

		if opErr != nil {
			atomic.AddInt64(&r.errors, 1)
			reconnect()
			continue
		}
		if _, isErr := resp.(*protocol.ErrorMessage); isErr {
			atomic.AddInt64(&r.errors, 1)
			continue
		}

		if isWrite {
			r.writeHist.RecordValue(latUs)
			atomic.AddInt64(&r.writeOps, 1)
		} else {
			r.readHist.RecordValue(latUs)
			atomic.AddInt64(&r.readOps, 1)
		}
	}
}

// ── CSV ───────────────────────────────────────────────────────────────────────

var csvHeader = []string{
	"label", "timestamp",
	"target_rate", "actual_rate", "duration_s",
	"workers", "read_ratio_pct", "value_bytes", "key_count", "hot_keys", "hot_ratio_pct",
	"write_ops", "read_ops", "errors", "dropped_tokens",
	"wp50_us", "wp95_us", "wp99_us", "wp999_us", "wmax_us",
	"rp50_us", "rp95_us", "rp99_us", "rp999_us", "rmax_us",
}

type csvRow struct {
	Label, Timestamp                    string
	TargetRate, ActualRate              int
	DurationSec                         float64
	Workers                             int
	ReadRatioPct, HotRatioPct           float64
	ValueBytes, KeyCount, HotKeys       int
	WriteOps, ReadOps, Errors, Dropped  int64
	Wp50, Wp95, Wp99, Wp999, Wmax       int64
	Rp50, Rp95, Rp99, Rp999, Rmax       int64
}

func (row csvRow) toRecord() []string {
	f := func(v int64) string { return strconv.FormatInt(v, 10) }
	return []string{
		row.Label, row.Timestamp,
		strconv.Itoa(row.TargetRate), strconv.Itoa(row.ActualRate),
		fmt.Sprintf("%.1f", row.DurationSec),
		strconv.Itoa(row.Workers),
		fmt.Sprintf("%.1f", row.ReadRatioPct),
		strconv.Itoa(row.ValueBytes), strconv.Itoa(row.KeyCount),
		strconv.Itoa(row.HotKeys), fmt.Sprintf("%.1f", row.HotRatioPct),
		f(row.WriteOps), f(row.ReadOps), f(row.Errors), f(row.Dropped),
		f(row.Wp50), f(row.Wp95), f(row.Wp99), f(row.Wp999), f(row.Wmax),
		f(row.Rp50), f(row.Rp95), f(row.Rp99), f(row.Rp999), f(row.Rmax),
	}
}

func appendCSV(path string, row csvRow) error {
	needHeader := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		needHeader = true
	}

	if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if needHeader {
		if err := w.Write(csvHeader); err != nil {
			return err
		}
	}
	if err := w.Write(row.toRecord()); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

// ── helpers ───────────────────────────────────────────────────────────────────

func printHist(label string, h *hdrhistogram.Histogram, ops int64) {
	if ops == 0 {
		fmt.Printf("%s: no operations\n", label)
		return
	}
	fmt.Printf("%s  [%d ops]\n", label, ops)
	fmt.Printf("  p50=%7d  p95=%7d  p99=%7d  p999=%7d  max=%7d\n",
		h.ValueAtQuantile(50), h.ValueAtQuantile(95),
		h.ValueAtQuantile(99), h.ValueAtQuantile(99.9), h.Max())
}

func mergeSeeds(a, b []string) []string {
	seen := make(map[string]bool)
	out  := make([]string, 0, len(a)+len(b))
	for _, s := range append(a, b...) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func dirOf(path string) string {
	i := strings.LastIndex(path, "/")
	if i < 0 {
		return "."
	}
	return path[:i]
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

func pct(num, den int64) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
