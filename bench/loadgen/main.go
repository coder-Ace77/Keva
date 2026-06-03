// loadgen: open-loop load generator for the KV cluster.
//
// Fires writes and reads at a fixed rate using N persistent connections to the
// leader, records per-op latency into HDR histograms, and prints a percentile
// report at the end.
//
// Usage:
//
//	go run ./bench/loadgen -addr localhost:7001,localhost:7002,localhost:7003 \
//	    -rate 10000 -duration 30s -workers 64 -read-ratio 0.5
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
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
	maxLatencyMicros = 30_000_000 // 30 s expressed in µs — histogram ceiling
	histSigFigs      = 3
)

type result struct {
	writeHist *hdrhistogram.Histogram
	readHist  *hdrhistogram.Histogram
	writeOps  int64
	readOps   int64
	errors    int64
}

func main() {
	addr      := flag.String("addr", "localhost:6379", "comma-separated seed node addresses")
	rate      := flag.Int("rate", 5000, "target ops/sec (open-loop)")
	dur       := flag.Duration("duration", 30*time.Second, "test duration")
	workers   := flag.Int("workers", 64, "persistent connections / goroutines")
	readRatio := flag.Float64("read-ratio", 0.5, "fraction of ops that are reads [0,1]")
	numKeys   := flag.Int("keys", 1000, "key-space size")
	valSize   := flag.Int("value-size", 64, "value payload size in bytes")
	token     := flag.String("token", os.Getenv("KV_AUTH_TOKEN"), "auth token")
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
	fmt.Printf("peers  : %v\n", peers)

	// Fixed value payload
	value := make([]byte, *valSize)
	for i := range value {
		value[i] = 'x'
	}

	// Key pool
	keys := make([][]byte, *numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench:k:%06d", i))
	}

	// Work channel: bool token — true = write, false = read
	workCh := make(chan bool, *workers*8)

	results := make([]*result, *workers)
	for i := range results {
		results[i] = &result{
			writeHist: hdrhistogram.New(1, maxLatencyMicros, histSigFigs),
			readHist:  hdrhistogram.New(1, maxLatencyMicros, histSigFigs),
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go runWorker(i, allSeeds, *token, keys, value, workCh, results[i], &wg)
	}

	// Open-loop coordinator: batch tokens so we tick at most 1000 Hz
	batchSize := max(1, *rate/1000)
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

	rr := *readRatio
	start := time.Now()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			for i := 0; i < batchSize; i++ {
				isWrite := rand.Float64() >= rr
				select {
				case workCh <- isWrite:
				default:
					atomic.AddInt64(&droppedTokens, 1)
				}
			}
		}
	}

	close(workCh)
	wg.Wait()
	elapsed := time.Since(start)

	// Merge per-worker results
	totalWrite := hdrhistogram.New(1, maxLatencyMicros, histSigFigs)
	totalRead  := hdrhistogram.New(1, maxLatencyMicros, histSigFigs)
	var totalWriteOps, totalReadOps, totalErrors int64
	for _, r := range results {
		totalWrite.Merge(r.writeHist)
		totalRead.Merge(r.readHist)
		totalWriteOps += atomic.LoadInt64(&r.writeOps)
		totalReadOps  += atomic.LoadInt64(&r.readOps)
		totalErrors   += atomic.LoadInt64(&r.errors)
	}

	totalOps := totalWriteOps + totalReadOps
	actualRate := float64(totalOps) / elapsed.Seconds()

	fmt.Printf("\n=== Load Generator Results ===\n")
	fmt.Printf("%-22s %s\n",   "Target node:", leaderAddr)
	fmt.Printf("%-22s %.1fs\n", "Duration:", elapsed.Seconds())
	fmt.Printf("%-22s %d\n",   "Workers:", *workers)
	fmt.Printf("%-22s %d ops/s\n", "Target rate:", *rate)
	fmt.Printf("%-22s %.0f ops/s  (%d total)\n", "Actual rate:", actualRate, totalOps)
	fmt.Printf("%-22s %d (%.3f%%)\n", "Errors:",
		totalErrors, 100*float64(totalErrors)/float64(max64(totalOps+totalErrors, 1)))
	fmt.Printf("%-22s %d\n", "Dropped tokens:", droppedTokens)
	fmt.Println()
	printHist("Write latency (µs)", totalWrite, totalWriteOps)
	printHist("Read  latency (µs)", totalRead, totalReadOps)
	fmt.Println()
	fmt.Printf("Resume: %.0f writes/s, %.0f reads/s  |  ",
		float64(totalWriteOps)/elapsed.Seconds(), float64(totalReadOps)/elapsed.Seconds())
	fmt.Printf("write p99 = %d µs\n", totalWrite.ValueAtQuantile(99))
}

func runWorker(
	id int,
	seeds []string,
	token string,
	keys [][]byte,
	value []byte,
	workCh <-chan bool,
	r *result,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	var conn *client.Conn
	reconnect := func() {
		if conn != nil {
			conn.Close()
			conn = nil
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
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	rng := rand.New(rand.NewSource(int64(id) * 0x9e3779b9))

	for isWrite := range workCh {
		key := keys[rng.Intn(len(keys))]

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

func printHist(label string, h *hdrhistogram.Histogram, ops int64) {
	if ops == 0 {
		fmt.Printf("%s: no operations recorded\n", label)
		return
	}
	fmt.Printf("%s  [%d ops]\n", label, ops)
	fmt.Printf("  p50=%7d   p95=%7d   p99=%7d   p999=%7d   max=%7d\n",
		h.ValueAtQuantile(50),
		h.ValueAtQuantile(95),
		h.ValueAtQuantile(99),
		h.ValueAtQuantile(99.9),
		h.Max(),
	)
}

func mergeSeeds(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, s := range append(a, b...) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
