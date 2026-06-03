// failover: measures the write-unavailability window across leader kill-9 events.
//
// Run this tool while sending a steady stream of writes, then kill -9 the leader
// process externally. The tool timestamps the last successful write before the
// outage and the first successful write after the new leader is elected, and
// reports that delta as the failover window.
//
// Run 30-50 times for meaningful p95 statistics.
//
// Usage:
//
//	go run ./bench/failover -addr localhost:7001,localhost:7002,localhost:7003 -runs 30
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"key_value_store/bench/client"
	"key_value_store/protocol"
)

func main() {
	addr  := flag.String("addr", "localhost:6379", "comma-separated seed addresses")
	token := flag.String("token", os.Getenv("KV_AUTH_TOKEN"), "auth token")
	runs  := flag.Int("runs", 30, "number of failover events to sample (0 = run until Ctrl-C)")
	flag.Parse()

	seeds := strings.Split(*addr, ",")

	fmt.Println("=== Failover Window Measurer ===")
	fmt.Printf("Seeds : %v\n", seeds)
	fmt.Printf("Runs  : %d\n", *runs)
	fmt.Println()
	fmt.Println("How to use:")
	fmt.Println("  1. This tool connects to the leader and writes a probe key every ~10 ms.")
	fmt.Println("  2. Kill -9 the leader process from another terminal.")
	fmt.Println("  3. The tool detects the outage, reconnects to the new leader,")
	fmt.Println("     and prints the write-unavailability window.")
	fmt.Println("  4. Repeat until you have enough samples, then press Ctrl-C.")
	fmt.Println()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	var windows []time.Duration

	for run := 1; *runs == 0 || run <= *runs; run++ {
		select {
		case <-sig:
			goto done
		default:
		}

		window, err := measureOneRun(run, seeds, *token, sig)
		if err != nil {
			// Ctrl-C or unrecoverable
			goto done
		}
		windows = append(windows, window)

		// Brief stabilisation pause before the next run
		fmt.Printf("[run %d] waiting 3s for new leader to stabilise before next run...\n\n", run)
		select {
		case <-sig:
			goto done
		case <-time.After(3 * time.Second):
		}
	}

done:
	if len(windows) > 0 {
		printStats(windows)
	} else {
		fmt.Println("No samples collected.")
	}
}

// measureOneRun blocks until one outage + recovery cycle is observed.
// It returns the write-unavailability window for that cycle.
func measureOneRun(run int, seeds []string, token string, sig <-chan os.Signal) (time.Duration, error) {
	fmt.Printf("[run %d] discovering leader ...\n", run)
	leaderAddr, peers, err := client.WaitForLeader(seeds, token, 15*time.Second)
	if err != nil {
		return 0, fmt.Errorf("no leader: %w", err)
	}
	seeds = mergeSeeds(seeds, peers)
	fmt.Printf("[run %d] leader = %s — now kill -9 it to trigger measurement\n", run, leaderAddr)

	conn, err := client.Dial(leaderAddr, token, 3*time.Second)
	if err != nil {
		return 0, fmt.Errorf("dial: %w", err)
	}

	const probeKey = "bench:failover:probe"
	probeVal := []byte("x")

	var lastSuccess time.Time
	inOutage := false

	for {
		select {
		case <-sig:
			conn.Close()
			return 0, fmt.Errorf("interrupted")
		default:
		}

		now := time.Now()
		resp, err := conn.Do(&protocol.SetPayload{
			Key:   []byte(probeKey),
			Value: probeVal,
		}, 500*time.Millisecond)

		// Treat server-level error responses the same as network errors
		if err == nil {
			if em, ok := resp.(*protocol.ErrorMessage); ok {
				err = fmt.Errorf("server: %s", em.Message)
			}
		}

		if err != nil {
			if !inOutage {
				inOutage = true
				fmt.Printf("[run %d] outage at %s  (last success was %s ago)\n",
					run,
					now.Format("15:04:05.000"),
					now.Sub(lastSuccess).Round(time.Millisecond),
				)
			}
			conn.Close()

			// Reconnect to whatever node is now the leader
			for {
				select {
				case <-sig:
					return 0, fmt.Errorf("interrupted")
				default:
				}
				newLeader, newPeers, e := client.DiscoverLeader(seeds, token)
				if e != nil {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				seeds = mergeSeeds(seeds, newPeers)
				conn, e = client.Dial(newLeader, token, 3*time.Second)
				if e != nil {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				break
			}
			continue
		}

		if inOutage {
			// First successful write after recovery
			window := now.Sub(lastSuccess)
			inOutage = false
			conn.Close()
			fmt.Printf("[run %d] recovered at %s  — window = %s\n",
				run,
				now.Format("15:04:05.000"),
				window.Round(time.Millisecond),
			)
			return window, nil
		}

		lastSuccess = now
		// 10 ms between probes keeps the last-success timestamp fresh
		time.Sleep(10 * time.Millisecond)
	}
}

func printStats(windows []time.Duration) {
	sort.Slice(windows, func(i, j int) bool { return windows[i] < windows[j] })
	n := len(windows)

	var sum time.Duration
	for _, w := range windows {
		sum += w
	}
	mean := sum / time.Duration(n)

	pct := func(p float64) time.Duration {
		idx := int(float64(n-1) * p / 100)
		return windows[idx].Round(time.Millisecond)
	}

	fmt.Printf("\n=== Failover Window Statistics  (%d samples) ===\n", n)
	fmt.Printf("  %-8s %s\n", "min:",    windows[0].Round(time.Millisecond))
	fmt.Printf("  %-8s %s\n", "p50:",    pct(50))
	fmt.Printf("  %-8s %s\n", "mean:",   mean.Round(time.Millisecond))
	fmt.Printf("  %-8s %s\n", "p95:",    pct(95))
	fmt.Printf("  %-8s %s\n", "p99:",    pct(99))
	fmt.Printf("  %-8s %s\n", "max:",    windows[n-1].Round(time.Millisecond))
	fmt.Println()
	fmt.Printf("Resume bullet template:\n")
	fmt.Printf("  Leader failover: median %s, p95 %s  (%d kill-9 trials, 3-node cluster)\n",
		pct(50), pct(95), n)
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
