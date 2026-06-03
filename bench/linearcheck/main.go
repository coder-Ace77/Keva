// linearcheck: verifies that the KV cluster's operation history is linearizable.
//
// Runs N goroutines doing mixed GET/SET on a small key space, records every
// operation's exact call/return timestamps, then feeds the history to Porcupine
// for per-key linearizability checking.
//
// All operations are routed to the leader so that reads see committed state.
// (Follower reads are eventually consistent — not linearizable — by design.)
//
// For partition testing: run this while injecting a network partition with
// Toxiproxy or tc/iptables (see ROADMAP.md), then observe whether the checker
// reports a violation.
//
// Usage:
//
//	go run ./bench/linearcheck -addr localhost:7001,localhost:7002,localhost:7003 \
//	    -workers 10 -keys 5 -duration 20s
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"key_value_store/bench/client"
	"key_value_store/protocol"
)

// opInput is the input side of one KV operation recorded for Porcupine.
type opInput struct {
	op    string // "get" or "set"
	key   string
	value string // only meaningful for "set"
}

// opOutput is the observed output of one KV operation.
type opOutput struct {
	value string // value read (for "get")
	ok    bool   // whether the op succeeded
}

type opRecord struct {
	clientID int
	input    opInput
	output   opOutput
	call     int64 // time.Now().UnixNano() when the op was invoked
	ret      int64 // time.Now().UnixNano() when the response was received
}

// registerModel returns a Porcupine model for a single-key register.
// State = the last successfully SET value (empty string = never set / deleted).
func registerModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} { return "" },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			s := state.(string)
			in := input.(opInput)
			out := output.(opOutput)
			switch in.op {
			case "set":
				if !out.ok {
					// Failed write — state unchanged, no constraint on next read
					return true, s
				}
				return true, in.value
			case "get":
				if !out.ok {
					// Read that returned an error is compatible with any state
					return true, s
				}
				return out.value == s, s
			}
			return false, s
		},
		DescribeOperation: func(input, output interface{}) string {
			in := input.(opInput)
			out := output.(opOutput)
			if in.op == "set" {
				return fmt.Sprintf("set(%q) ok=%v", in.value, out.ok)
			}
			return fmt.Sprintf("get() -> %q ok=%v", out.value, out.ok)
		},
		DescribeState: func(state interface{}) string {
			return fmt.Sprintf("%q", state.(string))
		},
	}
}

func main() {
	addr     := flag.String("addr", "localhost:6379", "comma-separated seed node addresses")
	token    := flag.String("token", os.Getenv("KV_AUTH_TOKEN"), "auth token")
	workers  := flag.Int("workers", 10, "concurrent client goroutines")
	numKeys  := flag.Int("keys", 5, "number of distinct keys to exercise")
	duration := flag.Duration("duration", 20*time.Second, "workload duration")
	flag.Parse()

	seeds := strings.Split(*addr, ",")

	fmt.Printf("discovering leader via %v ...\n", seeds)
	leaderAddr, _, err := client.WaitForLeader(seeds, *token, 10*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("leader  : %s\n", leaderAddr)
	fmt.Printf("workers : %d   keys : %d   duration : %s\n\n", *workers, *numKeys, *duration)

	keys := make([]string, *numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("lc:%d", i)
	}

	var mu sync.Mutex
	var history []opRecord

	var wg sync.WaitGroup
	deadline := time.Now().Add(*duration)

	for id := 0; id < *workers; id++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			conn, err := client.Dial(leaderAddr, *token, 3*time.Second)
			if err != nil {
				fmt.Fprintf(os.Stderr, "worker %d: dial: %v\n", clientID, err)
				return
			}
			defer conn.Close()

			rng := rand.New(rand.NewSource(int64(clientID)*0x9e3779b9 + 1))

			for time.Now().Before(deadline) {
				key := keys[rng.Intn(*numKeys)]

				var in opInput
				var out opOutput

				t0 := time.Now().UnixNano()

				if rng.Intn(2) == 0 {
					// SET
					val := fmt.Sprintf("%d:%d", clientID, rng.Int63n(1_000_000))
					in = opInput{op: "set", key: key, value: val}

					resp, err := conn.Do(&protocol.SetPayload{
						Key:   []byte(key),
						Value: []byte(val),
					}, 5*time.Second)

					if err == nil {
						if sr, ok := resp.(*protocol.SetResponse); ok {
							out = opOutput{ok: sr.Success}
						}
					}
				} else {
					// GET
					in = opInput{op: "get", key: key}

					resp, err := conn.Do(&protocol.GetPayload{Key: []byte(key)}, 5*time.Second)

					if err == nil {
						if gr, ok := resp.(*protocol.GetResponse); ok {
							if gr.Found {
								out = opOutput{value: string(gr.Value), ok: true}
							} else {
								// Key not yet set — treat as empty string (initial state)
								out = opOutput{value: "", ok: true}
							}
						}
					}
				}

				t1 := time.Now().UnixNano()

				mu.Lock()
				history = append(history, opRecord{
					clientID: clientID,
					input:    in,
					output:   out,
					call:     t0,
					ret:      t1,
				})
				mu.Unlock()
			}
		}(id)
	}

	wg.Wait()
	fmt.Printf("collected %d operations — checking linearizability per key...\n\n", len(history))

	model := registerModel()
	allLinear := true

	for _, key := range keys {
		var ops []porcupine.Operation
		for _, rec := range history {
			if rec.input.key != key {
				continue
			}
			ops = append(ops, porcupine.Operation{
				ClientId: rec.clientID,
				Input:    rec.input,
				Call:     rec.call,
				Output:   rec.output,
				Return:   rec.ret,
			})
		}
		if len(ops) == 0 {
			fmt.Printf("  key %q : no operations\n", key)
			continue
		}

		ok := porcupine.CheckOperations(model, ops)
		mark := "✓"
		label := "LINEARIZABLE"
		if !ok {
			mark = "✗"
			label = "NOT LINEARIZABLE"
			allLinear = false
		}
		fmt.Printf("  key %-12q : %s %s  (%d ops)\n", key, mark, label, len(ops))
	}

	fmt.Println()
	if allLinear {
		fmt.Println("RESULT  : All keys linearizable ✓")
		fmt.Println()
		fmt.Println("Resume bullet:")
		fmt.Println("  Verified linearizability under concurrent load with Porcupine")
		fmt.Println("  (all leader-routed reads and writes across", *workers, "clients)")
	} else {
		fmt.Println("RESULT  : Linearizability violation detected ✗")
		fmt.Println("Review the violating key's history for ordering anomalies.")
	}
}
