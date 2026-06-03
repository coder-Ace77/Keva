package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"key_value_store/protocol"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

const defaultAddr = "localhost:6379"

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorCyan   = "\033[36m"
	ColorBold   = "\033[1m"
)

// kvClient holds cluster topology and routes each command to the right node.
type kvClient struct {
	knownAddrs []string // seed + topology-discovered addresses
	leaderAddr string
	followers  []string
}

func newClient(seed string) *kvClient {
	return &kvClient{knownAddrs: []string{seed}}
}

// connect fetches topology from any reachable known node and updates routing state.
func (c *kvClient) connect() error {
	for _, addr := range c.knownAddrs {
		topo, err := c.fetchTopology(addr)
		if err != nil {
			continue
		}
		c.leaderAddr = topo.LeaderAddr
		c.followers = nil
		for _, n := range topo.Nodes {
			c.addKnown(n.Address)
			if n.Address != topo.LeaderAddr {
				c.followers = append(c.followers, n.Address)
			}
		}
		if c.leaderAddr != "" {
			return nil
		}
	}
	return fmt.Errorf("no reachable node returned a topology with a leader")
}

func (c *kvClient) addKnown(addr string) {
	for _, a := range c.knownAddrs {
		if a == addr {
			return
		}
	}
	c.knownAddrs = append(c.knownAddrs, addr)
}

// execute routes the command to the right node and retries on leader failure.
func (c *kvClient) execute(msg protocol.Message) (protocol.Message, error) {
	isWrite := msg.OpCode() == protocol.OpSet || msg.OpCode() == protocol.OpDelete

	for attempt := 0; attempt < 8; attempt++ {
		target := c.pickTarget(isWrite)
		if target == "" {
			fmt.Printf("%s(discovering leader...)%s\n", ColorYellow, ColorReset)
			if err := c.waitForLeader(); err != nil {
				return nil, err
			}
			continue
		}

		resp, err := c.sendToNode(target, msg)
		if err == nil {
			return resp, nil
		}

		fmt.Printf("%s(node %s unreachable, re-discovering cluster...)%s\n",
			ColorYellow, target, ColorReset)
		c.leaderAddr = ""
		time.Sleep(300 * time.Millisecond)
		_ = c.waitForLeader()
	}
	return nil, fmt.Errorf("cluster unavailable after retries")
}

func (c *kvClient) pickTarget(isWrite bool) string {
	if isWrite {
		return c.leaderAddr
	}
	if len(c.followers) > 0 {
		return c.followers[rand.Intn(len(c.followers))]
	}
	return c.leaderAddr
}

// waitForLeader retries topology discovery until a leader is found or all nodes are down.
func (c *kvClient) waitForLeader() error {
	for i := 0; i < 15; i++ {
		if err := c.connect(); err == nil && c.leaderAddr != "" {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("no leader elected after 4.5s — cluster may be down")
}

func (c *kvClient) fetchTopology(addr string) (*protocol.TopologyResponse, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second)) //nolint

	if err := sendMessage(conn, &protocol.TopologyRequest{}); err != nil {
		return nil, err
	}
	msg, err := receiveMessage(conn)
	if err != nil {
		return nil, err
	}
	topo, ok := msg.(*protocol.TopologyResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type 0x%02X", msg.OpCode())
	}
	return topo, nil
}

func (c *kvClient) sendToNode(addr string, msg protocol.Message) (protocol.Message, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second)) //nolint

	if err := sendMessage(conn, msg); err != nil {
		return nil, err
	}
	return receiveMessage(conn)
}

// ── Wire helpers ──────────────────────────────────────────────────────────────

func sendMessage(conn net.Conn, msg protocol.Message) error {
	wire, err := protocol.EncodeMessage(msg)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(wire)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err = conn.Write(wire)
	return err
}

func receiveMessage(conn net.Conn) (protocol.Message, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(header)
	frame := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return nil, err
	}
	return protocol.DecodeMessage(frame)
}

// ── Main REPL ─────────────────────────────────────────────────────────────────

func main() {
	url := flag.String("url", "", "any cluster node to connect to (e.g. localhost:7001)")
	flag.StringVar(url, "u", "", "any cluster node to connect to (shorthand)")
	flag.Parse()

	seed := defaultAddr
	if *url != "" {
		seed = *url
	} else if v := os.Getenv("KV_ADDR"); v != "" {
		seed = v
	}

	fmt.Printf("%s%sConnecting to cluster via %s...%s\n", ColorCyan, ColorBold, seed, ColorReset)

	client := newClient(seed)
	if err := client.waitForLeader(); err != nil {
		fmt.Printf("%sFailed to discover cluster: %v%s\n", ColorRed, err, ColorReset)
		os.Exit(1)
	}

	fmt.Printf("%sCluster ready.%s  Leader: %s%s%s  Followers: %v\n\n",
		ColorGreen, ColorReset,
		ColorBold, client.leaderAddr, ColorReset,
		client.followers)

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%skv-db: %s", ColorCyan, ColorReset)

		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("\n%sError reading input: %v%s\n", ColorRed, err, ColorReset)
			break
		}

		command := strings.TrimSpace(input)
		if command == "" {
			continue
		}
		if strings.ToLower(command) == "exit" || strings.ToLower(command) == "quit" {
			fmt.Println("Goodbye!")
			break
		}
		if strings.ToLower(command) == "topology" {
			printTopology(client)
			continue
		}

		msg, err := parseCommand(command)
		if err != nil {
			fmt.Printf("%s%s%s\n", ColorRed, err.Error(), ColorReset)
			continue
		}

		resp, err := client.execute(msg)
		if err != nil {
			fmt.Printf("%s%s%s\n", ColorRed, err.Error(), ColorReset)
			continue
		}
		printResponse(resp)
	}
}

func parseCommand(input string) (protocol.Message, error) {
	parts := strings.Fields(input)

	switch strings.ToUpper(parts[0]) {
	case "GET":
		if len(parts) != 2 {
			return nil, fmt.Errorf("usage: GET <key>")
		}
		return &protocol.GetPayload{Key: []byte(parts[1])}, nil

	case "SET":
		if len(parts) < 3 {
			return nil, fmt.Errorf("usage: SET <key> <value>")
		}
		return &protocol.SetPayload{
			Key:   []byte(parts[1]),
			Value: []byte(strings.Join(parts[2:], " ")),
		}, nil

	case "DEL":
		if len(parts) != 2 {
			return nil, fmt.Errorf("usage: DEL <key>")
		}
		return &protocol.DeletePayload{Key: []byte(parts[1])}, nil

	default:
		return nil, fmt.Errorf("unknown command %q — available: GET, SET, DEL, topology", parts[0])
	}
}

func printTopology(c *kvClient) {
	fmt.Printf("  Leader:    %s%s%s\n", ColorBold, c.leaderAddr, ColorReset)
	for i, f := range c.followers {
		fmt.Printf("  Follower%d: %s\n", i+1, f)
	}
}

func printResponse(msg protocol.Message) {
	switch r := msg.(type) {
	case *protocol.GetResponse:
		if r.Found {
			fmt.Printf("%s%s\"%s\"%s\n", ColorCyan, ColorBold, string(r.Value), ColorReset)
		} else {
			fmt.Printf("%s(nil)%s\n", ColorYellow, ColorReset)
		}

	case *protocol.SetResponse:
		if r.Success {
			fmt.Printf("%s%s%s\n", ColorGreen, r.Message, ColorReset)
		} else {
			fmt.Printf("%s%s%s%s\n", ColorRed, ColorBold, r.Message, ColorReset)
		}

	case *protocol.DeleteResponse:
		if r.Success {
			fmt.Printf("%s%s%s\n", ColorGreen, r.Message, ColorReset)
		} else {
			fmt.Printf("%s%s%s\n", ColorYellow, r.Message, ColorReset)
		}

	case *protocol.ErrorMessage:
		fmt.Printf("%s%sERR [%d]: %s%s\n", ColorRed, ColorBold, r.Code, r.Message, ColorReset)

	default:
		fmt.Printf("%s(unexpected opcode 0x%02X)%s\n", ColorRed, msg.OpCode(), ColorReset)
	}
}

func printError(msg string, err error) {
	fmt.Printf("\n%s%s: %v%s\n", ColorRed, msg, err, ColorReset)
}
