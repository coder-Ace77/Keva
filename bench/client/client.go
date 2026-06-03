// Package client provides a lightweight persistent-connection wrapper for the KV protocol.
package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"key_value_store/protocol"
	"net"
	"time"
)

// Conn is a single persistent authenticated TCP connection to one KV node.
type Conn struct {
	conn net.Conn
}

// Dial connects to addr and authenticates with token when non-empty.
func Dial(addr, authToken string, timeout time.Duration) (*Conn, error) {
	raw, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	c := &Conn{conn: raw}
	if authToken != "" {
		if err := c.doAuth(authToken); err != nil {
			raw.Close()
			return nil, fmt.Errorf("auth: %w", err)
		}
	}
	return c, nil
}

func (c *Conn) doAuth(token string) error {
	if err := c.Send(&protocol.AuthMessage{Token: []byte(token)}); err != nil {
		return err
	}
	msg, err := c.Recv()
	if err != nil {
		return err
	}
	resp, ok := msg.(*protocol.AuthResponse)
	if !ok {
		return fmt.Errorf("unexpected response 0x%02X during auth", msg.OpCode())
	}
	if !resp.Success {
		return fmt.Errorf("invalid token")
	}
	return nil
}

// Send encodes and writes one message.
func (c *Conn) Send(msg protocol.Message) error {
	wire, err := protocol.EncodeMessage(msg)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(wire)))
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err = c.conn.Write(wire)
	return err
}

// Recv reads and decodes one message.
func (c *Conn) Recv() (protocol.Message, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header)
	frame := make([]byte, n)
	if _, err := io.ReadFull(c.conn, frame); err != nil {
		return nil, err
	}
	return protocol.DecodeMessage(frame)
}

// Do sends req and reads the response within opTimeout. A zero opTimeout
// defaults to 5 seconds.
func (c *Conn) Do(req protocol.Message, opTimeout time.Duration) (protocol.Message, error) {
	d := opTimeout
	if d <= 0 {
		d = 5 * time.Second
	}
	c.conn.SetDeadline(time.Now().Add(d)) //nolint
	if err := c.Send(req); err != nil {
		return nil, err
	}
	return c.Recv()
}

// SetDeadline sets the underlying connection deadline.
func (c *Conn) SetDeadline(t time.Time) { c.conn.SetDeadline(t) } //nolint

// Close closes the underlying TCP connection.
func (c *Conn) Close() { c.conn.Close() }

// FetchTopology opens a short-lived connection to addr and returns the topology.
func FetchTopology(addr, authToken string) (*protocol.TopologyResponse, error) {
	c, err := Dial(addr, authToken, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	c.conn.SetDeadline(time.Now().Add(3 * time.Second)) //nolint
	if err := c.Send(&protocol.TopologyRequest{}); err != nil {
		return nil, err
	}
	msg, err := c.Recv()
	if err != nil {
		return nil, err
	}
	topo, ok := msg.(*protocol.TopologyResponse)
	if !ok {
		return nil, fmt.Errorf("expected TopologyResponse, got 0x%02X", msg.OpCode())
	}
	return topo, nil
}

// DiscoverLeader tries seeds in order until it finds a topology with a live leader.
func DiscoverLeader(seeds []string, authToken string) (leaderAddr string, peers []string, err error) {
	for _, seed := range seeds {
		topo, e := FetchTopology(seed, authToken)
		if e != nil || topo.LeaderAddr == "" {
			continue
		}
		for _, n := range topo.Nodes {
			peers = append(peers, n.Address)
		}
		return topo.LeaderAddr, peers, nil
	}
	return "", nil, fmt.Errorf("no seed returned a topology with a leader")
}

// WaitForLeader retries DiscoverLeader until maxWait elapses.
func WaitForLeader(seeds []string, authToken string, maxWait time.Duration) (string, []string, error) {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		l, peers, err := DiscoverLeader(seeds, authToken)
		if err == nil {
			return l, peers, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", nil, fmt.Errorf("no leader found within %s", maxWait)
}
