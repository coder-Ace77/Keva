// Package client provides a lightweight persistent-connection wrapper for the KV protocol.
package client

import (
	"fmt"
	"key_value_store/protocol"
	clientproto "key_value_store/protocol/client"
	"net"
	"time"
)

// Conn is a single persistent TCP connection to one KV node.
type Conn struct {
	conn net.Conn
}

// Dial connects to addr.
func Dial(addr string, timeout time.Duration) (*Conn, error) {
	raw, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: raw}, nil
}

// Send encodes and writes one message.
func (c *Conn) Send(msg protocol.Message) error {
	return protocol.WriteFrame(c.conn, msg)
}

// Recv reads and decodes one message.
func (c *Conn) Recv() (protocol.Message, error) {
	return protocol.ReadFrame(c.conn, 0)
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
func FetchTopology(addr string) (*clientproto.TopologyResponse, error) {
	c, err := Dial(addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	c.conn.SetDeadline(time.Now().Add(3 * time.Second)) //nolint
	if err := c.Send(&clientproto.TopologyRequest{}); err != nil {
		return nil, err
	}
	msg, err := c.Recv()
	if err != nil {
		return nil, err
	}
	topo, ok := msg.(*clientproto.TopologyResponse)
	if !ok {
		return nil, fmt.Errorf("expected TopologyResponse, got 0x%02X", msg.OpCode())
	}
	return topo, nil
}

// DiscoverLeader tries seeds in order until it finds a topology with a live leader.
func DiscoverLeader(seeds []string) (leaderAddr string, peers []string, err error) {
	for _, seed := range seeds {
		topo, e := FetchTopology(seed)
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
func WaitForLeader(seeds []string, maxWait time.Duration) (string, []string, error) {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		l, peers, err := DiscoverLeader(seeds)
		if err == nil {
			return l, peers, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", nil, fmt.Errorf("no leader found within %s", maxWait)
}
