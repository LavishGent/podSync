package transport

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/LavishGent/podsync/internal/wire"
)

// Errors injected by the in-memory transport.
var (
	ErrNoListener  = errors.New("transport: no listener at address")
	ErrPartitioned = errors.New("transport: network partition")
	ErrDropped     = errors.New("transport: message dropped by filter")
	ErrConnDropped = errors.New("transport: connection dropped")
)

// Network is a process-local registry of Memory endpoints with fault
// injection, for tests of T4/T5 and above. Messages still go through the
// wire codec, so handlers see real frames.
type Network struct {
	mu        sync.Mutex
	handlers  map[string]Handler
	partition map[[2]string]bool
	drop      func(from, to string, t wire.MsgType) bool
	delay     map[string]time.Duration
	failNext  map[string]int
}

// NewNetwork returns an empty Network.
func NewNetwork() *Network {
	return &Network{
		handlers:  map[string]Handler{},
		partition: map[[2]string]bool{},
		delay:     map[string]time.Duration{},
		failNext:  map[string]int{},
	}
}

func pair(a, b string) [2]string {
	if b < a {
		a, b = b, a
	}
	return [2]string{a, b}
}

// Partition blocks traffic between a and b in both directions.
func (n *Network) Partition(a, b string) {
	n.mu.Lock()
	n.partition[pair(a, b)] = true
	n.mu.Unlock()
}

// Heal undoes Partition(a, b).
func (n *Network) Heal(a, b string) {
	n.mu.Lock()
	delete(n.partition, pair(a, b))
	n.mu.Unlock()
}

// Drop installs a filter; a Send for which fn returns true fails with
// ErrDropped before reaching the handler. Drop(nil) removes the filter.
func (n *Network) Drop(fn func(from, to string, t wire.MsgType) bool) {
	n.mu.Lock()
	n.drop = fn
	n.mu.Unlock()
}

// SetDelay makes every Send to addr wait d (or until its context ends) before
// delivery, simulating a slow peer. Zero removes the delay.
func (n *Network) SetDelay(addr string, d time.Duration) {
	n.mu.Lock()
	if d <= 0 {
		delete(n.delay, addr)
	} else {
		n.delay[addr] = d
	}
	n.mu.Unlock()
}

// FailNext makes the next count Sends to addr fail with ErrConnDropped
// without reaching the handler, simulating dropped connections.
func (n *Network) FailNext(addr string, count int) {
	n.mu.Lock()
	n.failNext[addr] = count
	n.mu.Unlock()
}

func (n *Network) registered(addr string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.handlers[addr]
	return ok
}

// Endpoint returns the Transport for the node at addr. Its Send calls carry
// addr as the sender for partitions, filters, and the handler's from.
func (n *Network) Endpoint(addr string) *Memory { return &Memory{net: n, self: addr} }

// Memory is an in-memory Transport endpoint.
type Memory struct {
	net  *Network
	self string
}

// Listen registers h at addr and blocks until ctx is cancelled.
func (m *Memory) Listen(ctx context.Context, addr string, h Handler) error {
	m.net.mu.Lock()
	m.net.handlers[addr] = h
	m.net.mu.Unlock()
	<-ctx.Done()
	m.net.mu.Lock()
	delete(m.net.handlers, addr)
	m.net.mu.Unlock()
	return nil
}

// Send delivers msg to the handler at addr after applying injected faults.
func (m *Memory) Send(ctx context.Context, addr string, t wire.MsgType, msg any) (wire.MsgType, []byte, error) {
	n := m.net
	n.mu.Lock()
	h, ok := n.handlers[addr]
	part := n.partition[pair(m.self, addr)]
	drop := n.drop
	delay := n.delay[addr]
	failing := n.failNext[addr] > 0
	if failing {
		n.failNext[addr]--
	}
	n.mu.Unlock()

	switch {
	case !ok:
		return 0, nil, ErrNoListener
	case part:
		return 0, nil, ErrPartitioned
	case drop != nil && drop(m.self, addr, t):
		return 0, nil, ErrDropped
	case failing:
		return 0, nil, ErrConnDropped
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}

	payload, err := roundTripFrame(t, msg)
	if err != nil {
		return 0, nil, err
	}
	rt, resp, err := h(ctx, m.self, t, payload)
	if err != nil {
		return 0, nil, err
	}
	out, err := roundTripFrame(rt, resp)
	if err != nil {
		return 0, nil, err
	}
	return rt, out, nil
}

// roundTripFrame runs msg through the real framing and returns the payload.
func roundTripFrame(t wire.MsgType, msg any) ([]byte, error) {
	var b bytes.Buffer
	if err := wire.WriteFrame(&b, t, msg); err != nil {
		return nil, err
	}
	_, payload, err := wire.ReadFrame(&b)
	return payload, err
}

// Close is a no-op.
func (m *Memory) Close() error { return nil }

var (
	_ Transport = (*TCP)(nil)
	_ Transport = (*Memory)(nil)
)
