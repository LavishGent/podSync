package transport

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/LavishGent/podsync/internal/wire"
)

// Default timeouts.
const (
	DefaultDialTimeout    = 2 * time.Second
	DefaultRequestTimeout = time.Second
)

// TCP is the TCP Transport. The zero value is usable; set fields before
// first use. Each frame is written with a fresh gob encoder (wire.WriteFrame)
// because wire's type-definition pinning requires self-contained frames.
type TCP struct {
	// Secret enables the optional HMAC handshake when non-empty. It must be
	// at least MinSecretLen bytes and identical on every node.
	Secret []byte
	// DialTimeout bounds dial plus handshake on both sides. Default 2s.
	DialTimeout time.Duration
	// RequestTimeout bounds one request/response. Default 1s.
	RequestTimeout time.Duration
	// Logf receives handshake diagnostics. Default log.Printf.
	Logf func(format string, args ...any)

	onAccept    func() // test hook
	beforeTrack func() // test hook: between Accept and tracking the conn
	afterSlot   func() // test hook: between slotFor and taking the slot

	mu     sync.Mutex
	slots  map[string]*slot
	closed bool
	addr   string
	conns  map[net.Conn]struct{} // accepted connections
}

// slot is one peer's connection under a one-token semaphore, so waiters can
// give up when their context ends.
type slot struct {
	sem  chan struct{}
	conn net.Conn
}

func (t *TCP) dialTimeout() time.Duration {
	if t.DialTimeout > 0 {
		return t.DialTimeout
	}
	return DefaultDialTimeout
}

func (t *TCP) requestTimeout() time.Duration {
	if t.RequestTimeout > 0 {
		return t.RequestTimeout
	}
	return DefaultRequestTimeout
}

func (t *TCP) logf(format string, args ...any) {
	if t.Logf != nil {
		t.Logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// Addr returns the bound listen address, or "" before Listen has bound.
func (t *TCP) Addr() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addr
}

// Listen accepts connections on addr and serves each in its own goroutine
// until ctx is cancelled, then closes every connection and waits for the
// goroutines to exit.
func (t *TCP) Listen(ctx context.Context, addr string, h Handler) error {
	if err := t.checkSecret(); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.addr = ln.Addr().String()
	t.conns = make(map[net.Conn]struct{})
	t.mu.Unlock()

	var wg sync.WaitGroup
	stopped := make(chan struct{})
	go func() {
		<-ctx.Done()
		ln.Close()
		t.mu.Lock()
		for c := range t.conns {
			c.Close()
		}
		t.mu.Unlock()
		close(stopped)
	}()
	defer func() {
		t.mu.Lock()
		t.addr = ""
		t.mu.Unlock()
	}()

	for {
		c, err := ln.Accept()
		if err != nil {
			<-stopped
			wg.Wait()
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if t.beforeTrack != nil {
			t.beforeTrack()
		}
		// Under t.mu, either ctx is already cancelled (close the conn here) or
		// the cancel goroutine has not walked t.conns yet (it will close it).
		t.mu.Lock()
		if ctx.Err() != nil {
			t.mu.Unlock()
			c.Close()
			continue
		}
		t.conns[c] = struct{}{}
		t.mu.Unlock()
		if t.onAccept != nil {
			t.onAccept()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				c.Close()
				t.mu.Lock()
				delete(t.conns, c)
				t.mu.Unlock()
			}()
			t.serveConn(ctx, c, h)
		}()
	}
}

func (t *TCP) serveConn(ctx context.Context, c net.Conn, h Handler) {
	if err := t.handshake(c, false, t.dialTimeout()); err != nil {
		if errors.Is(err, ErrModeMismatch) {
			t.logf("handshake mode mismatch from %s", c.RemoteAddr())
		} else {
			t.logf("handshake from %s failed: %v", c.RemoteAddr(), err)
		}
		return
	}
	from := c.RemoteAddr().String()
	for {
		mt, payload, err := wire.ReadFrame(c)
		if err != nil {
			return
		}
		rt, resp, err := h(ctx, from, mt, payload)
		if err != nil {
			return
		}
		_ = c.SetWriteDeadline(time.Now().Add(t.requestTimeout()))
		if err := wire.WriteFrame(c, rt, resp); err != nil {
			return
		}
		_ = c.SetWriteDeadline(time.Time{})
	}
}

func (t *TCP) slotFor(addr string) (*slot, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errors.New("transport: closed")
	}
	if t.slots == nil {
		t.slots = make(map[string]*slot)
	}
	s := t.slots[addr]
	if s == nil {
		s = &slot{sem: make(chan struct{}, 1)}
		t.slots[addr] = s
	}
	return s, nil
}

// Send sends one request to addr, dialing lazily, and returns the response
// frame. Any error closes the cached connection so the next Send redials.
// Concurrent Sends to one peer serialize; other peers are unaffected.
func (t *TCP) Send(ctx context.Context, addr string, mt wire.MsgType, msg any) (wire.MsgType, []byte, error) {
	if err := t.checkSecret(); err != nil {
		return 0, nil, err
	}
	s, err := t.slotFor(addr)
	if err != nil {
		return 0, nil, err
	}
	if t.afterSlot != nil {
		t.afterSlot()
	}
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
	defer func() { <-s.sem }()

	// slotFor checked closed before we waited for the slot. Re-check so a Send
	// admitted before Close cannot dial and cache a conn Close will never see.
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return 0, nil, errors.New("transport: closed")
	}

	if s.conn == nil {
		c, err := t.dial(ctx, addr)
		if err != nil {
			return 0, nil, err
		}
		s.conn = c
	}
	rt, payload, err := t.roundTrip(ctx, s.conn, mt, msg)
	if err != nil {
		s.conn.Close()
		s.conn = nil
		return 0, nil, err
	}
	return rt, payload, nil
}

func (t *TCP) dial(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: t.dialTimeout()}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.SetDeadline(time.Unix(1, 0)) })
	err = t.handshake(c, true, t.dialTimeout())
	if !stop() && err == nil {
		err = ctx.Err()
	}
	if err != nil {
		c.Close()
		if errors.Is(err, ErrModeMismatch) {
			t.logf("handshake mode mismatch with %s", addr)
		}
		return nil, fmt.Errorf("transport: handshake with %s: %w", addr, err)
	}
	return c, nil
}

func (t *TCP) roundTrip(ctx context.Context, c net.Conn, mt wire.MsgType, msg any) (wire.MsgType, []byte, error) {
	deadline := time.Now().Add(t.requestTimeout())
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.SetDeadline(deadline); err != nil {
		return 0, nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	if err := wire.WriteFrame(c, mt, msg); err != nil {
		return 0, nil, err
	}
	rt, payload, err := wire.ReadFrame(c)
	if err != nil {
		return 0, nil, err
	}
	if !stop() {
		return 0, nil, ctx.Err()
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		return 0, nil, err
	}
	return rt, payload, nil
}

// Close closes all cached peer connections. It does not stop Listen; cancel
// its context for that.
func (t *TCP) Close() error {
	t.mu.Lock()
	t.closed = true
	slots := t.slots
	t.mu.Unlock()
	for _, s := range slots {
		s.sem <- struct{}{}
		if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
		<-s.sem
	}
	return nil
}
