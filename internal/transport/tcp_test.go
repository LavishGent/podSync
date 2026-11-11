package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LavishGent/podsync/internal/wire"
)

var testSecret = []byte("0123456789abcdef0123456789abcdef")

func newTCP(secret []byte) *TCP {
	return &TCP{
		Secret:         secret,
		DialTimeout:    time.Second,
		RequestTimeout: time.Second,
		Logf:           func(string, ...any) {},
	}
}

// serve starts srv on addr and returns its bound address and a stop func
// that waits for Listen to return.
func serve(t *testing.T, srv *TCP, addr string, h Handler) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Listen(ctx, addr, h) }()
	waitFor(t, func() bool { return srv.Addr() != "" })
	bound := srv.Addr()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("Listen did not return after cancel")
			}
		})
	}
	t.Cleanup(stop)
	return bound, stop
}

func ping(c *TCP, addr string) (wire.Pong, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mt, p, err := c.Send(ctx, addr, wire.MsgPing, wire.Ping{Origin: "c"})
	if err != nil {
		return wire.Pong{}, err
	}
	m, err := wire.DecodeMessage(mt, p)
	if err != nil {
		return wire.Pong{}, err
	}
	return m.(wire.Pong), nil
}

func TestTCPPingPong(t *testing.T) {
	for name, secret := range map[string][]byte{"plain": nil, "secret": testSecret} {
		t.Run(name, func(t *testing.T) {
			addr, _ := serve(t, newTCP(secret), "127.0.0.1:0", pongHandler)
			c := newTCP(secret)
			defer c.Close()
			got, err := ping(c, addr)
			if err != nil || got.Clock != 7 {
				t.Fatalf("got %+v, %v", got, err)
			}
		})
	}
}

func TestTCPStateRequestRoundtrip(t *testing.T) {
	h := func(_ context.Context, _ string, mt wire.MsgType, p []byte) (wire.MsgType, any, error) {
		m, err := wire.DecodeMessage(mt, p)
		if err != nil {
			return 0, nil, err
		}
		req := m.(wire.StateRequest)
		resp, err := wire.NewStateResponse("srv", req.Shard, req.NumShards, nil)
		return wire.MsgStateResponse, resp, err
	}
	addr, _ := serve(t, newTCP(nil), "127.0.0.1:0", h)
	c := newTCP(nil)
	defer c.Close()
	req, err := wire.NewStateRequest("c", 3, 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	mt, p, err := c.Send(context.Background(), addr, wire.MsgStateRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	m, err := wire.DecodeMessage(mt, p)
	if err != nil || m.(wire.StateResponse).Shard != 3 {
		t.Fatalf("got %+v, %v", m, err)
	}
}

func TestTCPReusesConnection(t *testing.T) {
	srv := newTCP(nil)
	var accepts atomic.Int32
	srv.onAccept = func() { accepts.Add(1) }
	addr, _ := serve(t, srv, "127.0.0.1:0", pongHandler)
	c := newTCP(nil)
	defer c.Close()
	for i := 0; i < 3; i++ {
		if _, err := ping(c, addr); err != nil {
			t.Fatal(err)
		}
	}
	if n := accepts.Load(); n != 1 {
		t.Fatalf("accepts = %d, want 1", n)
	}
}

func TestTCPRedialsAfterListenerRestart(t *testing.T) {
	addr, stop := serve(t, newTCP(nil), "127.0.0.1:0", pongHandler)
	c := newTCP(nil)
	c.RequestTimeout = 500 * time.Millisecond
	defer c.Close()
	if _, err := ping(c, addr); err != nil {
		t.Fatal(err)
	}
	stop()
	start := time.Now()
	if _, err := ping(c, addr); err == nil {
		t.Fatal("send to stopped listener succeeded")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("failure took %v", d)
	}
	serve(t, newTCP(nil), addr, pongHandler)
	if _, err := ping(c, addr); err != nil {
		t.Fatalf("send after restart: %v", err)
	}
}

func TestTCPHandlerErrorClosesConnection(t *testing.T) {
	srv := newTCP(nil)
	var accepts atomic.Int32
	srv.onAccept = func() { accepts.Add(1) }
	var fail atomic.Bool
	fail.Store(true)
	h := func(ctx context.Context, from string, mt wire.MsgType, p []byte) (wire.MsgType, any, error) {
		if fail.Load() {
			return 0, nil, errors.New("boom")
		}
		return pongHandler(ctx, from, mt, p)
	}
	addr, _ := serve(t, srv, "127.0.0.1:0", h)
	c := newTCP(nil)
	defer c.Close()
	if _, err := ping(c, addr); err == nil {
		t.Fatal("want error from failing handler")
	}
	fail.Store(false)
	if _, err := ping(c, addr); err != nil {
		t.Fatal(err)
	}
	if n := accepts.Load(); n != 2 {
		t.Fatalf("accepts = %d, want 2 (conn closed after handler error)", n)
	}
}

func TestTCPConcurrentSends(t *testing.T) {
	addr, _ := serve(t, newTCP(testSecret), "127.0.0.1:0", pongHandler)
	c := newTCP(testSecret)
	defer c.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ping(c, addr); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestTCPSlowPeerDoesNotBlockOthers(t *testing.T) {
	// A black-hole listener accepts but never answers.
	hole, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer hole.Close()
	go func() {
		for {
			c, err := hole.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	good, _ := serve(t, newTCP(nil), "127.0.0.1:0", pongHandler)
	c := newTCP(nil)
	c.DialTimeout = 2 * time.Second
	defer c.Close()
	go func() { _, _ = ping(c, hole.Addr().String()) }()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if _, err := ping(c, good); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("send to healthy peer waited on the unreachable peer")
	}
}

func TestTCPSecretTooShortRejected(t *testing.T) {
	short := newTCP([]byte("short"))
	if err := short.Listen(context.Background(), "127.0.0.1:0", pongHandler); !errors.Is(err, ErrSecretTooShort) {
		t.Fatalf("Listen err = %v", err)
	}
	if _, _, err := short.Send(context.Background(), "127.0.0.1:1", wire.MsgPing, wire.Ping{}); !errors.Is(err, ErrSecretTooShort) {
		t.Fatalf("Send err = %v", err)
	}
}
