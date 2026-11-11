package transport

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LavishGent/podsync/internal/wire"
)

func mac(secret []byte, label string, a, b []byte) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(label))
	m.Write(a)
	m.Write(b)
	return m.Sum(nil)
}

func countingHandler(n *atomic.Int32) Handler {
	return func(ctx context.Context, from string, mt wire.MsgType, p []byte) (wire.MsgType, any, error) {
		n.Add(1)
		return pongHandler(ctx, from, mt, p)
	}
}

func TestHandshakeWrongSecretRejectedHandlerNeverRuns(t *testing.T) {
	var calls atomic.Int32
	addr, _ := serve(t, newTCP(testSecret), "127.0.0.1:0", countingHandler(&calls))
	c := newTCP([]byte("fedcba9876543210fedcba9876543210"))
	defer c.Close()
	_, err := ping(c, addr)
	if err == nil {
		t.Fatal("wrong secret accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("handler ran for unauthenticated peer")
	}
}

// fakeServer runs fn on the first accepted connection.
func fakeServer(t *testing.T, fn func(net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		fn(c)
	}()
	t.Cleanup(func() { ln.Close(); <-done })
	return ln.Addr().String()
}

func TestHandshakeBadServerMACIsAuthErrorAndNoFrameSent(t *testing.T) {
	var extra atomic.Int32
	addr := fakeServer(t, func(c net.Conn) {
		_, _ = c.Write([]byte{'P', 'S', 'Y', '1', 1})
		hello := make([]byte, 5)
		_, _ = io.ReadFull(c, hello)
		ns := make([]byte, 32)
		_, _ = rand.Read(ns)
		_, _ = c.Write(ns)
		reply := make([]byte, 64) // Nc || MAC; deliberately not verified
		_, _ = io.ReadFull(c, reply)
		_, _ = c.Write(make([]byte, 32)) // bad server MAC
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := io.Copy(io.Discard, c) // dialer must close without sending a frame
		extra.Store(int32(n))
	})
	c := newTCP(testSecret)
	defer c.Close()
	_, err := ping(c, addr)
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("err = %v, want ErrAuth", err)
	}
	c.Close()
	time.Sleep(50 * time.Millisecond)
	if extra.Load() != 0 {
		t.Fatalf("dialer sent %d bytes after failed auth", extra.Load())
	}
}

func TestHandshakeIdleConnectionSurvivesPastDialTimeout(t *testing.T) {
	srv := newTCP(testSecret)
	srv.DialTimeout = 100 * time.Millisecond
	var accepts atomic.Int32
	srv.onAccept = func() { accepts.Add(1) }
	addr, _ := serve(t, srv, "127.0.0.1:0", pongHandler)
	c := newTCP(testSecret)
	c.DialTimeout = 100 * time.Millisecond
	defer c.Close()
	if _, err := ping(c, addr); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := ping(c, addr); err != nil {
		t.Fatalf("send on idle connection: %v", err)
	}
	if n := accepts.Load(); n != 1 {
		t.Fatalf("accepts = %d, want 1", n)
	}
}

func TestHandshakeConcurrentFirstSendsOpenOneConnection(t *testing.T) {
	srv := newTCP(testSecret)
	var accepts atomic.Int32
	srv.onAccept = func() { accepts.Add(1) }
	addr, _ := serve(t, srv, "127.0.0.1:0", pongHandler)
	c := newTCP(testSecret)
	defer c.Close()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ping(c, addr); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := accepts.Load(); n != 1 {
		t.Fatalf("accepts = %d, want 1", n)
	}
}

func TestHandshakeModeMismatchFailsFast(t *testing.T) {
	cases := map[string]struct{ server, client []byte }{
		"server secret, client none": {testSecret, nil},
		"server none, client secret": {nil, testSecret},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var logged atomic.Value
			srv := newTCP(tc.server)
			srv.Logf = func(f string, a ...any) { logged.Store(f) }
			var calls atomic.Int32
			addr, _ := serve(t, srv, "127.0.0.1:0", countingHandler(&calls))
			c := newTCP(tc.client)
			c.DialTimeout, c.RequestTimeout = 5*time.Second, 5*time.Second
			defer c.Close()
			start := time.Now()
			_, err := ping(c, addr)
			if !errors.Is(err, ErrModeMismatch) || !strings.Contains(err.Error(), "handshake mode mismatch") {
				t.Fatalf("err = %v", err)
			}
			if time.Since(start) > time.Second {
				t.Fatal("mismatch did not fail fast")
			}
			if calls.Load() != 0 {
				t.Fatal("handler ran")
			}
			waitFor(t, func() bool { return logged.Load() != nil })
			if !strings.Contains(logged.Load().(string), "handshake mode mismatch") {
				t.Fatalf("log = %q", logged.Load())
			}
		})
	}
}

func TestHandshakeSilentDialerClosedAtListenerDeadline(t *testing.T) {
	srv := newTCP(testSecret)
	srv.DialTimeout = 100 * time.Millisecond
	addr, stop := serve(t, srv, "127.0.0.1:0", pongHandler)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	start := time.Now()
	_, err = io.Copy(io.Discard, conn) // listener's hello arrives, then it must close
	if err != nil {
		t.Fatalf("expected clean close by listener, got %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("closed after %v", d)
	}
	stop() // Listen waits for every connection goroutine; hangs if one leaked
}

func TestHandshakeReplayedClientResponseRejected(t *testing.T) {
	var calls atomic.Int32
	addr, _ := serve(t, newTCP(testSecret), "127.0.0.1:0", countingHandler(&calls))

	handshake := func(replay []byte) (nc, resp []byte, ok bool) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = conn.Write([]byte{'P', 'S', 'Y', '1', 1})
		hello := make([]byte, 5)
		if _, err := io.ReadFull(conn, hello); err != nil {
			t.Fatal(err)
		}
		ns := make([]byte, 32)
		if _, err := io.ReadFull(conn, ns); err != nil {
			t.Fatal(err)
		}
		if replay != nil {
			_, _ = conn.Write(replay)
		} else {
			nc = make([]byte, 32)
			_, _ = rand.Read(nc)
			replay = append(append([]byte{}, nc...), mac(testSecret, "podsync/v1 client", ns, nc)...)
			_, _ = conn.Write(replay)
		}
		server := make([]byte, 32)
		_, err = io.ReadFull(conn, server)
		return nc, replay, err == nil
	}
	_, captured, ok := handshake(nil)
	if !ok {
		t.Fatal("legitimate handshake failed")
	}
	if _, _, ok := handshake(captured); ok {
		t.Fatal("replayed Nc+MAC accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("handler ran")
	}
}
