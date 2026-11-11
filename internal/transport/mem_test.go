package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LavishGent/podsync/internal/wire"
)

func pongHandler(_ context.Context, from string, t wire.MsgType, _ []byte) (wire.MsgType, any, error) {
	return wire.MsgPong, wire.Pong{Origin: "srv:" + from, Clock: 7}, nil
}

func listenMem(t *testing.T, ep *Memory, addr string, h Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = ep.Listen(ctx, addr, h) }()
	t.Cleanup(func() { cancel(); <-done })
	waitFor(t, func() bool { return ep.net.registered(addr) })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func decodePong(t *testing.T, mt wire.MsgType, payload []byte) wire.Pong {
	t.Helper()
	if mt != wire.MsgPong {
		t.Fatalf("got %v, want Pong", mt)
	}
	m, err := wire.DecodeMessage(mt, payload)
	if err != nil {
		t.Fatal(err)
	}
	return m.(wire.Pong)
}

func memPair(t *testing.T) (*Network, *Memory, *Memory) {
	n := NewNetwork()
	a, b := n.Endpoint("a"), n.Endpoint("b")
	listenMem(t, b, "b", pongHandler)
	return n, a, b
}

func TestMemoryPingRoundtrip(t *testing.T) {
	_, a, _ := memPair(t)
	mt, p, err := a.Send(context.Background(), "b", wire.MsgPing, wire.Ping{Origin: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if got := decodePong(t, mt, p); got.Clock != 7 || got.Origin != "srv:a" {
		t.Fatalf("got %+v", got)
	}
}

func TestMemorySendToUnknownAddrFails(t *testing.T) {
	n := NewNetwork()
	_, _, err := n.Endpoint("a").Send(context.Background(), "nobody", wire.MsgPing, wire.Ping{})
	if !errors.Is(err, ErrNoListener) {
		t.Fatalf("err = %v", err)
	}
}

func TestMemoryPartitionAndHeal(t *testing.T) {
	n, a, _ := memPair(t)
	n.Partition("b", "a") // order must not matter
	_, _, err := a.Send(context.Background(), "b", wire.MsgPing, wire.Ping{})
	if !errors.Is(err, ErrPartitioned) {
		t.Fatalf("err = %v", err)
	}
	n.Heal("a", "b")
	if _, _, err := a.Send(context.Background(), "b", wire.MsgPing, wire.Ping{}); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryDropFilterIsHonored(t *testing.T) {
	n, a, _ := memPair(t)
	n.Drop(func(from, to string, mt wire.MsgType) bool { return mt == wire.MsgMutation })
	if _, _, err := a.Send(context.Background(), "b", wire.MsgMutation, wire.Mutation{Origin: "a"}); !errors.Is(err, ErrDropped) {
		t.Fatalf("mutation err = %v", err)
	}
	if _, _, err := a.Send(context.Background(), "b", wire.MsgPing, wire.Ping{}); err != nil {
		t.Fatalf("ping err = %v", err)
	}
	n.Drop(nil)
	if _, _, err := a.Send(context.Background(), "b", wire.MsgMutation, wire.Mutation{Origin: "a"}); err != nil {
		t.Fatalf("after clearing: %v", err)
	}
}

func TestMemorySlowPeerHonorsContext(t *testing.T) {
	n, a, _ := memPair(t)
	n.SetDelay("b", time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := a.Send(ctx, "b", wire.MsgPing, wire.Ping{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("slow peer blocked past ctx deadline")
	}
	n.SetDelay("b", 0)
	if _, _, err := a.Send(context.Background(), "b", wire.MsgPing, wire.Ping{}); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryFailNextDropsConnection(t *testing.T) {
	n, a, _ := memPair(t)
	calls := 0
	listenMem(t, n.Endpoint("c"), "c", func(ctx context.Context, from string, mt wire.MsgType, p []byte) (wire.MsgType, any, error) {
		calls++
		return pongHandler(ctx, from, mt, p)
	})
	n.FailNext("c", 2)
	for i := 0; i < 2; i++ {
		if _, _, err := a.Send(context.Background(), "c", wire.MsgPing, wire.Ping{}); !errors.Is(err, ErrConnDropped) {
			t.Fatalf("send %d err = %v", i, err)
		}
	}
	if _, _, err := a.Send(context.Background(), "c", wire.MsgPing, wire.Ping{}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times, want 1", calls)
	}
}

func TestMemoryHandlerErrorSurfaces(t *testing.T) {
	n := NewNetwork()
	boom := errors.New("boom")
	listenMem(t, n.Endpoint("b"), "b", func(context.Context, string, wire.MsgType, []byte) (wire.MsgType, any, error) {
		return 0, nil, boom
	})
	if _, _, err := n.Endpoint("a").Send(context.Background(), "b", wire.MsgPing, wire.Ping{}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestMemoryListenUnregistersOnCancel(t *testing.T) {
	n := NewNetwork()
	b := n.Endpoint("b")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = b.Listen(ctx, "b", pongHandler) }()
	waitFor(t, func() bool { return n.registered("b") })
	cancel()
	<-done
	if n.registered("b") {
		t.Fatal("still registered")
	}
}
