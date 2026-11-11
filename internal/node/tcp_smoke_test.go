package node

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/LavishGent/podsync/internal/peers"
	"github.com/LavishGent/podsync/internal/transport"
	"github.com/LavishGent/podsync/internal/wire"
)

// freeAddr returns a 127.0.0.1 address that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func tcpNode(t *testing.T, self string, all []string) *N {
	t.Helper()
	return build(t, nil, self, all, func(cfg *Config[string, string]) {
		cfg.Transport = &transport.TCP{RequestTimeout: 10 * time.Second, Logf: func(string, ...any) {}}
		cfg.Peers = peers.NewSet(peers.Static(all...), self)
		cfg.RequestTimeout = 10 * time.Second
		cfg.HydrationTimeout = 20 * time.Second
		cfg.QueueSize = 32768 // the test writes 20k keys in a burst; default 1024 would drop
	})
}

func TestTCPSmokeReplicationAndLargeHydration(t *testing.T) {
	addrA, addrB, addrC := freeAddr(t), freeAddr(t), freeAddr(t)
	all := []string{addrA, addrB, addrC}
	a, b := tcpNode(t, addrA, all), tcpNode(t, addrB, all)
	startAll(t, a, b)

	if err := a.Set(context.Background(), "hello", "world", 0); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "replication over TCP", func() bool {
		v, ok := b.Get("hello")
		return ok && v == "world"
	})

	// State larger than one frame: 20k entries of 1 KiB values.
	val := strings.Repeat("x", 1024)
	for i := 0; i < 20_000; i++ {
		if err := a.Set(context.Background(), fmt.Sprintf("big-%d", i), val, 0); err != nil {
			t.Fatal(err)
		}
	}
	if total := 20_000 * 1024; total <= wire.MaxFrameSize {
		t.Fatalf("test state %d does not exceed MaxFrameSize", total)
	}
	eventually(t, 20*time.Second, "b catches up", func() bool { return len(state(b)) == 20_001 })

	// Hydrate a joiner over TCP; every page fits in a frame, the whole does not.
	c := tcpNode(t, addrC, all)
	start(t, c)
	if !equalState(a, c) || len(state(c)) != 20_001 {
		t.Fatalf("joiner has %d entries", len(state(c)))
	}
}
