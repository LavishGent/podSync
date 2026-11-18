package peers

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestStaticReturnsCopy(t *testing.T) {
	r := Static("a:1", "b:1")
	got, err := r.Resolve(context.Background())
	if err != nil || !reflect.DeepEqual(got, []string{"a:1", "b:1"}) {
		t.Fatalf("got %v, %v", got, err)
	}
	got[0] = "x"
	again, _ := r.Resolve(context.Background())
	if again[0] != "a:1" {
		t.Fatal("Static leaked its backing slice")
	}
}

type fakeLookup struct {
	host  string
	addrs []net.IPAddr
	err   error
}

func (f fakeLookup) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if f.err != nil {
		return nil, f.err
	}
	if host != f.host {
		return nil, errors.New("unexpected host " + host)
	}
	return f.addrs, nil
}

func TestDNSBuildsServiceNameAndAddrs(t *testing.T) {
	r := dnsResolver{
		service: "podsync", namespace: "prod", port: 7946,
		lookup: fakeLookup{host: "podsync.prod.svc.cluster.local", addrs: []net.IPAddr{
			{IP: net.ParseIP("10.0.0.2")}, {IP: net.ParseIP("fd00::1")}, {IP: net.ParseIP("10.0.0.1")},
		}},
	}
	got, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.0.0.1:7946", "10.0.0.2:7946", "[fd00::1]:7946"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestDNSPropagatesError(t *testing.T) {
	boom := errors.New("nxdomain")
	r := dnsResolver{service: "s", namespace: "n", port: 1, lookup: fakeLookup{err: boom}}
	if _, err := r.Resolve(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestDNSConstructorAcceptsOptionalNetResolver(t *testing.T) {
	if DNS("s", "n", 1) == nil || DNS("s", "n", 1, &net.Resolver{}) == nil {
		t.Fatal("nil resolver")
	}
}

type seqResolver struct {
	mu   sync.Mutex
	seq  [][]string
	errs []error
	i    int
}

func (s *seqResolver) Resolve(context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := min(s.i, len(s.seq)-1)
	s.i++
	var err error
	if i < len(s.errs) {
		err = s.errs[i]
	}
	return s.seq[i], err
}

func TestSetExcludesSelfAndReportsChanges(t *testing.T) {
	r := &seqResolver{seq: [][]string{
		{"10.0.0.1:1", "10.0.0.2:1", "10.0.0.3:1"},
		{"10.0.0.1:1", "10.0.0.3:1", "10.0.0.4:1"},
	}}
	s := NewSet(r, "10.0.0.1:1")
	type change struct{ added, removed []string }
	var changes []change
	s.OnChange(func(a, rm []string) { changes = append(changes, change{a, rm}) })

	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := s.Peers(); !reflect.DeepEqual(got, []string{"10.0.0.2:1", "10.0.0.3:1"}) {
		t.Fatalf("peers = %v", got)
	}
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := s.Peers(); !reflect.DeepEqual(got, []string{"10.0.0.3:1", "10.0.0.4:1"}) {
		t.Fatalf("peers = %v", got)
	}
	want := []change{
		{[]string{"10.0.0.2:1", "10.0.0.3:1"}, nil},
		{[]string{"10.0.0.4:1"}, []string{"10.0.0.2:1"}},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("changes = %v want %v", changes, want)
	}
}

func TestSetPeersReturnsCopy(t *testing.T) {
	s := NewSet(Static("a:1", "b:1"), "")
	_ = s.Refresh(context.Background())
	p := s.Peers()
	p[0] = "zzz"
	if s.Peers()[0] == "zzz" {
		t.Fatal("Peers leaked internal slice")
	}
}

func TestSetKeepsListOnResolveError(t *testing.T) {
	r := &seqResolver{seq: [][]string{{"a:1"}, nil}, errs: []error{nil, errors.New("dns down")}}
	s := NewSet(r, "")
	_ = s.Refresh(context.Background())
	if err := s.Refresh(context.Background()); err == nil {
		t.Fatal("want error")
	}
	if got := s.Peers(); !reflect.DeepEqual(got, []string{"a:1"}) {
		t.Fatalf("peers = %v", got)
	}
}

func TestSetRunObservesAdditionsAndRemovals(t *testing.T) {
	r := &seqResolver{seq: [][]string{{"a:1", "b:1"}, {"b:1", "c:1"}}}
	s := NewSet(r, "")
	s.RefreshInterval = 5 * time.Millisecond
	var mu sync.Mutex
	var added, removed []string
	s.OnChange(func(a, rm []string) {
		mu.Lock()
		defer mu.Unlock()
		added = append(added, a...)
		removed = append(removed, rm...)
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		ok := len(removed) == 1
		mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no removal observed")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(added, []string{"a:1", "b:1", "c:1"}) || !reflect.DeepEqual(removed, []string{"a:1"}) {
		t.Fatalf("added=%v removed=%v", added, removed)
	}
}

func TestSelfAddrPrefersPodIP(t *testing.T) {
	t.Setenv("POD_IP", "10.1.2.3")
	got, err := SelfAddr(7946)
	if err != nil || got != "10.1.2.3:7946" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestSelfAddrFallsBackToInterface(t *testing.T) {
	t.Setenv("POD_IP", "")
	got, err := SelfAddr(7946)
	if err != nil {
		t.Skipf("no non-loopback interface: %v", err)
	}
	host, port, err := net.SplitHostPort(got)
	if err != nil || port != "7946" {
		t.Fatalf("got %q, %v", got, err)
	}
	if ip := net.ParseIP(host); ip == nil || ip.IsLoopback() {
		t.Fatalf("host %q is not a non-loopback IP", host)
	}
}
