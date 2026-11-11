package peers

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

// DefaultRefreshInterval is how often Run re-resolves.
const DefaultRefreshInterval = 5 * time.Second

// Set is the current peer list, minus self, refreshed from a Resolver.
type Set struct {
	// RefreshInterval is the Run period. Default 5s. Set before Run.
	RefreshInterval time.Duration

	r    Resolver
	self string

	mu       sync.RWMutex
	peers    []string
	onChange []func(added, removed []string)
}

// NewSet returns a Set that resolves with r and excludes self (may be "").
func NewSet(r Resolver, self string) *Set { return &Set{r: r, self: self} }

// OnChange registers fn, called with the sorted added and removed addresses
// whenever a refresh changes the list.
func (s *Set) OnChange(fn func(added, removed []string)) {
	s.mu.Lock()
	s.onChange = append(s.onChange, fn)
	s.mu.Unlock()
}

// Peers returns a copy of the current peer list.
func (s *Set) Peers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.peers)
}

// Refresh resolves once and swaps the list. On error the old list is kept.
func (s *Set) Refresh(ctx context.Context) error {
	addrs, err := s.r.Resolve(ctx)
	if err != nil {
		return err
	}
	next := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a != s.self {
			next = append(next, a)
		}
	}
	slices.Sort(next)
	next = slices.Compact(next)

	s.mu.Lock()
	prev := s.peers
	s.peers = next
	fns := slices.Clone(s.onChange)
	s.mu.Unlock()

	var added, removed []string
	for _, a := range next {
		if !slices.Contains(prev, a) {
			added = append(added, a)
		}
	}
	for _, a := range prev {
		if !slices.Contains(next, a) {
			removed = append(removed, a)
		}
	}
	if len(added)+len(removed) > 0 {
		for _, fn := range fns {
			fn(added, removed)
		}
	}
	return nil
}

// Run refreshes immediately and then every RefreshInterval until ctx is
// cancelled. Resolution errors keep the previous list and are retried on the
// next tick.
func (s *Set) Run(ctx context.Context) {
	iv := s.RefreshInterval
	if iv <= 0 {
		iv = DefaultRefreshInterval
	}
	_ = s.Refresh(ctx)
	tick := time.NewTicker(iv)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			_ = s.Refresh(ctx)
		}
	}
}

// SelfAddr returns this node's "ip:port": POD_IP if set, else the first
// non-loopback unicast interface address.
func SelfAddr(port int) (string, error) {
	p := strconv.Itoa(port)
	if ip := os.Getenv("POD_IP"); ip != "" {
		return net.JoinHostPort(ip, p), nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && ipn.IP.IsGlobalUnicast() && !ipn.IP.IsLoopback() {
			return net.JoinHostPort(ipn.IP.String(), p), nil
		}
	}
	return "", errors.New("peers: no non-loopback interface address; set POD_IP")
}
