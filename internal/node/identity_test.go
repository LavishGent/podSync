package node

import (
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestNewIDDistinct(t *testing.T) {
	a, b := NewID(), NewID()
	if a == b {
		t.Fatalf("two IDs identical: %q", a)
	}
}

func TestNewIDUsesHostname(t *testing.T) {
	old := hostname
	defer func() { hostname = old }()
	hostname = func() (string, error) { return "pod-7", nil }
	if id := NewID(); !strings.HasPrefix(id, "pod-7-") {
		t.Fatalf("id %q lacks hostname prefix", id)
	}
}

func TestNewIDFallback(t *testing.T) {
	old := hostname
	defer func() { hostname = old }()
	hex16 := regexp.MustCompile(`^[0-9a-f]{16}$`)
	for name, fn := range map[string]func() (string, error){
		"empty": func() (string, error) { return "", nil },
		"error": func() (string, error) { return "host", errors.New("boom") },
	} {
		hostname = fn
		a, b := NewID(), NewID()
		if !hex16.MatchString(a) || a == b {
			t.Fatalf("%s: fallback IDs %q, %q", name, a, b)
		}
	}
}

func TestNewIDConcurrentUnique(t *testing.T) {
	old := hostname
	defer func() { hostname = old }()
	hostname = func() (string, error) { return "pod", nil }
	const n = 100
	ids := make([]string, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ids[i] = NewID()
		}(i)
	}
	close(start)
	wg.Wait()
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate ID %q", id)
		}
		seen[id] = true
	}
}
