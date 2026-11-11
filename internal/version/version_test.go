package version

import (
	"sync"
	"testing"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b Version
		want int
	}{
		{"higher clock wins", Version{2, "a"}, Version{1, "z"}, 1},
		{"lower clock loses", Version{1, "z"}, Version{2, "a"}, -1},
		{"equal clock higher node wins", Version{5, "b"}, Version{5, "a"}, 1},
		{"equal clock lower node loses", Version{5, "a"}, Version{5, "b"}, -1},
		{"identical is a no-op", Version{5, "a"}, Version{5, "a"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Compare(tt.a, tt.b); got != tt.want {
				t.Fatalf("Compare(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestIsZero(t *testing.T) {
	if !(Version{}).IsZero() {
		t.Fatal("zero Version should report IsZero")
	}
	if (Version{Clock: 1}).IsZero() || (Version{NodeID: "a"}).IsZero() {
		t.Fatal("non-zero Version reported IsZero")
	}
}

func TestNextIncrements(t *testing.T) {
	l := NewLamport("n1")
	a, b := l.Next(), l.Next()
	if a.Clock != 1 || b.Clock != 2 || a.NodeID != "n1" {
		t.Fatalf("got %v, %v", a, b)
	}
}

func TestObserveMonotonicConcurrent(t *testing.T) {
	l := NewLamport("n1")
	const workers, perWorker = 8, 1000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var last uint64
			for i := 0; i < perWorker; i++ {
				observed := uint64(i*workers + w)
				l.Observe(Version{Clock: observed})
				got := l.Next().Clock
				if got <= observed {
					t.Errorf("Next %d does not exceed observed %d", got, observed)
					return
				}
				if got <= last {
					t.Errorf("clock went backwards: %d after %d", got, last)
					return
				}
				last = got
			}
		}(w)
	}
	wg.Wait()
	if got := l.Next().Clock; got < workers*perWorker {
		t.Fatalf("final clock %d lost an observed value", got)
	}
}

func TestObserveNeverLowersClock(t *testing.T) {
	l := NewLamport("n1")
	l.Observe(Version{Clock: 10})
	l.Observe(Version{Clock: 3})
	if got := l.Next().Clock; got != 11 {
		t.Fatalf("got %d, want 11", got)
	}
}

func TestRestartedNodeCatchesUp(t *testing.T) {
	restarted := NewLamport("n2")
	restarted.Observe(Version{Clock: 1000, NodeID: "n1"})
	if got := restarted.Next().Clock; got != 1001 {
		t.Fatalf("got %d, want 1001", got)
	}
}

func TestObserveOnlyConcurrentExactMax(t *testing.T) {
	l := NewLamport("n1")
	const workers, perWorker = 8, 1000
	var wg sync.WaitGroup
	var maxObserved uint64
	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			if c := uint64(1_000_000 + i*workers + w); c > maxObserved {
				maxObserved = c
			}
		}
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				l.Observe(Version{Clock: uint64(1_000_000 + i*workers + w)})
			}
		}(w)
	}
	wg.Wait()
	if got := l.Next().Clock; got != maxObserved+1 {
		t.Fatalf("Next = %d, want %d", got, maxObserved+1)
	}
}

func TestLamportCurrentDoesNotAdvance(t *testing.T) {
	l := NewLamport("n")
	if l.Current() != 0 {
		t.Fatal("start at 0")
	}
	l.Next()
	l.Observe(Version{Clock: 10})
	if l.Current() != 10 || l.Current() != 10 {
		t.Fatalf("Current = %d", l.Current())
	}
}
