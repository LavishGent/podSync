package clock

import (
	"context"
	"sync/atomic"
	"time"
)

// Clock provides the current time as Unix nanoseconds.
// All storage engine time checks go through this interface so that
// the time source can be swapped for testing or replaced with a
// low-overhead coarse clock in production.
type Clock interface {
	Now() int64 // Unix nanoseconds
}

// SystemClock reads the system clock on every call via time.Now().
// This is the default clock used when no Clock is provided in StoreOptions.
// It is precise but relatively expensive under high concurrency, especially
// on Windows where time.Now() costs ~100–150 ns (vs ~15 ns on Linux via vDSO).
type SystemClock struct{}

// Now returns time.Now().UnixNano().
func (SystemClock) Now() int64 { return time.Now().UnixNano() }

// CoarseClock caches the current time in an atomic int64, updated by a
// background goroutine at a configurable interval (default 1 ms).
//
// Reading the cached value is a single atomic load (~1–2 ns), making it
// significantly faster than time.Now() on the hot path. The trade-off is
// that the returned time may lag behind wall-clock time by up to one tick
// interval — acceptable for TTL expiration checks where millisecond
// precision is sufficient.
//
// Usage:
//
//	clk := clock.NewCoarseClock(time.Millisecond)
//	clk.Start(ctx)
//	defer clk.Stop()
//	store := storage.NewStore[string, string](storage.StoreOptions{Clock: clk})
type CoarseClock struct {
	now      atomic.Int64
	interval time.Duration
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewCoarseClock creates a CoarseClock that updates every interval.
// A zero or negative interval defaults to 1 ms.
// The clock is not running until Start is called.
func NewCoarseClock(interval time.Duration) *CoarseClock {
	if interval <= 0 {
		interval = time.Millisecond
	}
	c := &CoarseClock{
		interval: interval,
	}
	// Seed with the current time so Now() returns a reasonable value
	// even before Start is called.
	c.now.Store(time.Now().UnixNano())
	return c
}

// Now returns the most recently cached Unix-nanosecond timestamp.
func (c *CoarseClock) Now() int64 { return c.now.Load() }

// Start launches the background ticker goroutine. It is safe to call
// Start multiple times, but only the first call has an effect. The
// goroutine will run until Stop is called or ctx is cancelled.
func (c *CoarseClock) Start(ctx context.Context) {
	if c.done != nil {
		return // already running
	}
	ctx, c.cancel = context.WithCancel(ctx)
	c.done = make(chan struct{})
	go c.loop(ctx)
}

// Stop cancels the background goroutine and blocks until it exits.
// It is safe to call Stop on a clock that was never started.
func (c *CoarseClock) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.done != nil {
		<-c.done
		c.done = nil
		c.cancel = nil
	}
}

func (c *CoarseClock) loop(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.now.Store(time.Now().UnixNano())
		case <-ctx.Done():
			return
		}
	}
}
