package clock

import (
	"context"
	"sync/atomic"
	"time"
)

// Clock provides the current time as Unix nanoseconds.
type Clock interface {
	Now() int64
}

// SystemClock reads the system clock on every call via time.Now().
type SystemClock struct{}

func (SystemClock) Now() int64 { return time.Now().UnixNano() }

// CoarseClock caches the current time in an atomic int64, updated by a
// background goroutine at a configurable interval (default 1 ms).
type CoarseClock struct {
	now      atomic.Int64
	interval time.Duration
	cancel   context.CancelFunc
	done     chan struct{}
}

func NewCoarseClock(interval time.Duration) *CoarseClock {
	if interval <= 0 {
		interval = time.Millisecond
	}
	c := &CoarseClock{
		interval: interval,
	}
	c.now.Store(time.Now().UnixNano())
	return c
}

func (c *CoarseClock) Now() int64 { return c.now.Load() }

// Start launches the background ticker goroutine. Only the first call has an effect.
func (c *CoarseClock) Start(ctx context.Context) {
	if c.done != nil {
		return
	}
	ctx, c.cancel = context.WithCancel(ctx)
	c.done = make(chan struct{})
	go c.loop(ctx)
}

// Stop cancels the background goroutine and blocks until it exits.
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
