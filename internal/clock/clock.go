package clock

import (
	"context"
	"io"
	"sync/atomic"
	"time"
)

// Clock provides the current time in Unix nanoseconds.
type Clock interface {
	Now() int64
}

var _ Clock = (*SystemClock)(nil)

// SystemClock reads the system clock on every call via time.Now().
type SystemClock struct{}

func (SystemClock) Now() int64 { return time.Now().UnixNano() }

var (
	_ Clock     = (*CoarseClock)(nil)
	_ io.Closer = (*CoarseClock)(nil)
)

// CoarseClock is a Clock that caches the current system time every 1ms by
// default. It is safe for concurrent use.
//
// "Coarse" clocks trade nanosecond precision for performance. Reading the time
// is a fast atomic load (~1-2 ns), avoiding expensive system clock lookups on
// hot paths.
type CoarseClock struct {
	now      atomic.Int64
	interval time.Duration
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewCoarseClock constructs a new CoarseClock. Upon instantiation, the current
// system time is immediately stored. The interval parameter specifies how often
// the clock should update the stored system time. The zero value is treated as
// one millisecond.
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

// Start launches the background ticker goroutine. Once the clock is started,
// subsequent calls to Start are a no-op.
func (c *CoarseClock) Start(ctx context.Context) {
	if c.done != nil {
		return
	}
	ctx, c.cancel = context.WithCancel(ctx)
	c.done = make(chan struct{})
	go c.loop(ctx)
}

// Close stops the clock. It is a blocking call.
func (c *CoarseClock) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.done != nil {
		<-c.done
		c.done = nil
		c.cancel = nil
	}
	return nil
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
