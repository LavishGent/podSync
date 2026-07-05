package clock

import "sync/atomic"

// FakeClock is a manually-controlled clock for deterministic testing.
// It does not spawn any goroutines — time only advances when the caller
// explicitly calls Set or Advance.
type FakeClock struct {
	now atomic.Int64
}

// NewFakeClock creates a FakeClock initialised to the given Unix-nanosecond time.
func NewFakeClock(nowNano int64) *FakeClock {
	f := &FakeClock{}
	f.now.Store(nowNano)
	return f
}

// Now returns the current fake time.
func (f *FakeClock) Now() int64 { return f.now.Load() }

// Set sets the fake clock to an absolute Unix-nanosecond value.
func (f *FakeClock) Set(nanos int64) { f.now.Store(nanos) }

// Advance moves the fake clock forward by delta nanoseconds.
func (f *FakeClock) Advance(deltaNanos int64) { f.now.Add(deltaNanos) }
