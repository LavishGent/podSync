package clock

import "sync/atomic"

// FakeClock is a manually-controlled clock for deterministic testing.
type FakeClock struct {
	now atomic.Int64
}

func NewFakeClock(nowNano int64) *FakeClock {
	f := &FakeClock{}
	f.now.Store(nowNano)
	return f
}

func (f *FakeClock) Now() int64 { return f.now.Load() }

func (f *FakeClock) Set(nanos int64) { f.now.Store(nanos) }

func (f *FakeClock) Advance(deltaNanos int64) { f.now.Add(deltaNanos) }
