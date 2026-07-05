package clock

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// SystemClock
// ---------------------------------------------------------------------------

func TestSystemClockReturnsReasonableTime(t *testing.T) {
	var clk SystemClock
	now := clk.Now()
	wall := time.Now().UnixNano()
	// The two calls should be within 1 ms of each other.
	if diff := wall - now; diff < 0 || diff > int64(time.Millisecond) {
		t.Fatalf("SystemClock.Now() too far from time.Now(): diff=%d ns", diff)
	}
}

// ---------------------------------------------------------------------------
// CoarseClock
// ---------------------------------------------------------------------------

func TestCoarseClockSeedBeforeStart(t *testing.T) {
	clk := NewCoarseClock(time.Millisecond)
	// Even before Start, Now() should return a reasonable timestamp
	// (seeded in the constructor).
	now := clk.Now()
	wall := time.Now().UnixNano()
	if diff := wall - now; diff < 0 || diff > int64(time.Second) {
		t.Fatalf("CoarseClock.Now() before Start too far from wall: diff=%d ns", diff)
	}
}

func TestCoarseClockUpdates(t *testing.T) {
	clk := NewCoarseClock(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk.Start(ctx)
	defer clk.Stop()

	before := clk.Now()
	// Sleep enough for several ticks to fire.
	time.Sleep(10 * time.Millisecond)
	after := clk.Now()

	if after <= before {
		t.Fatalf("expected CoarseClock to advance: before=%d, after=%d", before, after)
	}
}

func TestCoarseClockStopIdempotent(t *testing.T) {
	clk := NewCoarseClock(time.Millisecond)
	// Stop on a clock that was never started should not panic.
	clk.Stop()
	clk.Stop()
}

func TestCoarseClockStartIdempotent(t *testing.T) {
	clk := NewCoarseClock(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clk.Start(ctx)
	clk.Start(ctx) // second Start should be a no-op
	clk.Stop()
}

// ---------------------------------------------------------------------------
// FakeClock
// ---------------------------------------------------------------------------

func TestFakeClockSet(t *testing.T) {
	clk := NewFakeClock(1000)
	if clk.Now() != 1000 {
		t.Fatalf("expected 1000, got %d", clk.Now())
	}
	clk.Set(5000)
	if clk.Now() != 5000 {
		t.Fatalf("expected 5000, got %d", clk.Now())
	}
}

func TestFakeClockAdvance(t *testing.T) {
	clk := NewFakeClock(1000)
	clk.Advance(500)
	if clk.Now() != 1500 {
		t.Fatalf("expected 1500, got %d", clk.Now())
	}
}

func TestFakeClockImplementsClock(t *testing.T) {
	// Compile-time check that FakeClock satisfies the Clock interface.
	var _ Clock = (*FakeClock)(nil)
	var _ Clock = SystemClock{}
	var _ Clock = (*CoarseClock)(nil)
}
