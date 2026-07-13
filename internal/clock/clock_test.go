package clock

import (
	"context"
	"io"
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
	if diff := wall - now; diff < 0 || diff > int64(time.Millisecond) {
		t.Fatalf("SystemClock.Now() too far from time.Now(): diff=%d ns", diff)
	}
}

// ---------------------------------------------------------------------------
// CoarseClock
// ---------------------------------------------------------------------------

func TestCoarseClockSeedBeforeStart(t *testing.T) {
	clk := NewCoarseClock(time.Millisecond)
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
	defer clk.Close()

	before := clk.Now()
	time.Sleep(10 * time.Millisecond)
	after := clk.Now()

	if after <= before {
		t.Fatalf("expected CoarseClock to advance: before=%d, after=%d", before, after)
	}
}

func TestCoarseClockCloseIdempotent(t *testing.T) {
	clk := NewCoarseClock(time.Millisecond)
	if err := clk.Close(); err != nil {
		t.Fatalf("unexpected error closing clock: %v", err)
	}
	if err := clk.Close(); err != nil {
		t.Fatalf("unexpected error closing clock second time: %v", err)
	}
}

func TestCoarseClockStartIdempotent(t *testing.T) {
	clk := NewCoarseClock(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clk.Start(ctx)
	clk.Start(ctx)
	clk.Close()
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
	var _ Clock = (*FakeClock)(nil)
	var _ Clock = SystemClock{}
	var _ Clock = (*CoarseClock)(nil)
	var _ io.Closer = (*CoarseClock)(nil)
}
