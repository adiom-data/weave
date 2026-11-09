package retry

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestScheduleBackoffJitterBudgetAndReset(t *testing.T) {
	p := Policy{InitialDelay: time.Second, MaxDelay: 3 * time.Second, MaxAttempts: 5}
	now := time.Now()
	var state State
	for i, cap := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second, 3 * time.Second} {
		state.Failed(now, p)
		at := state.Next()
		if delay := at.Sub(now); delay < cap/2 || delay > cap {
			t.Fatalf("retry %d delay=%s cap=%s", i, delay, cap)
		}
		state.Failed(now.Add(time.Hour), p)
		if state.Next() != at || state.Take(at.Add(-time.Nanosecond)) {
			t.Fatal("duplicate failure moved the schedule or early attempt succeeded")
		}
		if !state.Take(at) || state.Take(at) {
			t.Fatal("attempt wasn't taken exactly once")
		}
		now = at
	}
	state.Failed(now, p)
	if !state.Next().IsZero() || state.Take(now.Add(time.Hour)) {
		t.Fatal("exhausted retry budget restarted")
	}
	state = State{}
	state.Failed(now, p)
	if state.Next().IsZero() {
		t.Fatal("new failure cycle didn't reset")
	}
}

func TestSchedulesDoNotOverflow(t *testing.T) {
	p := Policy{InitialDelay: time.Duration(math.MaxInt64/2 + 1), MaxDelay: time.Duration(math.MaxInt64), MaxAttempts: 3}
	var state State
	now := time.Now()
	for range 3 {
		state.Failed(now, p)
		if !state.Next().After(now) {
			t.Fatal("overflowed delay")
		}
		now = state.Next()
		state.Take(now)
	}
}

func TestWaitWithoutScheduleOnlyWakesForChangeOrCancellation(t *testing.T) {
	wake := make(chan struct{}, 1)
	wake <- struct{}{}
	if !Wait(t.Context(), wake, time.Time{}) {
		t.Fatal("lost change wake")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if Wait(ctx, wake, time.Time{}) {
		t.Fatal("ignored cancellation")
	}
}

func TestRejectsInvalidPolicies(t *testing.T) {
	for _, p := range []Policy{{}, {InitialDelay: time.Second, MaxDelay: time.Millisecond, MaxAttempts: 1}, {InitialDelay: time.Second, MaxDelay: time.Second}} {
		if p.Validate() == nil {
			t.Fatal("accepted invalid policy", p)
		}
	}
}
