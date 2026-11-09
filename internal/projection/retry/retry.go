// Package retry owns bounded recovery schedules. Owners serialize State with
// their existing locks; this package performs no IO and starts no goroutines.
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

type Policy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	MaxAttempts  int // Retries after the original attempt, not total attempts.
}

func (p Policy) Validate() error {
	if p.InitialDelay <= 0 || p.MaxDelay < p.InitialDelay || p.MaxAttempts <= 0 {
		return errors.New("recovery requires positive delays and attempts, with max delay >= initial delay")
	}
	return nil
}

type State struct {
	attempts int
	next     time.Time
}

// Failed schedules the next attempt once. Repeated notifications of the same
// failure do not move the deadline or replenish its budget. Successful owners
// reset State; replacing an errored branch carries it to the replacement.
func (s *State) Failed(now time.Time, p Policy) {
	if !s.next.IsZero() || s.attempts >= p.MaxAttempts {
		return
	}
	delay := p.InitialDelay
	for n := 0; n < s.attempts && delay < p.MaxDelay; n++ {
		if delay > p.MaxDelay/2 {
			delay = p.MaxDelay
		} else {
			delay *= 2
		}
	}
	// Equal jitter: [delay/2, delay], avoiding synchronized retry storms.
	lower := delay / 2
	delay = lower + time.Duration(rand.Int64N(int64(delay-lower)+1))
	s.next = now.Add(delay)
}

func (s *State) Take(now time.Time) bool {
	if s.next.IsZero() || now.Before(s.next) {
		return false
	}
	s.attempts++
	s.next = time.Time{}
	return true
}

func (s State) Next() time.Time { return s.next }

func Earlier(a, b time.Time) time.Time {
	if a.IsZero() || !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}

// Wait sleeps only until the earliest scheduled attempt or a lifecycle change.
// With no deadline (including exhausted budgets) it does no periodic polling.
func Wait(ctx context.Context, wake <-chan struct{}, next time.Time) bool {
	var due <-chan time.Time
	if !next.IsZero() {
		timer := time.NewTimer(time.Until(next))
		due = timer.C
		defer timer.Stop()
	}
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return true
	case <-due:
		return true
	}
}
