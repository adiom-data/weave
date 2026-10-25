package entitycache

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestIdleEvictionIsBoundedAndRespectsReactivation(t *testing.T) {
	c := NewEntityCache()
	now := time.Unix(100, 0)
	c.now = func() time.Time { return now }
	for n := 0; n < 10; n++ {
		key := cacheKey(fmt.Sprint(n))
		c.StoreCurrent(key, cacheRecord("idle"))
		c.MarkUnreferenced(key)
	}
	active := cacheKey("0")
	c.Acquire(active)
	if c.idle.Len() != 9 {
		t.Fatal("acquire left an expiry registration")
	}
	cutoff := now.Add(time.Second)
	removed := 0
	for {
		keys, more := c.EvictIdle(cutoff, 2)
		if len(keys) > 2 {
			t.Fatal("eviction exceeded batch limit")
		}
		removed += len(keys)
		if !more {
			break
		}
	}
	if removed != 9 {
		t.Fatalf("removed %d entries", removed)
	}
	if _, ok := c.Get(active); !ok {
		t.Fatal("reactivated entry was evicted")
	}
	now = now.Add(time.Minute)
	c.Release(active)
	if keys, _ := c.EvictIdle(cutoff, 2); len(keys) != 0 {
		t.Fatal("used old expiry after release")
	}
	if keys, _ := c.EvictIdle(now.Add(time.Second), 2); len(keys) != 1 {
		t.Fatal("released entry did not expire")
	}
}

func TestIdleIndexTracksLoadAndDeletionTransitions(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			c := NewEntityCache()
			key := cacheKey("one")
			c.StoreCurrent(key, cacheRecord("old"))
			c.MarkUnreferenced(key)
			load := c.BeginRefresh(key)
			if c.idle.Len() != 0 {
				t.Fatal("refresh left entry evictable")
			}
			if keys, _ := c.EvictIdle(time.Now().Add(time.Hour), 10); len(keys) != 0 {
				t.Fatal("in-flight refresh was evicted")
			}
			var err error
			if complete {
				err = c.Complete(load, cacheRecord("new"))
			} else {
				err = c.Fail(load, errors.New("read failed"))
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.idle.Len() != 1 {
				t.Fatal("refresh completion did not restore eligibility")
			}
			c.StorePartial(key, cacheRecord("partial"))
			if c.idle.Len() != 0 {
				t.Fatal("partial record remained evictable")
			}
			load = c.BeginRefresh(key)
			if _, err := c.CompleteMerged(load, cacheRecord("full")); err != nil {
				t.Fatal(err)
			}
			if c.idle.Len() != 1 {
				t.Fatal("merged completion did not register expiry")
			}
			if err := c.DeleteAuthoritative(key); err != nil {
				t.Fatal(err)
			}
			if c.idle.Len() != 0 {
				t.Fatal("deletion left stale expiry")
			}
			c.StoreCurrent(key, cacheRecord("replacement"))
			c.MarkUnreferenced(key)
			if c.idle.Len() != 1 {
				t.Fatal("replacement expiry missing")
			}
			c.InvalidateRelation(key.Relation)
			if c.idle.Len() != 0 {
				t.Fatal("relation invalidation left stale expiry")
			}
		})
	}
}

func TestIdleTimestampUpdatesDoNotAccumulateTimers(t *testing.T) {
	c := NewEntityCache()
	now := time.Unix(100, 0)
	c.now = func() time.Time { return now }
	key := cacheKey("one")
	c.StoreCurrent(key, cacheRecord("value"))
	for n := 0; n < 1000; n++ {
		c.Acquire(key)
		now = now.Add(time.Second)
		c.Release(key)
	}
	if c.idle.Len() != 1 {
		t.Fatal("idle timers accumulated")
	}
	if keys, _ := c.EvictIdle(now, 1); len(keys) != 0 {
		t.Fatal("strict idle cutoff changed")
	}
	if keys, more := c.EvictIdle(now.Add(time.Nanosecond), 1); len(keys) != 1 || more {
		t.Fatal("latest idle timestamp did not expire")
	}
}

func BenchmarkEvictionWithActiveEntries(b *testing.B) {
	for _, count := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			c := NewEntityCache()
			for n := 0; n < count; n++ {
				key := cacheKey(fmt.Sprint(n))
				c.StoreCurrent(key, cacheRecord("active"))
				c.Acquire(key)
			}
			before := time.Now().Add(time.Hour)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if keys, more := c.EvictIdle(before, 128); len(keys) != 0 || more {
					b.Fatal("active entries expired")
				}
			}
		})
	}
}
