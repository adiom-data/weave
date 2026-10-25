package dependency

import (
	"fmt"
	"testing"
	"time"
)

func TestIdleExpiryIsIndexedBoundedAndCancelable(t *testing.T) {
	now := time.Unix(100, 0)
	g := &generation{owner: &Cache{options: Options{IdleTTL: time.Minute, EvictionBatchSize: 2}}, entries: make(map[string]*entry), wake: make(chan struct{}, 1)}
	for n := 0; n < 10; n++ {
		e := &entry{id: fmt.Sprint(n), generation: g, finished: true, lastUsed: now, cancel: func() {}}
		g.entries[e.id] = e
		g.syncIdleLocked(e)
	}
	active := g.entries["0"]
	active.waiters++
	g.syncIdleLocked(active)
	if g.idle.Len() != 9 {
		t.Fatal("waiting subscriber did not cancel expiry")
	}
	cutoff := now.Add(time.Minute)
	if g.expireIdleLocked(cutoff.Add(-time.Nanosecond)) || len(g.retired) != 0 {
		t.Fatal("expired before TTL")
	}
	for g.idle.Len() > 0 {
		before := len(g.retired)
		more := g.expireIdleLocked(cutoff)
		if retired := len(g.retired) - before; retired < 1 || retired > 2 {
			t.Fatalf("retired %d entries in one batch", retired)
		}
		if more != (g.idle.Len() > 0) {
			t.Fatal("incorrect backlog indication")
		}
	}
	if active.retired {
		t.Fatal("active entry was retired")
	}
	active.waiters--
	active.lastUsed = cutoff
	g.syncIdleLocked(active)
	if g.expireIdleLocked(cutoff) || active.retired {
		t.Fatal("reactivation retained an old deadline")
	}
	// Repeatedly refreshing the idle timestamp updates one heap entry.
	for n := 0; n < 1000; n++ {
		active.lastUsed = cutoff.Add(time.Duration(n) * time.Second)
		g.syncIdleLocked(active)
	}
	if g.idle.Len() != 1 {
		t.Fatal("stale deadlines accumulated")
	}
	g.retireLocked(active)
	if g.idle.Len() != 0 {
		t.Fatal("explicit retirement left an expiry entry")
	}
}

func TestUnfinishedAndWatchedDependenciesAreNotScheduled(t *testing.T) {
	g := &generation{owner: &Cache{options: Options{IdleTTL: time.Minute}}}
	e := &entry{id: "one", lastUsed: time.Now()}
	g.syncIdleLocked(e)
	if g.idle.Len() != 0 {
		t.Fatal("unfinished entry scheduled")
	}
	e.finished = true
	e.watchers = map[*subscription]struct{}{new(subscription): {}}
	g.syncIdleLocked(e)
	if g.idle.Len() != 0 {
		t.Fatal("watched entry scheduled")
	}
	clear(e.watchers)
	g.syncIdleLocked(e)
	if g.idle.Len() != 1 {
		t.Fatal("last watcher release did not schedule expiry")
	}
	e.retired = true
	g.syncIdleLocked(e)
	if g.idle.Len() != 0 {
		t.Fatal("retired entry remained scheduled")
	}
}
