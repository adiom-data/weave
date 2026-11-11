package metrics

import (
	"sync"
	"sync/atomic"
	"testing"
)

type memoryProvider struct {
	name   string
	read   func() int64
	closed atomic.Int32
}

func (p *memoryProvider) RegisterGauge(name string, read func() int64) func() {
	p.name, p.read = name, read
	return func() { p.closed.Add(1) }
}

func TestGaugeUsesProviderWithoutExportWorkDuringSet(t *testing.T) {
	p := &memoryProvider{}
	g := NewGauge(p, CachedEntities)
	g.Set(7)
	if p.name != CachedEntities || p.read() != 7 {
		t.Fatal("wrong provider observation")
	}
	g.Close()
	g.Set(99)
	g.Close()
	if p.closed.Load() != 1 || p.read() != 0 {
		t.Fatal("closed gauge was retained or resurrected")
	}
	var disabled *Gauge = NewGauge(nil, CachedEntities)
	disabled.Set(1)
	disabled.Close()
	if disabled != nil {
		t.Fatal("disabled metrics allocated a handle")
	}
}

func TestConcurrentSetReadAndClose(t *testing.T) {
	p := &memoryProvider{}
	g := NewGauge(p, ActiveSubscriptions)
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for i := range 1000 {
				g.Set(i)
			}
			g.Close()
		})
	}
	for range 100 {
		_ = p.read()
	}
	workers.Wait()
	if p.closed.Load() != 1 || p.read() != 0 {
		t.Fatal("concurrent close retained a contribution")
	}
}
