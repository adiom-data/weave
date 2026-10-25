// Package expiryindex maintains one removable timestamp per key. Callers own
// synchronization, eligibility rules, and bounded draining of expired entries.
package expiryindex

import (
	"container/heap"
	"time"
)

type item[K comparable] struct {
	key   K
	at    time.Time
	index int
}
type queue[K comparable] []*item[K]

func (q queue[K]) Len() int           { return len(q) }
func (q queue[K]) Less(i, j int) bool { return q[i].at.Before(q[j].at) }
func (q queue[K]) Swap(i, j int)      { q[i], q[j] = q[j], q[i]; q[i].index = i; q[j].index = j }
func (q *queue[K]) Push(value any)    { n := value.(*item[K]); n.index = len(*q); *q = append(*q, n) }
func (q *queue[K]) Pop() any {
	old := *q
	n := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	return n
}

// Index is an indexed min-heap. Its zero value is ready for use. Updating or
// removing a key never leaves stale timer records behind.
type Index[K comparable] struct {
	items map[K]*item[K]
	queue queue[K]
}

func (i *Index[K]) Len() int { return len(i.items) }
func (i *Index[K]) Set(key K, at time.Time) {
	if n := i.items[key]; n != nil {
		n.at = at
		heap.Fix(&i.queue, n.index)
		return
	}
	if i.items == nil {
		i.items = make(map[K]*item[K])
	}
	n := &item[K]{key: key, at: at}
	i.items[key] = n
	heap.Push(&i.queue, n)
}
func (i *Index[K]) Remove(key K) {
	if n := i.items[key]; n != nil {
		heap.Remove(&i.queue, n.index)
		delete(i.items, key)
	}
}
func (i *Index[K]) First() (key K, at time.Time, ok bool) {
	if len(i.queue) == 0 {
		return key, at, false
	}
	n := i.queue[0]
	return n.key, n.at, true
}
