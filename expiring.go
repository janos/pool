// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pool

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// Expiring is a pool of elements associated with string keys that have an
// expiration time after no one is using them.
//
// The elements will be evicted from the pool only on the next Get function call
// or Prune function call. There is no assurances that the eviction will happen
// at the expiration time, just that it will not happen before.
type Expiring struct {
	constructor func(key string) (interface{}, error)
	destructor  func(interface{}) error
	pq          *priorityQueue
	m           map[string]*item
	mu          sync.Mutex
}

// NewExpiring creates a new Expiring pool with constructor and destructor for
// functions for pool elements.
func NewExpiring(
	constructor func(key string) (interface{}, error), // function that construct new elements
	destructor func(interface{}) error,
) *Expiring {
	pq := make(priorityQueue, 0)
	return &Expiring{
		constructor: constructor,
		destructor:  destructor,
		pq:          &pq,
		m:           make(map[string]*item),
	}
}

// Get retrieves a value from the pool referenced by the key. If the value is
// not in the pool, a new instance will be created using the pool's constructor
// function.
func (p *Expiring) Get(key string) (interface{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	i, ok := p.m[key]
	if !ok {
		if err := p.Prune(); err != nil {
			return nil, err
		}
		v, err := p.constructor(key)
		if err != nil {
			return nil, err
		}
		p.m[key] = &item{
			value:      v,
			refCounter: 1,
			index:      -1,
			key:        key,
		}
		return v, nil
	}

	i.refCounter++
	p.pq.remove(i)

	if err := p.Prune(); err != nil {
		return nil, err
	}

	return i.value, nil
}

// Release marks the key in pool as no longer used by the previous Get caller
// and sets it eventual expiration time.
func (p *Expiring) Release(key string, ttl time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	i, ok := p.m[key]
	if !ok {
		return
	}

	i.refCounter--
	if i.refCounter == 0 {
		i.deadtime = nowFunc().Add(ttl)
		heap.Push(p.pq, i)
	}
}

// Prune removes all expired elements.
func (p *Expiring) Prune() error {
	return p.pq.prune(p.destructor, func(key string) { delete(p.m, key) })
}

// Clear removes all elements in the pool regardless if they are expired or not.
func (p *Expiring) Clear() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.pq.Len() > 0 {
		heap.Pop(p.pq)
	}
	for k, v := range p.m {
		delete(p.m, k)
		if p.destructor != nil {
			if err := p.destructor(v); err != nil {
				return fmt.Errorf("close %s: %w", k, err)
			}
		}
	}
	return nil
}

type item struct {
	value      interface{} // The value of the item; arbitrary.
	deadtime   time.Time
	refCounter int
	index      int // The index of the item in the heap, needed by remove.
	key        string
}

// A priorityQueue implements heap.Interface.
type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].deadtime.Before(pq[j].deadtime)
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	i := x.(*item)
	i.index = n
	*pq = append(*pq, i)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	i := old[n-1]
	old[n-1] = nil // avoid memory leak
	i.index = -1
	*pq = old[0 : n-1]
	return i
}

func (pq priorityQueue) peek() *item {
	l := len(pq)
	if l == 0 {
		return nil
	}
	return pq[l-1]
}

func (pq *priorityQueue) remove(i *item) {
	if i.index >= 0 {
		heap.Remove(pq, i.index)
	}
}

func (pq *priorityQueue) prune(f func(v interface{}) error, cb func(key string)) error {
	now := nowFunc()
	for i := pq.peek(); i != nil && i.deadtime.Before(now); i = pq.peek() {
		v := heap.Pop(pq)
		if i == nil {
			break
		}
		i = v.(*item)
		cb(i.key)
		if f != nil {
			if err := f(i.value); err != nil {
				return err
			}
		}
	}
	return nil
}

var nowFunc = time.Now
