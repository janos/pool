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
type Expiring[T any] struct {
	constructor func(key string) (T, error)
	destructor  func(T) error
	pq          *priorityQueue[T]
	m           map[string]*item[T]
	mu          sync.Mutex
}

// NewExpiring creates a new Expiring pool with constructor and destructor for
// functions for pool elements.
func NewExpiring[T any](
	constructor func(key string) (T, error), // function that construct new elements
	destructor func(T) error,
) *Expiring[T] {
	pq := make(priorityQueue[T], 0)
	return &Expiring[T]{
		constructor: constructor,
		destructor:  destructor,
		pq:          &pq,
		m:           make(map[string]*item[T]),
	}
}

// Get retrieves a value from the pool referenced by the key. If the value is
// not in the pool, a new instance will be created using the pool's constructor
// function.
func (p *Expiring[T]) Get(key string) (t T, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	i, ok := p.m[key]
	if !ok {
		if err := p.Prune(); err != nil {
			return t, err
		}
		v, err := p.constructor(key)
		if err != nil {
			return t, err
		}
		p.m[key] = &item[T]{
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
		return t, err
	}

	return i.value, nil
}

// Release marks the key in pool as no longer used by the previous Get caller
// and sets it eventual expiration time.
func (p *Expiring[T]) Release(key string, ttl time.Duration) {
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
func (p *Expiring[T]) Prune() error {
	return p.pq.prune(p.destructor, func(key string) { delete(p.m, key) })
}

// Clear removes all elements in the pool regardless if they are expired or not.
func (p *Expiring[T]) Clear() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.pq.Len() > 0 {
		heap.Pop(p.pq)
	}
	for k, v := range p.m {
		delete(p.m, k)
		if p.destructor != nil {
			if err := p.destructor(v.value); err != nil {
				return fmt.Errorf("close %s: %w", k, err)
			}
		}
	}
	return nil
}

type item[T any] struct {
	value      T // The value of the item; arbitrary.
	deadtime   time.Time
	refCounter int
	index      int // The index of the item in the heap, needed by remove.
	key        string
}

// A priorityQueue implements heap.Interface.
type priorityQueue[T any] []*item[T]

func (pq priorityQueue[T]) Len() int { return len(pq) }

func (pq priorityQueue[T]) Less(i, j int) bool {
	return pq[i].deadtime.Before(pq[j].deadtime)
}

func (pq priorityQueue[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue[T]) Push(x any) {
	n := len(*pq)
	i := x.(*item[T])
	i.index = n
	*pq = append(*pq, i)
}

func (pq *priorityQueue[T]) Pop() any {
	old := *pq
	n := len(old)
	i := old[n-1]
	old[n-1] = nil // avoid memory leak
	i.index = -1
	*pq = old[0 : n-1]
	return i
}

func (pq *priorityQueue[T]) remove(i *item[T]) {
	if i.index >= 0 {
		heap.Remove(pq, i.index)
	}
}

func (pq *priorityQueue[T]) prune(destructor func(v T) error, callback func(key string)) error {
	now := nowFunc()
	for l := pq.Len(); l > 0; l = pq.Len() {
		root := (*pq)[0]
		if !root.deadtime.Before(now) {
			break
		}
		v := heap.Pop(pq)
		i := v.(*item[T])
		callback(i.key)
		if destructor != nil {
			if err := destructor(i.value); err != nil {
				return err
			}
		}
	}
	return nil
}

var nowFunc = time.Now
