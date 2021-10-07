// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pool_test

import (
	"testing"
	"time"

	"resenje.org/pool"
)

func TestExpiring(t *testing.T) {
	var createdCount int
	var closedCount int

	p := pool.NewExpiring(
		func(key string) (interface{}, error) {
			createdCount++
			return key, nil
		},
		func(v interface{}) error {
			closedCount++
			return nil
		},
	)

	assert(t, createdCount, 0)
	assert(t, closedCount, 0)

	key1 := "key1"

	got, err := p.Get(key1)
	if err != nil {
		t.Fatal(err)
	}
	assert(t, got, key1)
	assert(t, createdCount, 1)
	assert(t, closedCount, 0)

	p.Release(key1, time.Hour)
	assert(t, createdCount, 1)
	assert(t, closedCount, 0)

	got, err = p.Get(key1)
	if err != nil {
		t.Fatal(err)
	}
	assert(t, got, key1)
	assert(t, createdCount, 1)
	assert(t, closedCount, 0)

	p.Release(key1, time.Hour)
	assert(t, createdCount, 1)
	assert(t, closedCount, 0)

	pool.SetNowFunc(func() time.Time {
		return time.Now().Add(time.Hour)
	})
	defer pool.SetNowFunc(time.Now)

	key2 := "key2"

	got, err = p.Get(key2)
	if err != nil {
		t.Fatal(err)
	}
	assert(t, got, key2)
	assert(t, createdCount, 2)
	assert(t, closedCount, 1)

	got, err = p.Get(key1)
	if err != nil {
		t.Fatal(err)
	}
	assert(t, got, key1)
	assert(t, createdCount, 3)
	assert(t, closedCount, 1)

	if err := p.Clear(); err != nil {
		t.Fatal(err)
	}
	assert(t, createdCount, 3)
	assert(t, closedCount, 3)
}

func assert(t *testing.T, got, want interface{}) {
	t.Helper()

	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
