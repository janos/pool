// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pool_test

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"resenje.org/pool"
)

func TestExpiring(t *testing.T) {
	constructed := make([]string, 0)
	destructed := make([]string, 0)

	p := pool.NewExpiring(
		func(key string) (string, error) {
			constructed = append(constructed, key)
			return key, nil
		},
		func(v string) error {
			destructed = append(destructed, v)
			return nil
		},
	)

	assertEqual(t, constructed, []string{})
	assertEqual(t, destructed, []string{})

	got, err := p.Get("key1")
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, got, "key1")
	assertEqual(t, constructed, []string{"key1"})
	assertEqual(t, destructed, []string{})

	p.Release("key1", time.Hour)
	assertEqual(t, constructed, []string{"key1"})
	assertEqual(t, destructed, []string{})

	got, err = p.Get("key1")
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, got, "key1")
	assertEqual(t, constructed, []string{"key1"})
	assertEqual(t, destructed, []string{})

	p.Release("key1", time.Hour)
	assertEqual(t, constructed, []string{"key1"})
	assertEqual(t, destructed, []string{})

	pool.SetNowFunc(func() time.Time {
		return time.Now().Add(time.Hour + time.Minute)
	})
	defer pool.SetNowFunc(time.Now)

	got, err = p.Get("key2")
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, got, "key2")
	assertEqual(t, constructed, []string{"key1", "key2"})
	assertEqual(t, destructed, []string{"key1"})

	got, err = p.Get("key1")
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, got, "key1")
	assertEqual(t, constructed, []string{"key1", "key2", "key1"})
	assertEqual(t, destructed, []string{"key1"})

	if err := p.Clear(); err != nil {
		t.Fatal(err)
	}
	assertEqual(t, constructed, []string{"key1", "key2", "key1"})
	sort.Strings(destructed) // destruction order is not preserved on clear
	assertEqual(t, destructed, []string{"key1", "key1", "key2"})
}

func TestExpiring_Release_unknownKey(t *testing.T) {
	constructed := make([]string, 0)
	destructed := make([]string, 0)

	p := pool.NewExpiring(
		func(key string) (string, error) {
			constructed = append(constructed, key)
			return key, nil
		},
		func(v string) error {
			destructed = append(destructed, v)
			return nil
		},
	)

	p.Release("unknown key", time.Minute)

	assertEqual(t, constructed, []string{})
	assertEqual(t, destructed, []string{})
}

func TestExpiring_Clear(t *testing.T) {
	constructed := make([]string, 0)
	destructed := make([]time.Time, 0)

	values := map[string]time.Time{
		"key1": time.Now(),
		"key2": time.Now().Add(time.Hour),
	}

	p := pool.NewExpiring(
		func(key string) (time.Time, error) {
			constructed = append(constructed, key)
			return values[key], nil
		},
		func(v time.Time) error {
			destructed = append(destructed, v)
			return nil
		},
	)

	if _, err := p.Get("key1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Get("key2"); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, constructed, []string{"key1", "key2"})
	assertEqual(t, destructed, []time.Time{})

	p.Release("key1", time.Hour)

	assertEqual(t, constructed, []string{"key1", "key2"})
	assertEqual(t, destructed, []time.Time{})

	if err := p.Clear(); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, constructed, []string{"key1", "key2"})
	sort.Slice(destructed, func(i, j int) bool {
		return destructed[i].Before(destructed[j])
	}) // destruction order is not preserved on clear
	assertEqual(t, destructed, []time.Time{values["key1"], values["key2"]})
}

func TestExpiring_Prune(t *testing.T) {
	constructed := make([]string, 0)
	destructed := make([]string, 0)

	p := pool.NewExpiring(
		func(key string) (string, error) {
			constructed = append(constructed, key)
			return key, nil
		},
		func(v string) error {
			destructed = append(destructed, v)
			return nil
		},
	)

	if _, err := p.Get("key1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Get("key2"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Get("key3"); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, constructed, []string{"key1", "key2", "key3"})
	assertEqual(t, destructed, []string{})

	p.Release("key1", time.Hour)
	p.Release("key2", time.Minute)
	p.Release("key3", 10*time.Minute)

	assertEqual(t, constructed, []string{"key1", "key2", "key3"})
	assertEqual(t, destructed, []string{})

	pool.SetNowFunc(func() time.Time {
		return time.Now().Add(2 * time.Minute)
	})
	defer pool.SetNowFunc(time.Now)

	if err := p.Prune(); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, destructed, []string{"key2"})

	pool.SetNowFunc(func() time.Time {
		return time.Now().Add(time.Hour + time.Minute)
	})
	defer pool.SetNowFunc(time.Now)

	if err := p.Prune(); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, destructed, []string{"key2", "key3", "key1"})
}

func assertEqual[T any](t *testing.T, got, want T) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
