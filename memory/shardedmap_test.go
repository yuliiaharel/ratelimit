package memory

import (
	"fmt"
	"sync"
	"testing"
)

type counter struct {
	n int
}

func TestShardedMapWithLockCreatesOnce(t *testing.T) {
	sm := newShardedMap[counter](4)

	created := 0
	create := func() *counter {
		created++
		return &counter{}
	}

	sm.withLock("a", create, func(c *counter) { c.n++ })
	sm.withLock("a", create, func(c *counter) { c.n++ })
	sm.withLock("a", create, func(c *counter) { c.n++ })

	if created != 1 {
		t.Fatalf("create called %d times, want 1", created)
	}

	var got int
	sm.withLock("a", create, func(c *counter) { got = c.n })
	if got != 3 {
		t.Fatalf("n = %d, want 3", got)
	}
}

func TestShardedMapKeysAreIndependent(t *testing.T) {
	sm := newShardedMap[counter](4)
	create := func() *counter { return &counter{} }

	sm.withLock("a", create, func(c *counter) { c.n = 1 })
	sm.withLock("b", create, func(c *counter) { c.n = 2 })

	var a, b int
	sm.withLock("a", create, func(c *counter) { a = c.n })
	sm.withLock("b", create, func(c *counter) { b = c.n })

	if a != 1 || b != 2 {
		t.Fatalf("a = %d, b = %d, want 1, 2", a, b)
	}
}

func TestShardedMapShardCountFloorsAtOne(t *testing.T) {
	sm := newShardedMap[counter](0)
	if len(sm.shards) != 1 {
		t.Fatalf("len(shards) = %d, want 1", len(sm.shards))
	}
}

// TestShardedMapConcurrentManyKeys hammers many distinct keys from many
// goroutines at once. Unlike a single hot key, this actually exercises
// more than one shard's mutex concurrently. Run with -race.
func TestShardedMapConcurrentManyKeys(t *testing.T) {
	const (
		goroutines      = 50
		incPerGoroutine = 20
		numKeys         = 16
	)
	sm := newShardedMap[counter](defaultShardCount)
	create := func() *counter { return &counter{} }

	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range incPerGoroutine {
				key := keys[(g+i)%numKeys]
				sm.withLock(key, create, func(c *counter) { c.n++ })
			}
		}(g)
	}
	wg.Wait()

	total := 0
	for _, key := range keys {
		sm.withLock(key, create, func(c *counter) { total += c.n })
	}

	if want := goroutines * incPerGoroutine; total != want {
		t.Fatalf("total = %d, want %d", total, want)
	}
}
