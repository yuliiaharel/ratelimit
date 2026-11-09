package memory

import (
	"hash/maphash"
	"sync"
)

// defaultShardCount is how many shards a limiter's shardedMap gets. It's
// a middle ground: enough to spread contention across unrelated keys
// under concurrent load, without very many idle mutexes for callers
// with only a handful of keys.
const defaultShardCount = 32

// shardedMap is a map[string]*V split across multiple independently
// locked shards, so two goroutines working on different keys don't
// contend for the same mutex. V is the small, mutable per-key state a
// limiter keeps (e.g. windowEntry, bucketEntry); callers mutate it in
// place inside withLock's callback, under that key's shard lock.
//
// A single process-wide mutex -- what every limiter in this package
// used before this type existed -- scales fine for one hot key, since
// all callers for that key must serialize regardless. But it also
// serializes *unrelated* keys that never needed to wait on each other.
// Sharding by key hash removes that false contention; it does nothing
// for a workload dominated by one hot key, which still serializes on
// that key's shard.
type shardedMap[V any] struct {
	seed   maphash.Seed
	shards []shard[V]
}

type shard[V any] struct {
	mu sync.Mutex
	m  map[string]*V
}

// newShardedMap returns a shardedMap with shardCount shards. shardCount
// is a parameter (rather than always defaultShardCount) mainly so
// benchmarks can compare shard counts directly.
func newShardedMap[V any](shardCount int) *shardedMap[V] {
	if shardCount < 1 {
		shardCount = 1
	}
	sm := &shardedMap[V]{
		seed:   maphash.MakeSeed(),
		shards: make([]shard[V], shardCount),
	}
	for i := range sm.shards {
		sm.shards[i].m = make(map[string]*V)
	}
	return sm
}

func (sm *shardedMap[V]) shardFor(key string) *shard[V] {
	h := maphash.String(sm.seed, key)
	return &sm.shards[h%uint64(len(sm.shards))]
}

// withLock runs fn with the entry for key, holding that key's shard
// lock for fn's whole duration -- so fn can safely read and mutate the
// entry in place. If key hasn't been seen before, create builds its
// initial state, which is stored before fn runs.
func (sm *shardedMap[V]) withLock(key string, create func() *V, fn func(*V)) {
	s := sm.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	if !ok {
		e = create()
		s.m[key] = e
	}
	fn(e)
}
