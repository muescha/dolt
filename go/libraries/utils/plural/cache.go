// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plural

import "sync"

type Cache[K comparable, V any] struct {
	elems map[K]entry[V]
	keys  []K
	lru   pseudoLRU
	cb    EvictFn[K, V]
	lock  sync.RWMutex
}

type EvictFn[K comparable, V any] func(key K, value V)

type entry[V any] struct {
	value V
	idx   uint64
}

// NewCache returns a cache with capacity of at least |sz|.
func NewCache[K comparable, V any](sz uint64, cb EvictFn[K, V]) *Cache[K, V] {
	sz = nextMultiple(sz)
	lru := pseudoLRU{
		blocks: make([]uint64, sz/64),
	}
	if cb == nil {
		cb = func(K, V) {}
	}
	return &Cache[K, V]{
		elems: make(map[K]entry[V], sz),
		keys:  make([]K, sz),
		cb:    cb,
		lru:   lru,
	}
}

func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var e entry[V]
	if e, ok = c.elems[key]; ok {
		value = e.value
		c.lru.touch(e.idx)
	}
	return
}

func (c *Cache[K, V]) Put(key K, value V) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if e, ok := c.elems[key]; ok {
		// update |e| in-place
		e.value = value
		c.elems[key] = e
		c.lru.touch(e.idx)
		return
	}
	idx := c.lru.evict()
	victim := c.keys[idx]
	e, ok := c.elems[victim]
	// check |e.idx| in case
	// |victim| is zero-valued
	if ok && e.idx == idx {
		c.cb(victim, e.value)
		delete(c.elems, victim)
	}
	c.elems[key] = entry[V]{
		value: value,
		idx:   idx,
	}
	c.keys[idx] = key
}

func (c *Cache[K, V]) Capacity() int {
	return int(c.lru.capacity())
}

func (c *Cache[K, V]) Count() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.keys)
}

func nextMultiple(n uint64) uint64 {
	return ((n + 63) / 64) * 64
}
