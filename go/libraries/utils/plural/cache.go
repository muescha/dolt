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
	keys    map[K]uint64
	entries []entry[K, V]
	lru     pseudoLRU
	mu      sync.RWMutex
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

func NewCache[K comparable, V any](sz uint64) *Cache[K, V] {
	sz = nextMultiple(sz)
	lru := pseudoLRU{
		blocks: make([]uint64, sz/64),
	}
	return &Cache[K, V]{
		keys:    make(map[K]uint64, sz),
		entries: make([]entry[K, V], sz),
		lru:     lru,
	}
}

func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var idx uint64
	idx, ok = c.keys[key]
	if ok {
		value = c.entries[idx].value
		c.lru.touch(idx)
	}
	return
}

func (c *Cache[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := c.lru.evict()
	prev := c.entries[idx]
	delete(c.keys, prev.key)
	c.entries[idx] = entry[K, V]{
		key:   key,
		value: value,
	}
	c.keys[key] = idx
}

func (c *Cache[K, V]) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.keys)
}

func nextMultiple(n uint64) uint64 {
	return ((n + 63) / 64) * 64
}
