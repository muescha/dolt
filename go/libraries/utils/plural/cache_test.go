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

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

type generator[K comparable, V any] func(sz int) map[K]V

func TestCacheUnderCapacity(t *testing.T) {
	sizes := []uint64{64, 512, 4096, 32768}
	for _, sz := range sizes {
		t.Run(fmt.Sprintf("float64 cache_size=%d", sz), func(t *testing.T) {
			testCacheUnderCapacity(t, sz, genFloatData)
		})
		t.Run(fmt.Sprintf("int cache_size=%d", sz), func(t *testing.T) {
			testCacheUnderCapacity(t, sz, genIntData)
		})
		t.Run(fmt.Sprintf("string cache_size=%d", sz), func(t *testing.T) {
			testCacheUnderCapacity(t, sz, genStringData)
		})
	}
}

func TestCacheConcurrent(t *testing.T) {
	sizes := []uint64{64, 512, 4096, 32768}
	for _, sz := range sizes {
		t.Run(fmt.Sprintf("float64 cache_size=%d", sz), func(t *testing.T) {
			testCacheConcurrent(t, sz, genFloatData)
		})
		t.Run(fmt.Sprintf("int cache_size=%d", sz), func(t *testing.T) {
			testCacheConcurrent(t, sz, genIntData)
		})
		t.Run(fmt.Sprintf("string cache_size=%d", sz), func(t *testing.T) {
			testCacheConcurrent(t, sz, genStringData)
		})
	}
}

func TestCacheOverCapacity(t *testing.T) {
	sizes := []uint64{64, 512, 4096, 32768}
	for _, sz := range sizes {
		t.Run(fmt.Sprintf("float64 cache_size=%d", sz), func(t *testing.T) {
			testCacheOverCapacity(t, sz, genFloatData)
		})
		t.Run(fmt.Sprintf("int cache_size=%d", sz), func(t *testing.T) {
			testCacheOverCapacity(t, sz, genIntData)
		})
		t.Run(fmt.Sprintf("string cache_size=%d", sz), func(t *testing.T) {
			testCacheOverCapacity(t, sz, genStringData)
		})
	}
}

func testCacheUnderCapacity[K comparable, V any](t *testing.T, sz uint64, gen generator[K, V]) {
	cache := NewCache[K, V](sz, func(K, V) {
		t.Fatal("cache should not evict")
	})
	data := gen(int(sz))
	for k, v := range data {
		cache.Put(k, v)
	}
	assert.Equal(t, len(data), cache.Count())
	for k := range data {
		v, ok := cache.Get(k)
		assert.True(t, ok)
		assert.Equal(t, data[k], v)
	}
}

func testCacheOverCapacity[K comparable, V any](t *testing.T, sz uint64, gen generator[K, V]) {
	evicted := make(map[K]V)
	cache := NewCache[K, V](sz, func(k K, v V) {
		evicted[k] = v
	})
	data := gen(cache.Capacity() * 2)
	require.Equal(t, int(sz*2), len(data))
	for k, v := range data {
		cache.Put(k, v)
	}
	for k := range data {
		if _, ok := evicted[k]; ok {
			_, ok = cache.Get(k)
			assert.False(t, ok)
		} else {
			v, ok := cache.Get(k)
			assert.True(t, ok)
			assert.Equal(t, data[k], v)
		}
	}
	assert.Equal(t, len(data), cache.Count()+len(evicted))
	assert.Equal(t, cache.Count(), cache.Capacity())
}

func testCacheConcurrent[K comparable, V any](t *testing.T, sz uint64, gen generator[K, V]) {
	evicted := newSyncMap[K, V]()
	cache := NewCache[K, V](sz, func(k K, v V) {
		evicted.Put(k, v)
	})

	init := gen(int(sz))
	for k, v := range init {
		cache.Put(k, v)
	}

	edits := make([]map[K]V, 8)
	for i := range edits {
		edits[i] = gen(int(sz) / 8)
	}

	n := runtime.NumCPU()
	require.True(t, n%2 == 0)
	wg := new(sync.WaitGroup)
	wg.Add(n)

	for i := 0; i < n/2; i++ { // readers
		go func() {
			for k := range init {
				v, ok := cache.Get(k)
				if !ok {
					v, ok = evicted.Get(k)
				}
				assert.True(t, ok)
				assert.Equal(t, init[k], v)
			}
			wg.Done()
		}()
	}
	for i := 0; i < n/2; i++ { // writers
		data := edits[i]
		go func() {
			for k, v := range data {
				cache.Put(k, v)
			}
			for k := range data {
				v, ok := cache.Get(k)
				if !ok {
					v, ok = evicted.Get(k)
				}
				assert.True(t, ok)
				assert.Equal(t, data[k], v)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func genFloatData(sz int) (data map[float64]float64) {
	data = make(map[float64]float64, sz)
	for i := 0; i < sz; i++ {
		var k float64
		for { // find unique key |k|
			k = rand.Float64()
			if _, ok := data[k]; !ok {
				break
			}
		}
		data[k] = rand.Float64()
	}
	return
}

func genIntData(sz int) (data map[int]int) {
	data = make(map[int]int, sz)
	for i := 0; i < sz; i++ {
		var k int
		for { // find unique key |k|
			k = rand.Int()
			if _, ok := data[k]; !ok {
				break
			}
		}
		data[k] = rand.Int()
	}
	return
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func genStringData(sz int) (data map[string]string) {
	str := func(n int) string {
		s := make([]rune, n)
		for i := range s {
			s[i] = letters[rand.Intn(52)]
		}
		return string(s)
	}

	data = make(map[string]string, sz)
	for i := 0; i < sz; i++ {
		var k string
		for { // find unique key |k|
			k = str(10)
			if _, ok := data[k]; !ok {
				break
			}
		}
		data[k] = str(10)
	}
	return
}

func newSyncMap[K comparable, V any]() syncMap[K, V] {
	return syncMap[K, V]{
		data: make(map[K]V),
		lock: new(sync.RWMutex),
	}
}

type syncMap[K comparable, V any] struct {
	data map[K]V
	lock *sync.RWMutex
}

func (m syncMap[K, V]) Put(k K, v V) {
	m.lock.Lock()
	m.data[k] = v
	m.lock.Unlock()
}

func (m syncMap[K, V]) Get(k K) (v V, ok bool) {
	m.lock.RLock()
	v, ok = m.data[k]
	m.lock.RUnlock()
	return
}

func TestNextMultiple(t *testing.T) {
	assert.Equal(t, uint64(64), nextMultiple(1))
	assert.Equal(t, uint64(64), nextMultiple(63))
	assert.Equal(t, uint64(64), nextMultiple(64))
	assert.Equal(t, uint64(128), nextMultiple(65))
}
