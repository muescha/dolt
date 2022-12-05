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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheUnderCapacity(t *testing.T) {
	sizes := []uint64{64, 512, 4096}
	for _, sz := range sizes {
		t.Run(fmt.Sprintf("size=%d", sz), func(t *testing.T) {
			cache := NewCache[float64, float64](sz, func(_, _ float64) {
				t.Fatal("cache should not evict")
			})
			data := genTestData(cache.Capacity())
			for k, v := range data {
				cache.Put(k, v)
			}
			assert.Equal(t, len(data), cache.Count())
			for k := range data {
				v, ok := cache.Get(k)
				assert.True(t, ok)
				assert.Equal(t, data[k], v)
			}
		})
	}
}

func TestCacheOverCapacity(t *testing.T) {
	sizes := []uint64{64, 512, 4096}
	for _, sz := range sizes {
		t.Run(fmt.Sprintf("size=%d", sz), func(t *testing.T) {
			evicted := make(map[float64]float64)
			cache := NewCache[float64, float64](sz, func(k, v float64) {
				evicted[k] = v
			})
			data := genTestData(cache.Capacity())
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
		})
	}
}

func genTestData(sz int) (data map[float64]float64) {
	data = make(map[float64]float64, sz)
	for i := 0; i < sz; i++ {
		data[float64(i)] = rand.Float64()
	}
	return
}

func TestNextMultiple(t *testing.T) {
	assert.Equal(t, uint64(64), nextMultiple(1))
	assert.Equal(t, uint64(64), nextMultiple(63))
	assert.Equal(t, uint64(64), nextMultiple(64))
	assert.Equal(t, uint64(128), nextMultiple(65))
}
