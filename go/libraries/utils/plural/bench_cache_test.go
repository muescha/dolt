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
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkCache(b *testing.B) {
	sizes := []uint64{64, 512, 4096, 32768}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("int cache_size=%d", sz), func(b *testing.B) {
			benchmarkCache(b, sz, genIntData)
		})
	}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("string cache_size=%d", sz), func(b *testing.B) {
			benchmarkCache(b, sz, genStringData)
		})
	}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("float64 cache_size=%d", sz), func(b *testing.B) {
			benchmarkCache(b, sz, genFloatData)
		})
	}
}

func BenchmarkCacheConcurrent(b *testing.B) {
	sizes := []uint64{512, 4096, 32768}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("int cache_size=%d", sz), func(b *testing.B) {
			benchmarkCacheConcurrent(b, sz, genIntData)
		})
	}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("string cache_size=%d", sz), func(b *testing.B) {
			benchmarkCacheConcurrent(b, sz, genStringData)
		})
	}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("float64 cache_size=%d", sz), func(b *testing.B) {
			benchmarkCacheConcurrent(b, sz, genFloatData)
		})
	}
}

func benchmarkCache[K comparable, V any](b *testing.B, sz uint64, gen generator[K, V]) {
	data := gen(int(sz))
	cache := NewCache(sz, func(K, V) {})
	keys := make([]K, 0, sz)
	for k, v := range data {
		cache.Put(k, v)
		keys = append(keys, k)
	}
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = cache.Get(keys[i%int(sz)])
	}
	assert.True(b, ok)
	b.ReportAllocs()
}

func benchmarkCacheConcurrent[K comparable, V any](b *testing.B, sz uint64, gen generator[K, V]) {
	data := gen(int(sz))
	cache := NewCache(sz, func(K, V) {})
	keys := make([]K, 0, sz)
	for k, v := range data {
		cache.Put(k, v)
		keys = append(keys, k)
	}
	for i := range keys {
		_, ok := cache.Get(keys[i])
		assert.True(b, ok)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ok := true
		var i int
		for pb.Next() {
			_, ok = cache.Get(keys[i%int(sz)])
			i++
		}
		assert.True(b, ok)
	})
	b.ReportAllocs()
}
