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

package archive

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/constraints"
)

const (
	iters   = 8
	threads = 24
)

func TestHarrisList(t *testing.T) {
	t.Run("string harrisList", func(t *testing.T) {
		data := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
		ll := newHarrisList[string](len(data) * iters)
		assert.Equal(t, len(data)*iters, ll.size())
		testHarrisList(t, ll, data)
	})
	t.Run("int harrisList", func(t *testing.T) {
		data := []int{1, 2, 3, 4, 5, 6, 7, 8}
		ll := newHarrisList[int](len(data) * iters)
		assert.Equal(t, len(data)*iters, ll.size())
		testHarrisList(t, ll, data)
	})
}

func TestConcurrentHarrisList(t *testing.T) {
	t.Run("string harrisList", func(t *testing.T) {
		testConcurrentHarrisList(t, interleavedStrings())
	})
	t.Run("int harrisList", func(t *testing.T) {
		testConcurrentHarrisList(t, interleavedInts())
	})
}

func testHarrisList[T constraints.Ordered](t *testing.T, ll *harrisList[T], data []T) {
	for i := 0; i < iters; i++ {
		// test insert, find
		for _, s := range data {
			ok := ll.insert(s)
			assert.True(t, ok)
			ok = ll.find(s)
			assert.True(t, ok)
		}

		// test delete
		for _, s := range data {
			ok := ll.delete(s)
			assert.True(t, ok)
			ok = ll.find(s)
			assert.False(t, ok)
		}

		// shuffle data and test again
		rand.Shuffle(len(data), func(i, j int) {
			data[i], data[j] = data[j], data[i]
		})
	}
}

func testConcurrentHarrisList[T constraints.Ordered](t *testing.T, data [][]T) {
	var ll = newHarrisList[T](len(data) * len(data[0]) * iters)
	var wg sync.WaitGroup

	wg.Add(len(data))
	for i := range data {
		var cp = data[i]
		go func() {
			wg.Done()
			wg.Wait()
			testHarrisList(t, ll, cp)
		}()
	}
}

func interleavedStrings() [][]string {
	data := make([][]string, threads)
	base := []string{
		"a", "b", "c", "d", "e", "f", "g", "h",
		"i", "j", "k", "l", "m", "n", "o", "p",
		"q", "r", "s", "t", "u", "v", "w", "x",
	}

	for i := range data {
		data[i] = make([]string, len(base))
		for j := range data[i] {
			data[i][j] = base[j] + base[i]
		}
	}
	return data
}

func interleavedInts() [][]int {
	data := make([][]int, threads)
	base := []int{
		1, 2, 3, 4, 5, 6, 7, 8,
		9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24,
	}

	for i := range data {
		data[i] = make([]int, len(base))
		for j := range data[i] {
			data[i][j] = base[j]*10 + base[i]
		}
	}
	return data
}
