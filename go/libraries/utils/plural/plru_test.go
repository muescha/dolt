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
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testOp struct {
	touchIdx uint64
	evictIdx uint64
	evict    bool
}

func TestPseudoLRU(t *testing.T) {
	tests := []struct {
		name    string
		ops     []testOp
		initial uint64
		final   uint64
	}{
		{
			name: "smoke test",
		},
		{
			name: "touch zeroth bit",
			ops: []testOp{
				{touchIdx: 0},
			},
			final: uint64(0b0001),
		},
		{
			name: "touch zeroth bit twice",
			ops: []testOp{
				{touchIdx: 0},
				{touchIdx: 0},
			},
			final: uint64(0b0001),
		},
		{
			name: "touch first bit",
			ops: []testOp{
				{touchIdx: 1},
			},
			final: uint64(0b0010),
		},
		{
			name: "touch two bits",
			ops: []testOp{
				{touchIdx: 0},
				{touchIdx: 1},
			},
			final: uint64(0b0011),
		},
		{
			name: "evict zeroth bit",
			ops: []testOp{
				{evictIdx: 0, evict: true},
			},
			final: uint64(0b0001),
		},
		{
			name: "evict first bit",
			ops: []testOp{
				{touchIdx: 0},
				{evictIdx: 1, evict: true},
			},
			final: uint64(0b0011),
		},
		{
			name: "evict second bit",
			ops: []testOp{
				{touchIdx: 0},
				{touchIdx: 1},
				{evictIdx: 2, evict: true},
				{touchIdx: 3},
			},
			final: uint64(0b1111),
		},
		{
			name:    "touching zeroth bit resets block",
			initial: uint64(0xfffffffffffffffe),
			ops: []testOp{
				{touchIdx: 0},
			},
			final: uint64(0b0001),
		},
		{
			name:    "evicting first bit resets block",
			initial: uint64(0xfffffffffffffffd),
			ops: []testOp{
				{touchIdx: 0},
				{evictIdx: 1, evict: true},
			},
			final: uint64(0b0010),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lru := &pseudoLRU{
				blocks: []uint64{test.initial},
				cursor: 0,
			}
			for _, op := range test.ops {
				if op.evict {
					assert.Equal(t, op.evictIdx, lru.evict())
				} else {
					lru.touch(op.touchIdx)
					assert.True(t, bitIsSet(lru, op.touchIdx))
				}
			}
			assert.Equal(t, test.final, lru.blocks[0])
		})
	}
}

func TestConcurrentPLRU(t *testing.T) {
	sz := 64 * 64
	lru := &pseudoLRU{
		blocks: make([]uint64, sz/64),
	}

	n := runtime.NumCPU()
	wg := new(sync.WaitGroup)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			for j := 0; j < sz*1024; j++ {
				x := lru.evict()
				assert.True(t, int(x) < sz)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestBitMath(t *testing.T) {
	assert.True(t, all == ^none)
	for i := 0; i < 1024; i++ {
		j := uint64(rand.Int63n(math.MaxUint32))
		assert.Equal(t, j&mod64mask, j%64)
		assert.Equal(t, j>>div64shift, j/64)
		assert.Equal(t, j<<mul64shift, j*64)
	}
}

func bitIsSet(p *pseudoLRU, i uint64) bool {
	block := p.blocks[i>>div64shift]
	bit := uint64(1 << (i & mod64mask))
	return block&bit != 0
}
