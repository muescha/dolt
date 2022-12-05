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
	"math/bits"
	"sync/atomic"
)

// pseudoLRU is a lock-free Least-Recently-Used approximation.
//
// Adapted from rust implementation (https://docs.rs/plru/latest/plru)
// Originally published by Sun Microsystems (US patent 5,353,425).
// Algorithm described in detail as "PLRUm" by Al-Zoubi et al:
// (https://dl.acm.org/doi/10.1145/986537.986601)
type pseudoLRU struct {
	blocks []uint64
	cursor uint64
}

const (
	all  uint64 = math.MaxUint64
	none uint64 = 0

	mod64mask  = uint64(64) - 1
	div64shift = uint64(6)
	mul64shift = div64shift
)

// touch marks index |i| as recently used.
func (p *pseudoLRU) touch(i uint64) {
	bit := uint64(1 << (i & mod64mask))
	for { // spin until we set the bit
		block := atomic.LoadUint64(&p.blocks[i>>div64shift])
		if atomic.CompareAndSwapUint64(&p.blocks[i>>div64shift], block, block|bit) {
			break
		}
	}
	// maintain invariant that not all bits are set
	atomic.CompareAndSwapUint64(&p.blocks[i>>div64shift], all, bit)
}

// evict returns a not-recently-used index |i|.
func (p *pseudoLRU) evict() (i uint64) {
	// round-robin to pick eviction block
	j := atomic.AddUint64(&p.cursor, 1)
	j = j % uint64(len(p.blocks)) // wrap cursor
	block := atomic.LoadUint64(&p.blocks[j])
	// evict first unset bit in |b|,
	// modulo 64 returns 0th bit in the
	// case where all bits in |b| are set
	k := uint64(bits.TrailingZeros64(^block)) & mod64mask
	i = k + (j << mul64shift)
	p.touch(i)
	return
}

func (p *pseudoLRU) capacity() uint64 {
	return uint64(len(p.blocks)) << mul64shift
}
