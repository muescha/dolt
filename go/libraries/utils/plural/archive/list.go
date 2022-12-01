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
	"math"
	"sync/atomic"

	"golang.org/x/exp/constraints"
)

const (
	head ptr = 0
	tail ptr = math.MaxInt32
)

type ptr int32

type node[T constraints.Ordered] struct {
	value T
	next  ptr
}

// harrisList is a lock-free linked list by Timothy L. Harris
// "A Pragmatic Implementation of Non-Blocking Linked-Lists"
// (https://dl.acm.org/doi/10.5555/645958.676105)
type harrisList[T constraints.Ordered] struct {
	arena []node[T]
	bump  ptr
}

func newHarrisList[T constraints.Ordered](sz int) *harrisList[T] {
	if ptr(sz+1) >= tail {
		panic("too big") // todo
	}
	l := &harrisList[T]{
		arena: make([]node[T], sz+1),
		bump:  ptr(1), // node 0 is head
	}
	l.arena[head].next = tail
	return l
}

func (l *harrisList[T]) insert(value T) bool {
	//	public boolean List::insert (KeyType key) {
	//		Node *new_node = new Node(key);
	//		Node *right_node, *left_node;
	//		do {
	//			right_node = search (key, &left_node);
	//			if ((right_node != tail) && (right_node.key == key)) /*T1*/
	//			return false;
	//			new_node.next = right_node;
	//			if (CAS (&(left_node.next), right_node, new_node)) /*C2*/
	//			return true;
	//		} while (true); /*B3*/
	//	}

	novel, ok := l.allocate(value)
	if !ok {
		return false
	}

	var left, right ptr
	for {
		left, right = l.search(value)
		if right != tail && l.arena[right].value == value {
			return false
		}

		l.arena[novel].next = right
		if l.casNext(left, right, novel) {
			return true
		}
	}
}

func (l *harrisList[T]) delete(value T) bool {
	//	public boolean List::delete (KeyType search_key) {
	//		Node *right_node, *right_node_next, *left_node;
	//		do {
	//			right_node = search (search_key, &left_node);
	//			if ((right_node == tail) || (right_node.key != search_key)) /*T1*/
	//				return false;
	//			right_node_next = right_node.next;
	//			if (!is_marked_reference(right_node_next))
	//				if (CAS (&(right_node.next), /*C3*/
	//					right_node_next, get_marked_reference (right_node_next)))
	//					break;
	//		} while (true); /*B4*/
	//		if (!CAS (&(left_node.next), right_node, right_node_next)) /*C4*/
	//			right_node = search (right_node.key, &left_node);
	//		return true;
	//	}

	var left, right, rightnext ptr
	for {
		left, right = l.search(value)
		if right == tail || l.arena[right].value != value {
			return false
		}
		rightnext = l.loadNext(right)

		if !isMarked(rightnext) {
			// attempt to logically delete |right|
			if l.casNext(right, rightnext, getMarked(rightnext)) {
				break // marked right.next
			}
		}
	}
	// attempt to physically delete |right|
	if !l.casNext(left, right, rightnext) {
		// if we fail, cleanup marked nodes
		left, right = l.search(l.arena[right].value)
	}
	return true
}

func (l *harrisList[T]) find(value T) bool {
	//	public boolean List::find (KeyType search_key) {
	//		Node *right_node, *left_node;
	//		right_node = search (search_key, &left_node);
	//		if ((right_node == tail) ||
	//			(right_node.key != search_key))
	//		return false;
	//		else
	//		return true;
	//	}

	_, right := l.search(value)
	if right == tail || l.arena[right].value != value {
		return false
	}
	return true
}

func (l *harrisList[T]) search(value T) (ptr, ptr) {
	//	private Node *List::search (KeyType search_key, Node **left_node) {
	//		Node *left_node_next, *right_node;
	//
	//	search_again:
	//		do {
	//			Node *t = head;
	//			Node *t_next = head.next;
	//			/* 1: Find left_node and right_node */
	//			do {
	//				if (!is_marked_reference(t_next)) {
	//					(*left_node) = t;
	//					left_node_next = t_next;
	//				}
	//				t = get_unmarked_reference(t_next);
	//				if (t == tail) break;
	//				t_next = t.next;
	//			} while (is_marked_reference(t_next) || (t.key<search_key)); /*B1*/
	//			right_node = t;
	//			/* 2: Check nodes are adjacent */
	//			if (left_node_next == right_node)
	//				if ((right_node != tail) && is_marked_reference(right_node.next))
	//					goto search_again; /*G1*/
	//				else
	//					return right_node; /*R1*/
	//			/* 3: Remove one or more marked nodes */
	//			if (CAS (&(left_node.next), left_node_next, right_node)) /*C1*/
	//				if ((right_node != tail) && is_marked_reference(right_node.next))
	//					goto search_again; /*G2*/
	//				else
	//					return right_node; /*R2*/
	//		} while (true); /*B2*/
	//	}

	var left, leftnext, right ptr

SEARCH:
	for {
		cur, curnext := head, l.loadNext(head)

		// 1: find left and right
		for {
			if !isMarked(curnext) {
				left = cur
				leftnext = curnext
			}
			cur = getUnmarked(curnext)
			if cur == tail {
				break
			}
			curnext = l.loadNext(cur)

			if !isMarked(curnext) && l.arena[cur].value >= value {
				break // found left, right candidates
			}
		}
		right = cur

		// 2: check nodes are adjacent
		if leftnext == right {
			if right != tail && isMarked(l.loadNext(right)) {
				continue SEARCH
			} else {
				return left, right
			}
		}

		// 3: remove one or more marked nodes
		if l.casNext(left, leftnext, right) {
			if right != tail && isMarked(l.loadNext(right)) {
				continue SEARCH
			} else {
				return left, right
			}
		}
	}
}

func (l *harrisList[T]) allocate(value T) (ptr, bool) {
	var p ptr
	for {
		p = load32(&l.bump)
		if int(p) >= len(l.arena) {
			return tail, false // harrisList full
		}
		if cas32(&l.bump, p, p+1) {
			break // allocated node |p|
		}
	}
	l.arena[p].value = value
	return p, true
}

// loadNext returns node.next for node |p|.
func (l *harrisList[T]) loadNext(p ptr) ptr {
	return load32(&l.arena[p].next)
}

// casNext performs a CAS on node.next for node |p|.
func (l *harrisList[T]) casNext(p, old, new ptr) bool {
	return cas32(&l.arena[p].next, old, new)
}

func (l *harrisList[T]) size() int {
	return len(l.arena) - 1
}

func load32(addr *ptr) ptr {
	return ptr(atomic.LoadInt32((*int32)(addr)))
}

func cas32(addr *ptr, old, new ptr) bool {
	return atomic.CompareAndSwapInt32((*int32)(addr), int32(old), int32(new))
}

func isMarked(p ptr) bool {
	return p < 0
}

func getMarked(p ptr) ptr {
	if !isMarked(p) {
		return -p
	}
	return p
}

func getUnmarked(p ptr) ptr {
	if isMarked(p) {
		return -p
	}
	return p
}
