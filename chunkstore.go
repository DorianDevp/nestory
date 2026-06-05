package nestory

import "sync"

// chunkStore is nestory's backing store: entities live in a sequence of
// fixed-capacity []T blocks that are never grown past their reserved
// capacity, so every element's address is stable for the life of the store.
// This is what lets relations be real *T pointers (the nestory headline)
// without dangling when the dataset grows.
//
// Deletes are tombstones — values are never shifted in place (a shift would
// move elements and dangle pointers). Live elements are surfaced via Live()
// for relation wiring, and via Chunks()/Tombs() for tight, contiguous scans.
//
// See bench/chunkstore for the prototype + benchmarks this is derived from:
// at chunkLimit count scans match a flat []T to within noise, while appends
// avoid the grow-and-recopy tax of a growing slice.

// chunkLimit is the per-block element count, baked into chArray's array type
// (so it MUST be a compile-time constant). 512 is the sweet spot in the sweep:
// vs 128 it cuts append allocations ~3.5x and scans ~10% faster, while keeping
// the per-base memory waste (one partial trailing block) modest.
const chunkLimit = 512

// pointerLister is implemented by every chunkStore[T]; it lets generic,
// cross-type code (fillRelation) obtain a base's live []*T as `any`.
type pointerLister interface {
	livePointers() any
}

type Resource[T any] struct {
	item *T
	version int
	mu sync.RWMutex
}

type chArray[T any] struct {
	data [chunkLimit]T
	n int
}

func (arr *chArray[T]) Push(v T) bool {
      if arr.n >= len(arr.data) {
          return false
      }

      arr.data[arr.n] = v
      arr.n++

      return true
}

func (arr *chArray[T]) Len() int {
	return arr.n
}

type chunkStore[T any] struct {
	chunks []*chArray[Resource[T]]    // each block is heap-allocated; the block's address never moves
	tomb   []*chArray[bool] // parallel tombstone flags, same shape as chunks
	n      int              // total slots appended (including tombstoned)
	dead   int              // count of tombstoned slots
}

func newChunkStore[T any]() *chunkStore[T] {
	return &chunkStore[T]{}
}

// Append stores v and returns a stable pointer to its slot.
func (s *chunkStore[T]) Append(v T) *T {
	if len(s.chunks) == 0 || s.chunks[len(s.chunks)-1].Len() == chunkLimit {
		s.chunks = append(s.chunks, &chArray[Resource[T]]{})
		s.tomb = append(s.tomb, &chArray[bool]{})
	}

	last := s.chunks[len(s.chunks)-1]
	last.Push(Resource[T]{ item: &v })
	s.tomb[len(s.tomb)-1].Push(false)

	s.n++

	return last.data[last.Len()-1].item
}

// Len returns the number of live (non-tombstoned) elements.
func (s *chunkStore[T]) Len() int {
	return s.n - s.dead
}

// Chunks/Tombs expose the raw blocks for contiguous iteration. Callers must
// not append to the returned inner slices.
func (s *chunkStore[T]) Chunks() []*chArray[Resource[T]] {
	return s.chunks
}

func (s *chunkStore[T]) Tombs() []*chArray[bool] {
	return s.tomb
}

func (s *chunkStore[T]) Empty() bool {
	return len(s.chunks) == 0
}

// Range calls fn for every live element with a stable pointer to its slot.
func (s *chunkStore[T]) Range(fn func(p *T)) {
	for c := range s.chunks {
		ch := s.chunks[c]
		tb := s.tomb[c]

		for o := range ch.n {
			if !tb.data[o] {
				fn(ch.data[o].item)
			}
		}
	}
}

// Live returns stable pointers to every live element.
func (s *chunkStore[T]) Live() []*T {
	out := make([]*T, 0, s.Len())

	s.Range(func(p *T) { 
		out = append(out, p) 
	})

	return out
}

// livePointers is the type-erased view used by fillRelation to walk another
// base's elements reflectively without touching this type's unexported
// fields. It returns a []*T boxed in an interface, which is fully
// reflection-friendly (unlike the chunkStore's own private fields).
func (s *chunkStore[T]) livePointers() any { 
	return s.Live() 
}

// DeleteFunc tombstones every live element matching pred and returns the
// pointers that were tombstoned (so callers can clean up indexes). Values are
// left in place; existing pointers to them stay valid.
func (s *chunkStore[T]) DeleteFunc(pred func(*T) bool) []*T {
	var removed []*T

	for c := range s.chunks {
		ch := s.chunks[c]

		for o := range ch.n {
			if s.tomb[c].data[o] || !pred(ch.data[o].item) {
				continue
			}

			s.tomb[c].data[o] = true
			s.dead++
			removed = append(removed, ch.data[o].item)
		}
	}

	return removed
}
