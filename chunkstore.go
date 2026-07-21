package nestory

import "sync"

// chunkStore is the backing store: entities live in fixed-capacity blocks that
// never grow, so every element address is stable for the store's life. That's
// what lets relations be real *T pointers without dangling as the set grows.
// Deletes are tombstones — values are never shifted in place.

// chunkLimit is the per-block count, baked into chunkBlock's array type so it must
// be a compile-time constant. 512 is the sweet spot in the sweep.
const chunkLimit = 512

// pointerStoreIterator lets fillRelation get a base's live []*T as `any`.
type pointerStoreIterator interface {
	iterateStorePointers() any
}

type resourceSlot[T any] struct {
	item     *T
	version  int
	chunk    int // owning block index; drives per-chunk dirty marking
	mu       sync.RWMutex
	updateMu sync.Mutex // serializes short UpdateWithin calls for this root
}

type chunkBlock[T any] struct {
	data [chunkLimit]T
	n    int
}

func (arr *chunkBlock[T]) Push(v T) bool {
	if arr.n >= len(arr.data) {
		return false
	}

	arr.data[arr.n] = v
	arr.n++

	return true
}

func (arr *chunkBlock[T]) Len() int {
	return arr.n
}

type chunkStore[T any] struct {
	chunks []*chunkBlock[resourceSlot[T]] // each block is heap-allocated, never moves
	tomb   []*chunkBlock[bool]            // tombstone flags, same shape as chunks
	n      int                            // total slots appended (incl. tombstoned)
	dead   int                            // tombstoned slots

	// dirty[i]: chunk i changed since last save. Guarded by dirtyMu because
	// commits mark dirty under per-row locks, not db.mu, so two could race.
	dirty   []bool
	dirtyMu sync.Mutex
}

func (s *chunkStore[T]) markDirty(ci int) {
	s.dirtyMu.Lock()
	if ci >= 0 && ci < len(s.dirty) {
		s.dirty[ci] = true
	}

	s.dirtyMu.Unlock()
}

func (s *chunkStore[T]) dirtyIndices() []int {
	s.dirtyMu.Lock()
	defer s.dirtyMu.Unlock()

	var out []int
	for i, d := range s.dirty {
		if d {
			out = append(out, i)
		}
	}

	return out
}

func (s *chunkStore[T]) clearDirty(idxs []int) {
	s.dirtyMu.Lock()
	for _, i := range idxs {
		if i >= 0 && i < len(s.dirty) {
			s.dirty[i] = false
		}
	}

	s.dirtyMu.Unlock()
}

// chunkLive calls fn for every live element of chunk ci.
func (s *chunkStore[T]) chunkLive(ci int, fn func(*T)) {
	ch := s.chunks[ci]
	tb := s.tomb[ci]
	for o := range ch.n {
		if !tb.data[o] {
			fn(ch.data[o].item)
		}
	}
}

func (s *chunkStore[T]) chunkSlots(ci int) int { return s.chunks[ci].n }

// loadChunk appends a freshly read block, keeping file ↔ chunk index alignment.
// The block is clean — it already matches disk.
func (s *chunkStore[T]) loadChunk(vals []T) {
	ch := &chunkBlock[resourceSlot[T]]{}
	tb := &chunkBlock[bool]{}
	ci := len(s.chunks)
	for i := range vals {
		v := vals[i]
		ch.data[ch.n] = resourceSlot[T]{item: &v, chunk: ci}
		ch.n++
		tb.data[tb.n] = false
		tb.n++
	}

	s.chunks = append(s.chunks, ch)
	s.tomb = append(s.tomb, tb)
	s.dirty = append(s.dirty, false)
	s.n += len(vals)
}

func newChunkStore[T any]() *chunkStore[T] {
	return &chunkStore[T]{}
}

// Append stores v and returns a stable pointer to its resourceSlot slot — safe to
// index by id elsewhere, since neither the resourceSlot nor its item ever moves.
func (s *chunkStore[T]) Append(v T) *resourceSlot[T] {
	if len(s.chunks) == 0 || s.chunks[len(s.chunks)-1].Len() == chunkLimit {
		s.chunks = append(s.chunks, &chunkBlock[resourceSlot[T]]{})
		s.tomb = append(s.tomb, &chunkBlock[bool]{})
		s.dirtyMu.Lock()
		s.dirty = append(s.dirty, false)
		s.dirtyMu.Unlock()
	}

	ci := len(s.chunks) - 1
	last := s.chunks[ci]
	last.Push(resourceSlot[T]{item: &v, chunk: ci})
	s.tomb[ci].Push(false)

	s.n++
	s.markDirty(ci)

	return &last.data[last.Len()-1]
}

// Len returns the number of live (non-tombstoned) elements.
func (s *chunkStore[T]) Len() int {
	return s.n - s.dead
}

// Chunks/Tombs expose the raw blocks for contiguous iteration. Callers must
// not append to the returned inner slices.
func (s *chunkStore[T]) Chunks() []*chunkBlock[resourceSlot[T]] {
	return s.chunks
}

func (s *chunkStore[T]) Tombs() []*chunkBlock[bool] {
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

// rangeResources calls fn for every live resourceSlot slot. Used to rebuild
// resById after a load.
func (s *chunkStore[T]) rangeResources(fn func(*resourceSlot[T])) {
	for c := range s.chunks {
		ch := s.chunks[c]
		tb := s.tomb[c]

		for o := range ch.n {
			if !tb.data[o] {
				fn(&ch.data[o])
			}
		}
	}
}

// StorePointers returns stable pointers to every live element.
func (s *chunkStore[T]) StorePointers() []*T {
	out := make([]*T, 0, s.Len())

	s.Range(func(p *T) {
		out = append(out, p)
	})

	return out
}

// iterateStorePointers returns a []*T boxed in an interface, for fillRelation to walk
// reflectively without touching this type's unexported fields.
func (s *chunkStore[T]) iterateStorePointers() any {
	return s.StorePointers()
}

// DeleteFunc tombstones every live element matching pred and returns them (so
// callers can clean up indexes). Values stay in place; pointers stay valid.
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
			s.markDirty(c)
			removed = append(removed, ch.data[o].item)
		}
	}

	return removed
}
