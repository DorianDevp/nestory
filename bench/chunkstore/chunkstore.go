// Package chunkstore is a prototype of the chunked-arena storage discussed
// for nestory: data lives in a sequence of fixed-capacity []T blocks that are
// never grown past their reserved capacity, so element addresses are stable
// for the life of the store. This preserves "relation === pointer" while
// keeping []T cache locality within each chunk.
//
// Deletes are tombstones (no in-place shift, which would move elements and
// dangle pointers). Compaction is offered separately and DOES invalidate
// pointers — it is meant to run only at a rebuild point.
package chunkstore

// ChunkStore holds T values in fixed-capacity chunks. A pointer returned by
// Append (or At) stays valid until Compact or the store itself is dropped.
type ChunkStore[T any] struct {
	chunks [][]T    // each inner slice has cap == limit, never reallocated
	tomb   [][]bool // parallel tombstone flags, same shape as chunks
	limit  int
	n      int // total slots appended (including tombstoned)
	dead   int // count of tombstoned slots
}

// New returns a store whose chunks each hold up to limit elements.
func New[T any](limit int) *ChunkStore[T] {
	if limit <= 0 {
		panic("chunkstore: limit must be > 0")
	}
	return &ChunkStore[T]{limit: limit}
}

// Append stores v and returns a STABLE pointer to its slot.
func (s *ChunkStore[T]) Append(v T) *T {
	// Start a new chunk if there is none or the active one is full. Because
	// the active chunk was made with cap == limit, appends into it below
	// never reallocate, so existing element pointers stay valid.
	if len(s.chunks) == 0 || len(s.chunks[len(s.chunks)-1]) == s.limit {
		s.chunks = append(s.chunks, make([]T, 0, s.limit))
		s.tomb = append(s.tomb, make([]bool, 0, s.limit))
	}
	ci := len(s.chunks) - 1
	s.chunks[ci] = append(s.chunks[ci], v)
	s.tomb[ci] = append(s.tomb[ci], false)
	s.n++
	return &s.chunks[ci][len(s.chunks[ci])-1]
}

// At returns a stable pointer to the i-th slot (including tombstoned ones).
func (s *ChunkStore[T]) At(i int) *T {
	return &s.chunks[i/s.limit][i%s.limit]
}

// Delete tombstones slot i. The value stays in place; pointers to it remain
// valid but the slot is skipped by Range and Len-of-live accounting.
func (s *ChunkStore[T]) Delete(i int) {
	c, o := i/s.limit, i%s.limit
	if !s.tomb[c][o] {
		s.tomb[c][o] = true
		s.dead++
	}
}

// IsDead reports whether slot i is tombstoned.
func (s *ChunkStore[T]) IsDead(i int) bool { return s.tomb[i/s.limit][i%s.limit] }

// Len returns the number of live (non-tombstoned) elements.
func (s *ChunkStore[T]) Len() int { return s.n - s.dead }

// Cap returns the total number of slots, live plus tombstoned.
func (s *ChunkStore[T]) Cap() int { return s.n }

// Chunks exposes the raw chunk slices for tight, allocation-free iteration.
// Callers must not append to the returned inner slices.
func (s *ChunkStore[T]) Chunks() [][]T { return s.chunks }

// Tombs exposes the parallel tombstone flags, aligned to Chunks.
func (s *ChunkStore[T]) Tombs() [][]bool { return s.tomb }

// Range calls fn for every live element with its global index. fn receives a
// stable pointer into the store.
func (s *ChunkStore[T]) Range(fn func(i int, p *T)) {
	for c := range s.chunks {
		ch := s.chunks[c]
		tb := s.tomb[c]
		base := c * s.limit
		for o := range ch {
			if !tb[o] {
				fn(base+o, &ch[o])
			}
		}
	}
}

// Compact returns a fresh store containing only live elements, densely
// packed. WARNING: pointers into the old store are NOT carried over and must
// be treated as invalid after this call — run it only where the whole graph
// is rebuilt (e.g. on load).
func (s *ChunkStore[T]) Compact() *ChunkStore[T] {
	out := New[T](s.limit)
	s.Range(func(_ int, p *T) { out.Append(*p) })
	return out
}
