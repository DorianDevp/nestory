package nestory

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
// at chunkLimit=1024 scans match a flat []T to within noise, while appends
// avoid the grow-and-recopy tax of a growing slice.

// chunkLimit is the per-block element count. 1024 was the sweet spot in the
// prototype: scan locality indistinguishable from []T, few allocations.
const chunkLimit = 1024

type chunkStore[T any] struct {
	chunks [][]T    // each inner slice has cap == limit, never reallocated
	tomb   [][]bool // parallel tombstone flags, same shape as chunks
	limit  int
	n      int // total slots appended (including tombstoned)
	dead   int // count of tombstoned slots
}

func newChunkStore[T any](limit int) *chunkStore[T] {
	if limit <= 0 {
		panic("nestory: chunk limit must be > 0")
	}
	return &chunkStore[T]{limit: limit}
}

// Append stores v and returns a stable pointer to its slot.
func (s *chunkStore[T]) Append(v T) *T {
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

// Len returns the number of live (non-tombstoned) elements.
func (s *chunkStore[T]) Len() int {
	return s.n - s.dead
}

// Chunks/Tombs expose the raw blocks for contiguous iteration. Callers must
// not append to the returned inner slices.
func (s *chunkStore[T]) Chunks() [][]T {
	return s.chunks
}

func (s *chunkStore[T]) Tombs() [][]bool {
	return s.tomb
}

// Range calls fn for every live element with a stable pointer to its slot.
func (s *chunkStore[T]) Range(fn func(p *T)) {
	for c := range s.chunks {
		ch := s.chunks[c]
		tb := s.tomb[c]

		for o := range ch {
			if !tb[o] {
				fn(&ch[o])
			}
		}
	}
}

// Live returns stable pointers to every live element.
func (s *chunkStore[T]) Live() []*T {
	out := make([]*T, 0, s.Len())
	s.Range(func(p *T) { out = append(out, p) })
	return out
}

// livePointers is the type-erased view used by fillRelation to walk another
// base's elements reflectively without touching this type's unexported
// fields. It returns a []*T boxed in an interface, which is fully
// reflection-friendly (unlike the chunkStore's own private fields).
func (s *chunkStore[T]) livePointers() any { return s.Live() }

// pointerLister is implemented by every chunkStore[T]; it lets generic,
// cross-type code (fillRelation) obtain a base's live []*T as `any`.
type pointerLister interface {
	livePointers() any
}

// DeleteFunc tombstones every live element matching pred and returns the
// pointers that were tombstoned (so callers can clean up indexes). Values are
// left in place; existing pointers to them stay valid.
func (s *chunkStore[T]) DeleteFunc(pred func(*T) bool) []*T {
	var removed []*T
	for c := range s.chunks {
		ch := s.chunks[c]
		for o := range ch {
			if !s.tomb[c][o] && pred(&ch[o]) {
				s.tomb[c][o] = true
				s.dead++
				removed = append(removed, &ch[o])
			}
		}
	}
	return removed
}
