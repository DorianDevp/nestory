package nestory

// UnsafeDB exposes stable live pointers. The caller must provide exclusive
// access until Flush completes; validation errors do not roll mutations back.
type UnsafeDB[T Entity] struct {
	db *DB[T]
}

// Unsafe enters the live-pointer API without allocating.
func (db *DB[T]) Unsafe() UnsafeDB[T] { return UnsafeDB[T]{db: db} }

// Get returns the stable live pointer for id and marks its chunk dirty.
func (unsafe UnsafeDB[T]) Get(id int) (*T, error) {
	resource, ok := unsafe.db.resource(id)
	if !ok {
		return nil, ErrNotFound
	}

	if err := ensureCommittedOwnership(); err != nil {
		return nil, err
	}

	unsafe.db.store.markDirty(resource.chunk)
	return resource.item, nil
}

// All returns every stable live pointer and marks every chunk dirty.
func (unsafe UnsafeDB[T]) All() []*T {
	unsafe.db.store.markAllDirty()

	return unsafe.db.store.StorePointers()
}

// Create stages an entity for the next Flush without transaction isolation.
func (unsafe UnsafeDB[T]) Create(entity *T) { unsafe.db.queueCreate(entity) }

// Delete stages a live delete for the next Flush.
func (unsafe UnsafeDB[T]) Delete(id int) error { return unsafe.db.queueDelete(id) }

// Flush validates the complete live graph, applies unsafe deletes, and writes
// every dirty chunk.
func (unsafe UnsafeDB[T]) Flush() error { return flushRelations() }
