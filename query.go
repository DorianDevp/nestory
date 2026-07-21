package nestory

import (
	"fmt"
	"reflect"
	"sync"
)

// Look by indices: exists && empty -> nil
// routines through chunks
func (db *DB[T]) FindOneBy(key string, withValue any) (*T, error) {
	if db.store.Len() == 0 {
		return nil, ErrEmptyEntity
	}

	indexMap := db.index[key]
	if indexMap != nil {
		if val, ok := indexMap[withValue]; ok {
			return val, nil
		} else {
			return nil, nil
		}
	}

	chunks := db.store.Chunks()
	tombs := db.store.Tombs()

	needle := make(chan *Resource[T], 1)

	var wg sync.WaitGroup

	wg.Add(len(chunks))

	for chunkIdx := range chunks {
		go func(chunkIdx int) {
			defer wg.Done()

			chunk := chunks[chunkIdx]
			tomb := tombs[chunkIdx]

			for i := range chunk.n {
				if tomb.data[i] {
					continue
				}

				s := &chunk.data[i]

				val := reflect.ValueOf(s).Elem()
				for val.Kind() == reflect.Pointer || val.Kind() == reflect.Interface {
					if val.IsNil() {
						break
					}

					val = val.Elem()
				}

				if val.Kind() != reflect.Struct {
					continue
				}

				f := val.FieldByName(key)
				if f.IsValid() && f.Interface() == withValue {
					s.mu.RLock()
					needle <- s

					return
				}
			}
		}(chunkIdx)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case item := <-needle:
		item.mu.RUnlock()
		return item.item, nil
	case <-done:
		return nil, nil
	}
}

// Filter returns every live entity for which filterFn returns true (by value).
func (db *DB[T]) Filter(filterFn func(T) bool) []T {
	var filteredSlice []T
	db.store.Range(func(p *T) {
		if filterFn(*p) {
			filteredSlice = append(filteredSlice, *p)
		}
	})
	return filteredSlice
}

// FilterPtr is like Filter but returns stable *T pointers into the backing
// store. Returns an error if the result is empty — callers depend on this for
// "no rows" detection.
func (db *DB[T]) FilterPtr(filterFn func(*T) bool) ([]*T, error) {
	var filteredSlice []*T
	db.store.Range(func(p *T) {
		if filterFn(p) {
			filteredSlice = append(filteredSlice, p)
		}
	})

	if len(filteredSlice) == 0 {
		return filteredSlice, fmt.Errorf("empty array")
	}

	return filteredSlice, nil
}

// These open and commit transactions through the Engine; the machinery they
// drive lives in tx.go and engine.go.

// Get returns a detached branch containing id and its complete ownership
// subtree. Mutate any owned node through normal pointers, then hand the root to
// Update. Non-owning relations outside the branch remain read-only live links.
func (db *DB[T]) Get(id int) (*T, error) {
	tx := engine.begin()
	work, err := db.getInTransaction(tx, id)
	if err != nil {
		engine.evict(tx)
		return nil, err
	}

	return work, nil
}

func (db *DB[T]) getInTransaction(tx txId, id int) (*T, error) {
	if existing, ok := engine.work(tx, db.name, id); ok {
		return existing.(*T), nil
	}

	root := nodeKey{typ: reflect.TypeFor[T](), id: id}
	work, original, versions, err := cloneOwnershipAggregate(root)
	if err != nil {
		return nil, err
	}

	for key, value := range work {
		if _, ok := baseRegistry[key.typ.Name()].(committer); !ok {
			return nil, fmt.Errorf("nestory: %s is not open", key.typ)
		}

		engine.record(tx, touchedResource{
			dbName: key.typ.Name(), id: key.id, ver: versions[key],
			work: value.Interface(), original: original[key].Interface(),
		})
	}

	return work[root].Interface().(*T), nil
}

// UnsafeGet returns the live, stable store pointer for id and marks its chunk
// dirty so a later Flush persists direct mutations. It performs no snapshot,
// copy, transaction bookkeeping, row locking, or rollback.
//
// The caller must provide exclusive access until Flush completes. Mutations are
// visible immediately, even when Flush later rejects the relation graph. After
// such an error, repair the live value and call Flush again.
func (db *DB[T]) UnsafeGet(id int) (*T, error) {
	resource, ok := db.resource(id)
	if !ok {
		return nil, ErrNotFound
	}
	if err := ensureCommittedOwnership(); err != nil {
		return nil, err
	}

	db.store.markDirty(resource.chunk)

	return resource.item, nil
}

// Update commits the transaction that produced work. On conflict it returns
// ErrConflict and refreshes work in place to live state, so the caller can
// re-apply and call Update again.
func (db *DB[T]) Update(work *T) error {
	return engine.commitByPtr(work)
}

// UpdateWithin runs fn against a fresh snapshot of id and commits, retrying on
// conflict. fn must express intent (c.N++), not absolute values from a stale
// read — it re-runs against the refreshed snapshot on each retry.
func (db *DB[T]) UpdateWithin(id int, fn func(*T)) error {
	work, err := db.Get(id)
	if err != nil {
		return err
	}

	for {
		fn(work)

		err := db.Update(work)
		if err != ErrConflict {
			return err
		}
	}
}
