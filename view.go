package nestory

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
)

// View runs fn against the stable live pointer for id without cloning its
// ownership tree. The tree is read-locked for the callback. References outside
// that tree are navigable but are not covered by the same consistency window.
// Mutating or retaining the pointer for synchronized use after fn returns
// violates the View contract; use Transaction or Unsafe for writes.
func (db *DB[T]) View(id int, fn func(*T) error) error {
	if fn == nil {
		return fmt.Errorf("nestory: View requires a callback")
	}

	if !relationGraphParticipant(reflect.TypeFor[T]()) {
		resource, found := db.resource(id)
		if !found {
			return ErrNotFound
		}

		resource.mu.RLock()
		defer resource.mu.RUnlock()

		return fn(resource.item)
	}

	graphMu.RLock()
	defer graphMu.RUnlock()

	if err := ensureCommittedOwnership(); err != nil {
		return err
	}

	root, found := db.resource(id)
	if !found {
		return ErrNotFound
	}

	// One acquisition for the whole branch. Locking every row instead cost 112
	// atomics across as many cache lines, two thirds of a graph read.
	branch := lockBranchForRead(reflect.TypeFor[T](), id)
	defer branch.RUnlock()

	return fn(root.item)
}

// ViewMany exposes stable live pointers for ids during fn without cloning.
// IDs are locked in ascending order. The pointers are read-only by contract
// and must not be retained for synchronized use after fn returns.
func (db *DB[T]) ViewMany(ids []int, fn func([]*T) error) error {
	if fn == nil {
		return fmt.Errorf("nestory: ViewMany requires a callback")
	}

	participant := relationGraphParticipant(reflect.TypeFor[T]())
	if participant {
		graphMu.RLock()
		defer graphMu.RUnlock()
	}

	db.structureMu.RLock()
	defer db.structureMu.RUnlock()

	ordered := append([]int(nil), ids...)
	slices.Sort(ordered)
	ordered = slices.Compact(ordered)
	resources := make([]*resourceSlot[T], len(ordered))
	locked := 0
	defer func() {
		for position := locked - 1; position >= 0; position-- {
			resources[position].mu.RUnlock()
		}
	}()

	for position, id := range ordered {
		resource, found := db.resource(id)
		if !found {
			return ErrNotFound
		}

		resources[position] = resource
		resource.mu.RLock()
		locked++
	}

	entities := make([]*T, len(resources))
	for position, resource := range resources {
		entities[position] = resource.item
	}

	return fn(entities)
}

// ViewRange visits an ordered index prefix. For an index on (SessionID, Seq),
// prefix []any{sessionID} returns one session in Seq order. Returned pointers
// obey the same callback-only read contract as ViewMany.
func (db *DB[T]) ViewRange(indexName string, prefix []any, fn func([]*T) error) error {
	if fn == nil {
		return fmt.Errorf("nestory: ViewRange requires a callback")
	}

	return db.viewRange(indexName, prefix, nil, false, fn)
}

// ViewRangeAfter visits the part of an ordered index prefix whose next index
// field is strictly greater than after. For an index on (SessionID, Seq), a
// prefix containing SessionID and an after Seq form an efficient delta cursor.
func (db *DB[T]) ViewRangeAfter(
	indexName string,
	prefix []any,
	after any,
	fn func([]*T) error,
) error {
	if fn == nil {
		return fmt.Errorf("nestory: ViewRangeAfter requires a callback")
	}

	return db.viewRange(indexName, prefix, after, true, fn)
}

func (db *DB[T]) viewRange(
	indexName string,
	prefix []any,
	after any,
	hasCursor bool,
	fn func([]*T) error,
) error {
	participant := relationGraphParticipant(reflect.TypeFor[T]())
	if participant {
		graphMu.RLock()
		defer graphMu.RUnlock()
	}

	db.structureMu.RLock()
	defer db.structureMu.RUnlock()
	db.mu.RLock()
	index := db.secondary[indexName]
	if index == nil {
		db.mu.RUnlock()

		return fmt.Errorf("nestory: index %q does not exist", indexName)
	}

	var entries []*T
	var err error
	if hasCursor {
		entries, err = index.rangeAfter(prefix, after)
	} else {
		entries, err = index.rangePrefix(prefix)
	}

	if err != nil {
		db.mu.RUnlock()

		return err
	}

	entities := append([]*T(nil), entries...)
	db.mu.RUnlock()
	locks := append([]*T(nil), entities...)
	slices.SortFunc(locks, func(left, right *T) int {
		return cmp.Compare((*left).GetId(), (*right).GetId())
	})
	resources := make([]*resourceSlot[T], len(locks))
	locked := 0
	defer func() {
		for position := locked - 1; position >= 0; position-- {
			resources[position].mu.RUnlock()
		}
	}()

	for position, entity := range locks {
		resource, found := db.resource((*entity).GetId())
		if !found {
			return ErrNotFound
		}

		resources[position] = resource
		resource.mu.RLock()
		locked++
	}

	return fn(entities)
}
