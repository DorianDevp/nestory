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

	keys := committedOwnershipKeys(nodeKey{typ: reflect.TypeFor[T](), id: id})
	root, found := db.resource(id)
	if !found {
		return ErrNotFound
	}

	for _, key := range keys {
		committerFor(key.typ.Name()).readLockResource(key.id)
	}

	defer func() {
		for i := len(keys) - 1; i >= 0; i-- {
			committerFor(keys[i].typ.Name()).readUnlockResource(keys[i].id)
		}
	}()

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

	entries, err := index.rangePrefix(prefix)
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
