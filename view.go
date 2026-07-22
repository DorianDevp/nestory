package nestory

import (
	"fmt"
	"reflect"
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
