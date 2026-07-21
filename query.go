package nestory

import (
	"reflect"
)

// FindOneBy returns a detached branch for the first matching entity.
func (db *DB[T]) FindOneBy(field string, value any) (*T, error) {
	graphMu.RLock()
	id, err := db.findID(field, value)
	graphMu.RUnlock()
	if err != nil {
		return nil, err
	}

	return db.Get(id)
}

func (db *DB[T]) findID(field string, value any) (int, error) {
	if index := db.index[field]; index != nil {
		entity, found := index[value]
		if !found {
			return 0, ErrNotFound
		}

		return (*entity).GetId(), nil
	}

	var id int
	db.store.Range(func(entity *T) {
		if id != 0 {
			return
		}

		candidate := reflect.ValueOf(entity).Elem().FieldByName(field)
		if candidate.IsValid() && reflect.DeepEqual(candidate.Interface(), value) {
			id = (*entity).GetId()
		}
	})
	if id == 0 {
		return 0, ErrNotFound
	}

	return id, nil
}

// Filter returns detached root values matching filterFn. Use Transaction for
// writable ownership branches.
func (db *DB[T]) Filter(filterFn func(T) bool) []T {
	graphMu.RLock()
	defer graphMu.RUnlock()

	var filtered []T
	db.store.Range(func(entity *T) {
		if filterFn(*entity) {
			filtered = append(filtered, *entity)
		}
	})
	return filtered
}

// Get returns a detached branch containing id and its complete ownership
// subtree. Mutate owned nodes and hand the root to Update to merge the branch.
func (db *DB[T]) Get(id int) (*T, error) {
	root := nodeKey{typ: reflect.TypeFor[T](), id: id}
	if work, handled, err := db.getDetachedRoot(root); handled {
		return work, err
	}

	tx := engine.begin()
	work, err := db.getInTransaction(tx, id)
	if err != nil {
		engine.evict(tx)
		return nil, err
	}

	return work, nil
}

func (db *DB[T]) getInTransaction(tx *transactionState, id int) (*T, error) {
	if existing, ok := engine.work(tx, db.name, id); ok {
		return existing.(*T), nil
	}

	root := nodeKey{typ: reflect.TypeFor[T](), id: id}
	rootWork, err := cloneOwnershipAggregate(root, func(resource touchedResource) {
		engine.record(tx, resource)
	})
	if err != nil {
		return nil, err
	}

	return rootWork.(*T), nil
}

// Update merges the detached ownership branch returned by Get.
func (db *DB[T]) Update(branch *T) error {
	db.promoteDetachedRoot(branch)

	return engine.commitByPtr(branch)
}

// UpdateWithin retries a short transaction on conflict. fn may run more than
// once and must therefore express intent without external side effects.
func (db *DB[T]) UpdateWithin(id int, fn func(*T) error) error {
	resource, found := db.resource(id)
	if !found {
		return ErrNotFound
	}

	resource.updateMu.Lock()
	defer resource.updateMu.Unlock()

	for {
		err := db.Transaction(func(tx *Tx[T]) error {
			return tx.UpdateWithin(id, fn)
		})
		if err != ErrConflict {
			return err
		}
	}
}
