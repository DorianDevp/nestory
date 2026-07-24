package nestory

import (
	"errors"
	"fmt"
	"reflect"
)

var ErrAlreadyExists = errors.New("nestory: entity already exists")

func (db *DB[T]) add(entity *T) {
	db.mu.Lock()
	defer db.mu.Unlock()

	resource := db.store.Append(*entity)
	id := (*resource.item).GetId()
	db.resById[id] = resource
	db.markChanged()
}

// Create persists entity and its new ownership subtree in one short
// transaction. Use Transaction to batch it with more work.
func (db *DB[T]) Create(entity *T) error {
	return db.Transaction(func(tx *Tx[T]) error {
		return tx.Create(entity)
	})
}

// Delete removes id and its complete owned subtree in one short transaction.
// Use Transaction and Join to combine it with operations on other entity types.
func (db *DB[T]) Delete(id int) error {
	return db.Transaction(func(tx *Tx[T]) error {
		return tx.Delete(id)
	})
}

// DeleteMany removes ids in one transaction and one durable WAL frame.
func (db *DB[T]) DeleteMany(ids []int) error {
	return db.Transaction(func(tx *Tx[T]) error {
		return tx.DeleteMany(ids)
	})
}

// DeleteByIndex removes every row matching an ordered index prefix. The prefix
// is resolved once; rows appended after that snapshot belong to a later state.
func (db *DB[T]) DeleteByIndex(indexName string, prefix []any) error {
	ids, err := db.idsByIndexPrefix(indexName, prefix)
	if err != nil {
		return err
	}

	return db.DeleteMany(ids)
}

func (db *DB[T]) idsByIndexPrefix(indexName string, prefix []any) ([]int, error) {
	participant := relationGraphParticipant(reflect.TypeFor[T]())
	if participant {
		graphMu.RLock()
		defer graphMu.RUnlock()
	}

	db.structureMu.RLock()
	defer db.structureMu.RUnlock()
	db.mu.RLock()
	defer db.mu.RUnlock()

	index := db.secondary[indexName]
	if index == nil {
		return nil, fmt.Errorf("nestory: index %q does not exist", indexName)
	}

	entries, err := index.rangePrefix(prefix)
	if err != nil {
		return nil, err
	}

	ids := make([]int, len(entries))
	for position, entity := range entries {
		ids[position] = (*entity).GetId()
	}

	return ids, nil
}

// The queue remains an internal adapter for unsafe Flush while that path is
// intentionally live and non-transactional.
func (db *DB[T]) queueCreate(entity *T) {
	id := (*entity).GetId()
	if id == 0 {
		db.counter++
		setID(entity, db.counter)
		db.persistQueue = append(db.persistQueue, entity)
		return
	}

	if _, exists := db.resById[id]; exists {
		return
	}

	for _, queued := range db.persistQueue {
		if (*queued).GetId() == id {
			return
		}
	}

	db.persistQueue = append(db.persistQueue, entity)
}

func (db *DB[T]) resetPersistQueue() { db.persistQueue = []*T{} }

func (db *DB[T]) queueDelete(id int) error {
	resource, ok := db.resById[id]
	if !ok {
		return fmt.Errorf("%w: %s(%d)", ErrNotFound, db.name, id)
	}

	for _, queued := range db.deleteQueue {
		if (*queued).GetId() == id {
			return nil
		}
	}

	db.deleteQueue = append(db.deleteQueue, resource.item)
	return nil
}

func (db *DB[T]) resetDeleteQueue() { db.deleteQueue = []*T{} }
