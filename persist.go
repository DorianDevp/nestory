package nestory

import (
	"errors"
	"fmt"
)

var ErrAlreadyExists = errors.New("nestory: entity already exists")

func (db *DB[T]) add(entity *T) {
	db.mu.Lock()
	defer db.mu.Unlock()

	resource := db.store.Append(*entity)
	id := (*resource.item).GetId()
	db.index["Id"][id] = resource.item
	db.resById[id] = resource
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
	if existing := db.index[db.identifier][id]; existing != nil {
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
	instance, ok := db.index["Id"][id]
	if !ok {
		return fmt.Errorf("%w: %s(%d)", ErrNotFound, db.name, id)
	}
	for _, queued := range db.deleteQueue {
		if (*queued).GetId() == id {
			return nil
		}
	}
	db.deleteQueue = append(db.deleteQueue, instance)
	return nil
}

func (db *DB[T]) resetDeleteQueue() { db.deleteQueue = []*T{} }
