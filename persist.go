package nestory

import (
	"fmt"
	"log"
	"reflect"
)

// Add appends entity and points the Id index at the store's stable slot, not
// the caller's pointer. Used by Flush; prefer AddToPersistQueue + Flush.
func (db *DB[T]) Add(entity *T) {
	db.mu.Lock()
	defer db.mu.Unlock()

	res := db.store.Append(*entity)
	id := (*res.item).GetId()
	db.index["Id"][id] = res.item
	db.resById[id] = res
}

// AddToPersistQueue stages entity for the next Flush. Zero Id → auto-assigned.
// An explicit Id that already exists (persisted or queued) is a no-op.
func (db *DB[T]) AddToPersistQueue(entity *T) {
	id := (*entity).GetId()
	if id == 0 {
		db.counter += 1
		SetId(entity, db.counter)
		db.persistQueue = append(db.persistQueue, entity)

		return
	}

	// explicit Id — accept it, but dedupe against persisted + queued.
	if existing := db.index[db.identifier][id]; existing != nil {
		return
	}

	for _, q := range db.persistQueue {
		if (*q).GetId() == id {
			return
		}
	}

	db.persistQueue = append(db.persistQueue, entity)
}

// ResetpersistQueue empties the persist queue without saving.
func (db *DB[T]) ResetpersistQueue() {
	db.persistQueue = []*T{}
}

// QueueDelete marks the entity with id for removal on next Flush. No-op if queued.
func (db *DB[T]) QueueDelete(id any) error {
	instance, ok := db.index["Id"][id]
	if !ok {
		return fmt.Errorf("There is no item with given Id %d\n", id)
	}

	for _, q := range db.deleteQueue {
		if (*q).GetId() == id {
			return nil
		}
	}

	db.deleteQueue = append(db.deleteQueue, instance)

	return nil
}

func (db *DB[T]) resetDeleteQueue() {
	db.deleteQueue = []*T{}
}

// PatchById applies non-zero, non-primary fields from item onto the instance
// with id.
func (db *DB[T]) PatchById(id any, item T) (*T, error) {
	baseInstance, ok := db.index["Id"][id]
	if !ok {
		return nil, fmt.Errorf("There is no item with given Id %d\n", id)
	}

	if err := merge(baseInstance, item); err != nil {
		return nil, err
	}

	if r, ok := db.resById[(*baseInstance).GetId()]; ok {
		db.store.markDirty(r.chunk)
	}

	return baseInstance, nil
}

// Flush applies the persist queue (inserts or patches), then the delete queue,
// then writes every dirty chunk to disk.
func (db *DB[T]) Flush() error {
	// Persisted-id set once (O(n)) so insert-vs-patch is O(1) per queued item.
	existing := make(map[int]bool, db.store.Len())
	db.store.Range(func(p *T) { existing[(*p).GetId()] = true })

	for _, ent := range db.persistQueue {
		id := (*ent).GetId()
		if existing[id] {
			db.PatchById(id, *ent)
			continue
		}
		db.Add(ent)
		existing[id] = true
	}

	db.ResetpersistQueue()
	// No syncIdIndex: Add maintains Index["Id"] and the delete loop drops ids.

	// Deletes tombstone in place (no shift → pointers stay valid). Drop the index too.
	for _, instance := range db.deleteQueue {
		delId := (*instance).GetId()
		db.store.DeleteFunc(func(p *T) bool { return (*p).GetId() == delId })
		delete(db.index["Id"], delId)
		delete(db.resById, delId)
	}

	db.resetDeleteQueue()

	if err := db.save(); err != nil {
		log.Panicln("Panic during saving a base", db.TypeName(), err)
	}

	// Compaction folded every WAL'd change into the rewritten chunks, so drop it.
	if db.wal != nil {
		if err := db.wal.truncate(); err != nil {
			log.Panicln("Panic during WAL truncate", db.TypeName(), err)
		}
	}

	return nil
}

// merge copies non-zero fields from merger into target, skipping key:"primary".
func merge[T any](target *T, merger T) error {
	targetVal := reflect.ValueOf(target).Elem()
	mergerVal := reflect.ValueOf(merger)
	mergerType := reflect.TypeOf(merger)

	if targetVal.Kind() != reflect.Struct || mergerVal.Kind() != reflect.Struct {
		return fmt.Errorf("merge: target and merger must be structs")
	}

	for i := range mergerVal.NumField() {
		mergerField := mergerVal.Field(i)
		mergerFieldType := mergerType.Field(i)

		targetField := targetVal.FieldByName(mergerFieldType.Name)
		fieldKeyType := mergerFieldType.Tag.Get("key")

		if fieldKeyType == "primary" || !targetField.CanSet() {
			continue
		}

		if targetField.Kind() == reflect.Ptr && !mergerField.IsNil() {
			targetField.Set(mergerField)
			continue
		}

		if !mergerField.IsZero() {
			targetField.Set(mergerField)
		}
	}

	return nil
}
