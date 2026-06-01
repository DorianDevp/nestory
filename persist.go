package nestory

import (
	"fmt"
	"log"
	"reflect"
)

// Add appends entity to the backing store and points the Id index at the
// store's stable slot (NOT the caller's pointer). Used internally by Flush;
// rarely needed by callers — prefer [DB.AddToPersistQueue] + [DB.Flush].
func (gb *DB[T]) Add(entity *T) {
	gb.mu.Lock()
	defer gb.mu.Unlock()

	slot := gb.store.Append(*entity)
	gb.Index["Id"][(*slot).GetId()] = slot
}

// AddToPersistQueue stages entity for the next Flush. If entity.Id is zero,
// nestory auto-assigns one based on max(persisted, queued) + 1. If you
// supply an explicit Id that already exists in either the persisted set
// or the queue, the call is a no-op.
func (gb *DB[T]) AddToPersistQueue(entity *T) error {
	id := (*entity).GetId()
	dbIdentifier := gb.Identifier

	// Caller supplied an Id explicitly — accept it, but dedupe.
	if id != 0 {
		if existing := gb.Index[dbIdentifier][id]; existing != nil {
			return nil
		}

		for _, q := range gb.persistQueue {
			if (*q).GetId() == id {
				return nil
			}
		}

		gb.persistQueue = append(gb.persistQueue, entity)

		return nil
	}

	gb.counter += 1
	nextId := gb.counter

	SetId(entity, nextId)
	gb.persistQueue = append(gb.persistQueue, entity)

	return nil
}

// ResetpersistQueue empties the persist queue without saving. Used after
// a successful Flush; exposed for testing.
func (gb *DB[T]) ResetpersistQueue() {
	gb.persistQueue = []*T{}
}

// QueueDelete marks the entity with the given Id for removal on next Flush.
// No-op if already queued.
func (gb *DB[T]) QueueDelete(id any) error {
	instance, ok := gb.Index["Id"][id]
	if !ok {
		return fmt.Errorf("There is no item with given Id %d\n", id)
	}
	for _, q := range gb.deleteQueue {
		if (*q).GetId() == id {
			return nil
		}
	}
	gb.deleteQueue = append(gb.deleteQueue, instance)
	return nil
}

func (gb *DB[T]) resetDeleteQueue() {
	gb.deleteQueue = []*T{}
}

// PatchById applies non-zero, non-primary fields from item onto the
// existing instance with the given Id.
func (gb *DB[T]) PatchById(id any, item T) (*T, error) {
	baseInstance, ok := gb.Index["Id"][id]
	if !ok {
		return nil, fmt.Errorf("There is no item with given Id %d\n", id)
	}

	if err := merge(baseInstance, item); err != nil {
		return nil, err
	}

	return baseInstance, nil
}

// Flush applies the persist queue (inserts or patches), then the delete
// queue, then writes the resulting slice atomically to disk.
func (gb *DB[T]) Flush() error {
	// Build the set of already-persisted ids once (O(n)) so the per-item
	// insert-vs-patch decision is O(1), instead of a full scan per queued
	// item (which, via the old goroutine-spawning FindOneBy, made Flush
	// O(items x rows)).
	existing := make(map[int]bool, gb.store.Len())
	gb.store.Range(func(p *T) { existing[(*p).GetId()] = true })

	for _, ent := range gb.persistQueue {
		id := (*ent).GetId()
		if existing[id] {
			gb.PatchById(id, *ent)
			continue
		}
		gb.Add(ent)
		existing[id] = true
	}

	gb.ResetpersistQueue()
	gb.syncIdIndex()

	// Deletes are tombstones in the backing store (no in-place shift, which
	// would move elements and dangle pointers). Drop the Id index entry too.
	for _, instance := range gb.deleteQueue {
		delId := (*instance).GetId()
		gb.store.DeleteFunc(func(p *T) bool { return (*p).GetId() == delId })
		delete(gb.Index["Id"], delId)
	}

	gb.resetDeleteQueue()

	log.Println("Saved entity", gb.TypeName())

	if err := gb.save(); err != nil {
		log.Panicln("Panic during saving a base", gb.TypeName(), err)
	}

	fmt.Println("Data successfully written to file:", gb.Filepath())
	return nil
}

// merge copies non-zero fields from merger into target, skipping fields
// marked with key:"primary". Used by [DB.PatchById] to apply partial updates.
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
		log.Println("Fieldname:", mergerFieldType.Name)

		targetField := targetVal.FieldByName(mergerFieldType.Name)
		fieldKeyType := mergerFieldType.Tag.Get("key")

		if fieldKeyType == "primary" || !targetField.CanSet() {
			log.Println("Cannot merge field", mergerFieldType.Name, "because it is primary or not settable")
			continue
		}

		if targetField.Kind() == reflect.Ptr && !mergerField.IsNil() {
			log.Println("Merged field with name:", mergerFieldType.Name)
			targetField.Set(mergerField)
			continue
		}

		if !mergerField.IsZero() {
			log.Println("Merged field with name:", targetField.Type().Name())
			targetField.Set(mergerField)
		}
	}

	return nil
}
