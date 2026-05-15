package nestory

import (
	"fmt"
	"log"
	"reflect"
	"slices"
)

// Add appends entity to the in-memory slice without queueing it for save.
// Used internally by Flush; rarely needed by callers — prefer
// [DB.AddToPersistQueue] + [DB.Flush].
func (gb *DB[T]) Add(entity *T) {
	gb.mu.Lock()
	defer gb.mu.Unlock()

	_ent := *entity
	*gb.GoEntity = append(*gb.GoEntity, _ent)
}

// AddToPersistQueue stages entity for the next Flush. If entity.Id is zero,
// nestory auto-assigns one based on max(persisted, queued) + 1. If you
// supply an explicit Id that already exists in either the persisted set
// or the queue, the call is a no-op.
func (gb *DB[T]) AddToPersistQueue(entity *T) error {
	id := (*entity).GetId()

	// Caller supplied an Id explicitly — accept it, but dedupe.
	if id != 0 {
		if existing, _ := gb.FindOneBy("Id", id); existing != nil {
			return nil
		}
		for _, q := range gb.persistQueue {
			if (*q).GetId() == id {
				return nil
			}
		}
		gb.persistQueue = append(gb.persistQueue, entity)
		gb.Index["Id"][id] = entity
		return nil
	}

	// Auto-assign the next Id. Must consider both persisted entities AND
	// the pending queue — otherwise batch-queueing multiple items before
	// Flush() would hand each one the same Id.
	nextId := 0
	for _, e := range *gb.GoEntity {
		if e.GetId() > nextId {
			nextId = e.GetId()
		}
	}
	for _, q := range gb.persistQueue {
		if qid := (*q).GetId(); qid > nextId {
			nextId = qid
		}
	}
	nextId++

	SetId(entity, nextId)
	gb.persistQueue = append(gb.persistQueue, entity)
	gb.Index["Id"][nextId] = entity
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
	for _, ent := range gb.persistQueue {
		entity := *ent
		id := entity.GetId()

		if existing, err := gb.FindOneBy("Id", id); existing != nil {
			gb.PatchById(id, entity)
			continue
		} else if err != nil && err != ErrEmptyEntity {
			log.Panicln(err)
		}

		gb.Add(ent)
	}

	gb.ResetpersistQueue()
	gb.syncIdIndex()

	_ent := gb.GoEntity
	for _, instance := range gb.deleteQueue {
		i := *instance
		for idx, _instance := range *_ent {
			if _instance.GetId() == i.GetId() {
				*_ent = slices.Delete(*_ent, idx, idx+1)
				break
			}
		}
	}

	gb.resetDeleteQueue()
	gb.GoEntity = _ent

	log.Println("Saved entity", gb.TypeName(), *gb.GoEntity)
	fmt.Printf("PERSIST QUEUE (should be empty): %+v\n", gb.persistQueue)

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
