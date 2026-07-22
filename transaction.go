package nestory

import (
	"fmt"
	"reflect"
)

// Tx is a transaction-local view of one root entity type. Objects returned by
// Get are detached branches; all mutations are detected and committed when the
// Transaction callback returns nil.
type Tx[T Entity] struct {
	db    *DB[T]
	state *transactionState
}

// TransactionContext is implemented by a live Tx. Join uses it to expose a
// differently typed DB inside the same commit context.
type TransactionContext interface {
	transactionContext() *transactionState
}

func (tx *Tx[T]) transactionContext() *transactionState { return tx.state }

// Join binds db to an existing transaction without opening a nested commit.
func (db *DB[T]) Join(context TransactionContext) *Tx[T] {
	return &Tx[T]{db: db, state: context.transactionContext()}
}

// Transaction runs fn against one shared transaction context and commits every
// changed branch once. Returning an error discards the complete working set.
func (db *DB[T]) Transaction(fn func(*Tx[T]) error) error {
	state := engine.begin()
	tx := &Tx[T]{db: db, state: state}
	if err := fn(tx); err != nil {
		engine.evict(state)
		return err
	}

	if err := engine.commit(state); err != nil {
		engine.evict(state)
		return err
	}

	return nil
}

// Get loads id and its ownership subtree into this transaction. Repeated loads
// of the same node return the same transaction-local pointer.
func (tx *Tx[T]) Get(id int) (*T, error) {
	key := nodeKey{typ: reflect.TypeFor[T](), id: id}
	if created, ok := engine.created(tx.state, key); ok {
		return created.Interface().(*T), nil
	}

	return tx.db.getInTransaction(tx.state, id)
}

// Create stages entity and its new ownership subtree in this transaction.
func (tx *Tx[T]) Create(entity *T) error {
	if entity == nil {
		return fmt.Errorf("nestory: create: nil entity")
	}

	return stageCreatedOwnershipTree(tx.state, reflect.ValueOf(entity))
}

// Delete stages id and its committed ownership subtree for deletion.
func (tx *Tx[T]) Delete(id int) error {
	key := nodeKey{typ: reflect.TypeFor[T](), id: id}
	if _, created := engine.created(tx.state, key); created {
		return engine.stageDelete(tx.state, stagedDelete{key: key})
	}

	resource, found := tx.db.resource(id)
	if !found {
		return ErrNotFound
	}

	resource.mu.RLock()
	version := resource.version
	resource.mu.RUnlock()

	return engine.stageDelete(tx.state, stagedDelete{key: key, ver: version})
}

// DeleteMany stages ids in the same transaction. Duplicate IDs are harmless;
// either every surviving row is deleted at commit or none is.
func (tx *Tx[T]) DeleteMany(ids []int) error {
	seen := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			continue
		}

		seen[id] = struct{}{}
		if err := tx.Delete(id); err != nil {
			return err
		}
	}

	return nil
}

// UpdateWithin mutates id inside this transaction. It joins the current context
// and does not commit independently.
func (tx *Tx[T]) UpdateWithin(id int, fn func(*T) error) error {
	entity, err := tx.Get(id)
	if err != nil {
		return err
	}

	return fn(entity)
}

func stageCreatedOwnershipTree(tx *transactionState, root reflect.Value) error {
	seen := make(map[uintptr]struct{})
	var stage func(reflect.Value) error
	stage = func(value reflect.Value) error {
		if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
			return fmt.Errorf("nestory: create requires non-nil entity pointers")
		}

		if _, duplicate := seen[value.Pointer()]; duplicate {
			return nil
		}

		seen[value.Pointer()] = struct{}{}

		typ := value.Type().Elem()
		runtime, ok := baseRegistry[typ.Name()].(relationRuntime)
		if !ok {
			return fmt.Errorf("nestory: %s is not open", typ)
		}

		if err := runtime.relationPrepareCreate(value); err != nil {
			return err
		}

		id, ok := valueID(value)
		if !ok {
			return fmt.Errorf("nestory: %s does not implement Entity", typ)
		}

		key := nodeKey{typ: typ, id: id}
		if err := engine.stageCreate(tx, createdResource{key: key, work: value}); err != nil {
			return err
		}

		specs, err := relationSpecs(typ)
		if err != nil {
			return err
		}

		for _, spec := range specs {
			if spec.kind != ownRelation {
				continue
			}

			field := value.Elem().Field(spec.fieldIndex)
			if !spec.many {
				if err := stage(field); err != nil {
					return err
				}

				continue
			}

			for i := range field.Len() {
				if err := stage(field.Index(i)); err != nil {
					return err
				}
			}
		}

		return nil
	}

	return stage(root)
}
