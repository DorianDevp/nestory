package nestory

import (
	"errors"
	"fmt"
	"reflect"
)

// AuditTrackedWrites makes every tracked write also run the full branch diff
// and fail when a node changed that [Edit] never named. It costs exactly what
// declaring nothing would have cost, so switch it on in tests — that is where a
// forgotten Edit should surface, rather than as a write that quietly vanishes.
var AuditTrackedWrites bool

// ErrUndeclaredWrite reports a node that changed without a matching [Edit].
var ErrUndeclaredWrite = errors.New("nestory: node changed without a matching Edit")

// TrackedDB is the write-declaring API. An owned child is compared against
// committed state only when [Edit] hands it over, so a callback that touches a
// handful of nodes costs a handful of comparisons instead of one per node in
// the branch. Reading stays ordinary pointer traversal.
//
// The root is always compared, so a callback that only changes the root needs
// no Edit at all.
type TrackedDB[T Entity] struct {
	db *DB[T]
}

// Tracked enters the write-declaring API without allocating.
func (db *DB[T]) Tracked() TrackedDB[T] { return TrackedDB[T]{db: db} }

// Writes collects the nodes a tracked callback promises to change.
type Writes struct {
	// owner is the top-level owner of the root being written. Every declared
	// node has to sit under it: writing outside that tree would touch shadow
	// nodes another writer holds the lock for.
	owner    nodeKey
	declared []nodeKey
	err      error
}

func (writes *Writes) fail(err error) {
	if writes.err == nil {
		writes.err = err
	}
}

// Edit declares that node is about to change and returns it unchanged, so the
// declaration is what produces the value written through:
//
//	child := nestory.Edit(w, owner.Children[3])
//	child.Value = 11
//
// Writing straight through owner.Children[3] instead compiles and runs, but the
// change is not published. Set [AuditTrackedWrites] in tests to turn that into
// a failure.
func Edit[C Entity](writes *Writes, node *C) *C {
	if node == nil {
		writes.fail(errors.New("nestory: Edit needs a non-nil node"))
		return node
	}

	key := nodeKey{typ: reflect.TypeFor[C](), id: (*node).GetId()}
	if owner := committedOwnershipRoot(key); owner != writes.owner {
		writes.fail(fmt.Errorf("nestory: Edit(%s) is outside the branch being updated", key))
		return node
	}

	writes.declared = append(writes.declared, key)

	return node
}

// UpdateWithin retries a short transaction on conflict, like [DB.UpdateWithin],
// and additionally limits the comparison to the root plus whatever fn declares
// through [Edit]. fn may run more than once and must therefore express intent
// without external side effects.
//
// Roots outside the relation graph, and roots that own nothing, have no branch
// to skip: those fall back to the ordinary path and the declaration is unused.
func (tracked TrackedDB[T]) UpdateWithin(id int, fn func(*Writes, *T) error) error {
	db := tracked.db
	resource, found := db.resource(id)
	if !found {
		return ErrNotFound
	}

	resource.updateMu.Lock()
	defer resource.updateMu.Unlock()

	root := nodeKey{typ: reflect.TypeFor[T](), id: id}
	participant := relationGraphParticipant(root.typ)
	for {
		if !participant {
			err := db.Transaction(func(tx *Tx[T]) error {
				return tx.UpdateWithin(id, func(entity *T) error {
					return fn(&Writes{}, entity)
				})
			})
			if err != ErrConflict {
				return err
			}

			continue
		}

		handled, err := towerRun(root.typ, id, func(shadow reflect.Value, owner nodeKey) ([]nodeKey, error) {
			writes := &Writes{owner: owner}
			if err := fn(writes, shadow.Interface().(*T)); err != nil {
				return nil, err
			}
			if writes.err != nil {
				return nil, writes.err
			}

			// Never nil, so the diff knows a promise was made even when the
			// callback declared nothing beyond the root.
			if writes.declared == nil {
				writes.declared = []nodeKey{}
			}

			return writes.declared, nil
		})
		if handled {
			if err == ErrConflict {
				continue
			}

			return err
		}

		err = db.Transaction(func(tx *Tx[T]) error {
			return tx.UpdateWithin(id, func(entity *T) error {
				return fn(&Writes{}, entity)
			})
		})
		if err != ErrConflict {
			return err
		}
	}
}
