package nestory

import "reflect"

// getDetachedRoot handles the overwhelmingly common one-node branch without
// allocating a general transaction context. Update promotes the snapshot into
// the same commit engine used by larger ownership branches.
func (db *DB[T]) getDetachedRoot(root nodeKey) (*T, bool, error) {
	if relationGraphParticipant(root.typ) {
		graphMu.RLock()
		defer graphMu.RUnlock()

		if err := ensureCommittedOwnership(); err != nil {
			return nil, true, err
		}

		if committedOwnerHasChildren(root) {
			return nil, false, nil
		}
	}

	resource, found := db.resource(root.id)
	if !found {
		return nil, true, ErrNotFound
	}

	resource.mu.RLock()
	work := new(T)
	*work = *resource.item
	cloneSliceFields(reflect.ValueOf(work).Elem())
	original := *work
	cloneSliceFields(reflect.ValueOf(&original).Elem())
	version := resource.version
	resource.mu.RUnlock()

	if err := rewireDetachedRoot(root, work); err != nil {
		return nil, true, err
	}

	db.snapshotMu.Lock()
	db.snapshots[work] = detachedRoot[T]{id: root.id, version: version, original: original}
	db.snapshotMu.Unlock()

	return work, true, nil
}

func rewireDetachedRoot[T Entity](key nodeKey, work *T) error {
	specs, err := relationSpecs(key.typ)
	if err != nil {
		return err
	}

	for _, spec := range specs {
		field := reflect.ValueOf(work).Elem().Field(spec.fieldIndex)
		if !spec.many {
			rewireSelfRelationPointer(field, spec.target, key, work)
			continue
		}

		for i := range field.Len() {
			rewireSelfRelationPointer(field.Index(i), spec.target, key, work)
		}
	}

	return nil
}

func (db *DB[T]) promoteDetachedRoot(branch *T) {
	db.snapshotMu.Lock()
	defer db.snapshotMu.Unlock()

	snapshot, found := db.snapshots[branch]
	if !found {
		return
	}

	original := new(T)
	*original = snapshot.original
	state := engine.begin()
	engine.record(state, touchedResource{
		dbName: db.name, id: snapshot.id, ver: snapshot.version,
		work: branch, original: original,
	})
	delete(db.snapshots, branch)
}
