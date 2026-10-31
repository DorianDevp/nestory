package nestory

// Checkpoint writes every registered table as a snapshot and empties the
// logs, so a start reads snapshots rather than replaying history. Call it
// while nothing else commits — at startup, before serving.
//
// It is safe to be interrupted. Every snapshot is written before any log is
// emptied, and the logs replay onto a snapshot to the same state it already
// holds, so stopping anywhere in between leaves a store that opens the same.
func Checkpoint() error {
	runtimes := relationRuntimes()
	for _, runtime := range runtimes {
		checkpointer, ok := runtime.(interface{ checkpointSave() error })
		if !ok {
			continue
		}
		if err := checkpointer.checkpointSave(); err != nil {
			return err
		}
	}
	if err := sharedTransactionWAL().truncate(); err != nil {
		return err
	}
	for _, runtime := range runtimes {
		if truncater, ok := runtime.(interface{ checkpointTruncate() error }); ok {
			if err := truncater.checkpointTruncate(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB[T]) checkpointSave() error {
	db.store.markAllDirty()
	return db.save()
}

func (db *DB[T]) checkpointTruncate() error {
	if db.wal == nil {
		return nil
	}
	return db.wal.truncate()
}

// Reserve makes sure no new row is given an id at or below this one. It is
// for ids something still points at although their row is gone, when the
// table can no longer know it ever handed them out.
func (db *DB[T]) Reserve(id int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.counter = max(db.counter, id)
	db.store.highWater = max(db.store.highWater, id)
}
