package nestory

// tx.go is the DB's side of the transaction protocol — the committer methods
// the Engine drives at commit, the resById lookup they share, and the public
// abort entry point. The CRUD verbs (Get/Update/...) live in query.go; the
// coordinator in engine.go.

// resource resolves the stable Resource slot for id, guarding resById against
// a concurrent Add. The returned pointer is stable for the store's life.
func (db *DB[T]) resource(id int) (*Resource[T], bool) {
	db.mu.RLock()
	r, ok := db.resById[id]
	db.mu.RUnlock()

	return r, ok
}

// committer — driven by the Engine. lockResource/unlockResource own the per-row
// mutex; the rest assume it's already held.

func (db *DB[T]) lockResource(id int) {
	if r, ok := db.resource(id); ok {
		r.mu.Lock()
	}
}

func (db *DB[T]) unlockResource(id int) {
	if r, ok := db.resource(id); ok {
		r.mu.Unlock()
	}
}

func (db *DB[T]) resourceVersion(id int) (int, bool) {
	if r, ok := db.resource(id); ok {
		return r.version, true
	}

	return 0, false
}

func (db *DB[T]) applyWrite(id int, work any) {
	if r, ok := db.resource(id); ok {
		*r.item = *(work.(*T)) // write through the stable pointer — never moves
		r.version++
		db.store.markDirty(r.chunk)
	}
}

func (db *DB[T]) refreshSnapshot(id int, work any) {
	if r, ok := db.resource(id); ok {
		*(work.(*T)) = *r.item
	}
}

func (db *DB[T]) logWrites(items []pendingWrite) error {
	rec := walFrame{Rows: make([]walRow, 0, len(items))}

	for _, it := range items {
		rowBytes, err := encodeRow(db.NormalizeToSchema(*(it.work.(*T))))
		if err != nil {
			return err
		}

		rec.Rows = append(rec.Rows, walRow{Id: int64(it.id), Row: rowBytes})
	}

	return db.wal.appendFrame(rec)
}

// DiscardSnapshot drops the transaction behind a snapshot from [DB.Get] without
// committing it. Call it when you took a snapshot but decided not to write back.
func (db *DB[T]) DiscardSnapshot(snapshot *T) {
	engine.discardByPtr(snapshot)
}
