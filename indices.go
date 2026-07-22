package nestory

// initIndices creates only indexes that are actually maintained. Non-indexed
// fields deliberately remain absent so FindOneBy can fall back to a scan.
func (db *DB[T]) initIndices() {
	db.index["Id"] = make(map[any]*T)
}

// syncIdIndex points Index["Id"] at the stable slot for every live entity and
// drops stale nil entries.
func (db *DB[T]) syncIdIndex() {
	db.store.Range(func(p *T) {
		if _, ok := db.index["Id"][(*p).GetId()]; !ok {
			db.index["Id"][(*p).GetId()] = p
		}
	})

	for key, val := range db.index["Id"] {
		if val == nil {
			delete(db.index["Id"], key)
		}
	}
}
