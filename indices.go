package nestory

import "reflect"

// initIndices creates only indexes that are actually maintained. Non-indexed
// fields deliberately remain absent so FindOneBy can fall back to a scan.
func (db *DB[T]) initIndices() {
	db.index["Id"] = make(map[any]*T)
	db.secondary = make(map[string]*secondaryIndex[T])
	specs, err := secondaryIndexSpecs(reflect.TypeFor[T]())
	if err != nil {
		panic(err)
	}

	for _, spec := range specs {
		db.secondary[spec.name] = newSecondaryIndex[T](spec)
		if spec.lookupField != "" {
			db.index[spec.lookupField] = make(map[any]*T)
		}
	}
}

// syncIndices rebuilds every declared index from the durable live rows.
func (db *DB[T]) syncIndices() error {
	db.store.Range(func(p *T) {
		db.index["Id"][(*p).GetId()] = p
	})

	return db.rebuildSecondaryIndices()
}
