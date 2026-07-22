package nestory

import "reflect"

// initIndices creates only indexes that are actually maintained. Non-indexed
// fields deliberately remain absent so FindOneBy can fall back to a scan.
func (db *DB[T]) initIndices() {
	db.secondary = make(map[string]*secondaryIndex[T])
	specs, err := secondaryIndexSpecs(reflect.TypeFor[T]())
	if err != nil {
		panic(err)
	}

	for _, spec := range specs {
		db.secondary[spec.name] = newSecondaryIndex[T](spec)
	}
}

// syncIndices rebuilds every declared index from the durable live rows.
func (db *DB[T]) syncIndices() error {
	return db.rebuildSecondaryIndices()
}
