package nestory

import "reflect"

// initIndices creates an empty inner map for every field of T.
func (db *DB[T]) initIndices() {
	baseType := reflect.TypeOf(*new(T))
	if baseType.Kind() == reflect.Pointer {
		baseType = baseType.Elem()
	}

	for i := range baseType.NumField() {
		fieldName := baseType.Field(i).Name
		db.index[fieldName] = make(map[any]*T)
	}
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
