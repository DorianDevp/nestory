package nestory

import "reflect"

// initIndices creates an empty inner map for every field of T, so the
// outer Index map is ready for direct subscript writes.
func (gb *DB[T]) initIndices() {
	baseType := reflect.TypeOf(*new(T))
	if baseType.Kind() == reflect.Ptr {
		baseType = baseType.Elem()
	}

	for i := range baseType.NumField() {
		fieldName := baseType.Field(i).Name
		gb.Index[fieldName] = make(map[any]*T)
	}
}

// syncIdIndex ensures Index["Id"] points at the backing store's stable slot
// for every live entity, and drops any stale nil entries.
func (gb *DB[T]) syncIdIndex() {
	gb.store.Range(func(p *T) {
		if _, ok := gb.Index["Id"][(*p).GetId()]; !ok {
			gb.Index["Id"][(*p).GetId()] = p
		}
	})

	for key, val := range gb.Index["Id"] {
		if val == nil {
			delete(gb.Index["Id"], key)
		}
	}
}
