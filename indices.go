package nestory

import "reflect"

// initIndices creates an empty inner map for every field of T, so the
// outer Index map is ready for direct subscript writes.
func (gb *DB[T]) initIndices() {
	baseType := reflect.TypeOf(*gb.GoEntity).Elem()
	if baseType.Kind() == reflect.Ptr {
		baseType = baseType.Elem()
	}

	for i := range baseType.NumField() {
		fieldName := baseType.Field(i).Name
		gb.Index[fieldName] = make(map[any]*T)
	}
}

// syncIdIndex ensures Index["Id"] contains a pointer to every entity in
// the slice, and drops any stale nil entries.
func (gb *DB[T]) syncIdIndex() {
	for idx, el := range *gb.GoEntity {
		if _, ok := gb.Index["Id"][el.GetId()]; !ok {
			gb.Index["Id"][el.GetId()] = &(*gb.GoEntity)[idx]
		}
	}

	for key, val := range gb.Index["Id"] {
		if val == nil {
			delete(gb.Index["Id"], key)
		}
	}
}
