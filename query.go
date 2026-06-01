package nestory

import (
	"fmt"
	"reflect"
)

// FindOneBy returns the first entity where field `key` equals `withValue`.
// Returns [ErrEmptyEntity] if the base has no rows. Returns (nil, nil) if
// the base is non-empty but no row matches.
//
// Primary-key lookups (key == gb.Identifier) hit the Id index directly and
// are O(1). Any other key is a linear scan comparing the requested field.
func (gb *DB[T]) FindOneBy(key string, withValue any) (*T, error) {
	if gb.store.Len() == 0 {
		return nil, ErrEmptyEntity
	}

	// Fast path: the primary key is indexed, so skip the scan entirely.
	if key == gb.Identifier {
		if ptr, ok := gb.Index[key][withValue]; ok {
			return ptr, nil
		}

		return nil, nil
	}

	chunks := gb.store.Chunks()
	tombs := gb.store.Tombs()

	for chunkIdx := range chunks {
		chunk := chunks[chunkIdx]
		tomb := tombs[chunkIdx]

		for i := range chunk {
			if tomb[i] {
				continue
			}
			s := &chunk[i]

			val := reflect.ValueOf(s).Elem()
			for val.Kind() == reflect.Ptr || val.Kind() == reflect.Interface {
				if val.IsNil() {
					break
				}
				val = val.Elem()
			}

			if val.Kind() != reflect.Struct {
				continue
			}

			f := val.FieldByName(key)
			if f.IsValid() && f.Interface() == withValue {
				return s, nil
			}
		}
	}

	return nil, nil
}

// Filter returns every live entity for which filterFn returns true (by value).
func (gb *DB[T]) Filter(filterFn func(T) bool) []T {
	var filteredSlice []T
	gb.store.Range(func(p *T) {
		if filterFn(*p) {
			filteredSlice = append(filteredSlice, *p)
		}
	})
	return filteredSlice
}

// FilterPtr is like Filter but returns stable *T pointers into the backing
// store. Returns an error if the result is empty — callers depend on this for
// "no rows" detection.
func (gb *DB[T]) FilterPtr(filterFn func(*T) bool) ([]*T, error) {
	var filteredSlice []*T
	gb.store.Range(func(p *T) {
		if filterFn(p) {
			filteredSlice = append(filteredSlice, p)
		}
	})

	if len(filteredSlice) == 0 {
		return filteredSlice, fmt.Errorf("empty array")
	}
	return filteredSlice, nil
}
