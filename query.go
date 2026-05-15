package nestory

import (
	"fmt"
	"reflect"
	"sync"
)

// FindOneBy returns the first entity where field `key` equals `withValue`.
// Returns [ErrEmptyEntity] if the base has no rows. Returns (nil, nil) if
// the base is non-empty but no row matches.
func (gb *DB[T]) FindOneBy(key string, withValue any) (*T, error) {
	if len(*gb.GoEntity) <= 0 {
		fmt.Println("Entity is empty")
		return nil, ErrEmptyEntity
	}

	var wg sync.WaitGroup
	chResult := make(chan *T, 1)
	chStop := make(chan struct{})

	memberMatch := func(s *T) {
		defer wg.Done()

		val := reflect.ValueOf(s).Elem()

		for {
			if val.Kind() == reflect.Ptr || val.Kind() == reflect.Interface {
				if val.IsNil() {
					fmt.Printf("Value is nil\n")
					return
				}
				val = val.Elem()
			} else {
				break
			}
		}

		if val.Kind() != reflect.Struct {
			fmt.Printf("Expected struct, got %v\n", val.Kind())
			return
		}

		for idx := range val.NumField() {
			k := val.Type().Field(idx).Name
			v := val.Field(idx).Interface()

			if k == key && v == withValue {
				select {
				case chResult <- s:
					return
				case <-chStop:
					return
				}
			}
		}
	}

	for i := range *gb.GoEntity {
		wg.Add(1)
		entity := *gb.GoEntity
		go memberMatch(&entity[i])
	}

	var result *T

	go func() {
		wg.Wait()
		close(chResult)
	}()

	select {
	case result = <-chResult:
		close(chStop)
	case <-chStop:
	}

	if result == nil {
		return nil, nil
	}
	return result, nil
}

// Filter returns every entity for which filterFn returns true (by value).
func (gb *DB[T]) Filter(filterFn func(T) bool) []T {
	var filteredSlice []T
	for _, item := range *gb.GoEntity {
		if filterFn(item) {
			filteredSlice = append(filteredSlice, item)
		}
	}
	return filteredSlice
}

// FilterPtr is like Filter but returns *T pointers. Returns an error if
// the result is empty — callers depend on this for "no rows" detection.
func (gb *DB[T]) FilterPtr(filterFn func(*T) bool) ([]*T, error) {
	var filteredSlice []*T
	for _, item := range *gb.GoEntity {
		if filterFn(&item) {
			filteredSlice = append(filteredSlice, &item)
		}
	}

	if len(filteredSlice) == 0 {
		return filteredSlice, fmt.Errorf("Empty array\n")
	}
	return filteredSlice, nil
}
