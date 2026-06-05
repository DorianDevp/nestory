package nestory

import (
	"fmt"
	"reflect"
	"sync"
)

// Look by indices: exists && empty -> nil
// routines through chunks
func (gb *DB[T]) FindOneBy(key string, withValue any) (*T, error) {
	if gb.store.Len() == 0 {
		return nil, ErrEmptyEntity
	}

	indexMap := gb.Index[key]
	if indexMap != nil {
		if val, ok := indexMap[withValue]; ok {
			return val, nil
		} else {
			return nil, nil
		}
	}

	chunks := gb.store.Chunks()
	tombs := gb.store.Tombs()

	needle := make(chan *Resource[T], 1);

	var wg sync.WaitGroup

	wg.Add(len(chunks))

	for chunkIdx := range chunks {
		go func(chunkIdx int) {
			defer wg.Done()

			chunk := chunks[chunkIdx]
			tomb := tombs[chunkIdx]

			for i := range chunk.n {
				if tomb.data[i] {
					continue
				}
				s := &chunk.data[i]

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
					s.mu.RLock()
					needle <- s

					return;
				}
			}
		}(chunkIdx) 
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case item := <-needle:
		item.mu.RUnlock()
		return item.item, nil
	case <-done:
		return nil, nil
	}
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
