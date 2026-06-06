package nestory

import (
	"log"
	"reflect"
)

// fillRelation rebuilds the pointer graph for this base. relto fields get the
// matching target instance; mapby fields get every child whose mapby field
// equals this entity's Id. Every target must already be in entityRegistry.
func (db *DB[T]) fillRelation() {
	relFields := make(map[string]string)
	mapFields := make(map[string]string)

	t := reflect.TypeOf(*new(T))
	for idx := range t.NumField() {
		f := t.Field(idx)
		if relto := f.Tag.Get("relto"); relto != "" {
			relFields[f.Name] = relto
		}
		if mapby := f.Tag.Get("mapby"); mapby != "" {
			mapFields[f.Name] = mapby
		}
	}

	db.store.Range(func(el *T) {
		v := reflect.ValueOf(el)
		t := reflect.TypeOf(*el)

		for fieldName, relto := range relFields {
			f := v.Elem().FieldByName(fieldName)
			_, ok := t.FieldByName(fieldName)
			if !ok {
				log.Panicln("No field", fieldName)
			}

			if f.Kind() != reflect.Pointer {
				log.Panic("Relation field must be a pointer")
			}

			if f.IsZero() {
				continue
			}

			relTypeName := f.Elem().Type().Name()

			foreignStore, ok := getForeignStoreByType(relTypeName)

			if !ok {
				log.Panicln("Could not relate any instance with type of", relTypeName)
			}

			relFieldVal := f.Elem().FieldByName(relto)

			for idx := 0; idx < foreignStore.Len(); idx++ {
				instance := foreignStore.Index(idx)

				instanceRelField := instance.Elem().FieldByName(relto)
				if relFieldVal.Interface() != instanceRelField.Interface() {
					continue
				}

				f.Set(instance)

				break
			}

			if f.IsZero() {
				log.Panicf("Field's value does not match to any of related field in the specified entity\n Rel field: %s \n; Entity fields value %s\n",
					relto, f.Elem().FieldByName(relto))
			}
		}

		for fieldName, mapby := range mapFields {
			id := v.Elem().FieldByName("Id").Interface()

			f := v.Elem().FieldByName(fieldName)
			if _, ok := t.FieldByName(fieldName); !ok {
				log.Panicln("No field", fieldName)
			}

			if f.Kind() != reflect.Slice && f.Type().Elem().Kind() != reflect.Pointer {
				log.Panic("All mapby fields must by a slice of pointers")
			}

			relTypeName := f.Type().Elem().Elem().Name()
			foreignStore, ok := getForeignStoreByType(relTypeName)
			if !ok {
				log.Panicln("Could not relate any instance with type of", relTypeName)
			}

			for idx := 0; idx < foreignStore.Len(); idx++ {
				instance := foreignStore.Index(idx)

				instanceRelField := instance.Elem().FieldByName(mapby)
				if instanceRelField.Interface() != id {
					continue
				}

				f.Set(reflect.Append(f, instance))
			}
		}
	})
}

// getForeignStoreByType returns the []*T of live entities for typeName, wrapped in a
// reflect.Value. Elements are stable store pointers, ready to assign.
func getForeignStoreByType(typeName string) (reflect.Value, bool) {
	e, ok := storeRegistry[typeName]
	if !ok {
		return reflect.Value{}, false
	}

	iter, ok := e.(pointerStoreIterator)
	if !ok {
		return reflect.Value{}, false
	}

	return reflect.ValueOf(iter.iterateStorePointers()), true
}
