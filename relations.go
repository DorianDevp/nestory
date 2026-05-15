package nestory

import (
	"log"
	"reflect"
)

// fillRelation rebuilds the pointer graph on every entity in this base.
// Called once from [Open]. For each relto field it locates the matching
// instance in the target base's slice and assigns it. For each mapby field
// it scans the target base and appends every child whose mapby field
// equals this entity's Id.
//
// All target bases must be present in entityRegistry — that's why
// [Register] for every type must run before any [Open].
func (gb *DB[T]) fillRelation() {
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

	for idx := range *gb.GoEntity {
		el := &(*gb.GoEntity)[idx]
		v := reflect.ValueOf(el)
		t := reflect.TypeOf(*el)

		for fieldName, relto := range relFields {
			f := v.Elem().FieldByName(fieldName)
			tf, ok := t.FieldByName(fieldName)
			if !ok {
				log.Panicln("No field", fieldName)
			}

			if f.Kind() != reflect.Pointer {
				log.Panic("All relfields must by a pointer")
			}

			if f.IsZero() {
				log.Println("No relation found for field", tf.Name, "with value", f.Interface(), ";  - skipping")
				continue
			}

			relTypeName := f.Elem().Type().Name()
			relEntity, ok := entityRegistry[relTypeName]
			if !ok {
				log.Panicln("Could not relate any instance with type of", relTypeName)
			}

			relEntityPtr := reflect.ValueOf(relEntity)
			relEntityValue := relEntityPtr.Elem()
			if relEntityValue.Kind() != reflect.Slice {
				log.Panicln("Value of related entity is not a slice")
			}

			concreteField := f.Elem().FieldByName(relto)

			for idx := range relEntityValue.Len() {
				instance := relEntityValue.Index(idx)
				if instance.Kind() != reflect.Ptr {
					instance = instance.Addr()
				}

				instanceRelField := instance.Elem().FieldByName(relto)
				if concreteField.Interface() != instanceRelField.Interface() {
					continue
				}

				log.Println(instance)
				f.Set(instance)
			}

			if f.IsZero() {
				log.Panicf("Field's value does not match to any of related field in the specified entity\n Rel field: %s \n; Entity fields value %s\n",
					relto, f.Elem().FieldByName(relto))
			}
		}

		for fieldName, mapby := range mapFields {
			log.Printf("\n\n Many To One \n\n")

			id := v.Elem().FieldByName("Id").Interface()

			f := v.Elem().FieldByName(fieldName)
			if _, ok := t.FieldByName(fieldName); !ok {
				log.Panicln("No field", fieldName)
			}

			if f.Kind() != reflect.Slice && f.Type().Elem().Kind() != reflect.Pointer {
				log.Panic("All mapby fields must by a slice of pointers")
			}

			// Empty slice is fine — we'll append matches below if any exist.

			relTypeName := f.Type().Elem().Elem().Name()
			relEntity, ok := entityRegistry[relTypeName]
			if !ok {
				log.Panicln("Could not relate any instance with type of", relTypeName)
			}

			relEntityPtr := reflect.ValueOf(relEntity)
			relEntityValue := relEntityPtr.Elem()
			if relEntityValue.Kind() != reflect.Slice {
				log.Panicln("Value of related entity is not a slice")
			}

			for idx := range relEntityValue.Len() {
				instance := relEntityValue.Index(idx)
				if instance.Kind() != reflect.Ptr {
					instance = instance.Addr()
				}

				instanceRelField := instance.Elem().FieldByName(mapby)
				if instanceRelField.Interface() != id {
					continue
				}

				log.Println(instance)
				f.Set(reflect.Append(f, instance))
			}

			if f.IsZero() {
				log.Printf("mapby: no children matched for field %q (parent.Id=%v) — leaving empty\n",
					mapby, id)
			}
		}
	}
}
