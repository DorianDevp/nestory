package nestory

import (
	"fmt"
	"log"
	"reflect"
)

// isScalarKey reports whether a foreign-key column type is an acceptable
// primitive: a string or any integer width (the primary key is int, but a
// relto target may legitimately be any integer kind).
func isScalarKey(k reflect.Kind) bool {
	switch k {
	case reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

// goBaseCreator is the untyped sibling of [DB], used during initial file
// load before the concrete generic parameter T is bound. It only works
// through reflection — no generic methods.
type goBaseCreator struct{}

// readEntity reads a slice of flat schema rows from disk and inflates each
// row into a concrete T. relto fields become hollow pointers carrying only
// the foreign Id; [DB.fillRelation] rewires them to real instances once
// every type has been registered.
func readEntity[T Entity](base any) *chunkStore[T] {
	creator := &goBaseCreator{}

	store := newChunkStore[T](chunkLimit)
	if base == nil {
		return store
	}

	bv := reflect.ValueOf(base)
	if bv.Kind() != reflect.Slice {
		log.Panic("Provided entity can only be a slice")
	}

	for idx := range bv.Len() {
		zero := new(T)

		zeroStruct := reflect.ValueOf(zero).Elem()
		schemaStruct := bv.Index(idx)

		fields := creator.getSchemaFields(zero)

		for idx := range fields {
			fieldName := fields[idx][0]
			schemaFieldName := fields[idx][1]

			schemaField := schemaStruct.FieldByName(schemaFieldName)
			if !schemaField.IsValid() {
				log.Panic("ReadingEntityError: ",
					fmt.Sprintf("Interface instance field: '%s' is not valid", schemaFieldName))
			}

			zeroField := zeroStruct.FieldByName(fieldName)
			if !zeroField.IsValid() {
				log.Panic("ReadingEntityError: ",
					fmt.Sprintf("Entity 'zero' instance field: '%s' is not valid", fieldName))
			}

			zeroFieldType, ok := zeroStruct.Type().FieldByName(fieldName)
			if !ok {
				log.Panic("ReadingEntityError: ",
					fmt.Sprintf("'Zero' instance field name type reflection failed"))
			}

			relFieldName := zeroFieldType.Tag.Get("relto")
			if relFieldName != "" {
				if schemaField.IsZero() {
					log.Println(schemaFieldName, "is empty, cannot proceed with creating a field")
					continue
				}

				if zeroField.Type().Kind() != reflect.Pointer {
					log.Panic("In concrete instance of an entity, rel field must a pointer")
				}

				hollowRel := reflect.New(zeroFieldType.Type.Elem())
				hollowRelField := hollowRel.Elem().FieldByName(relFieldName)
				hollowRelField.Set(schemaField)

				zeroField.Set(hollowRel)
				continue
			}

			mapFieldName := zeroFieldType.Tag.Get("mapby")
			if mapFieldName != "" {
				continue
			}

			zeroField.Set(schemaField)
		}

		store.Append(*zero)
	}

	return store
}

// NormalizeToSchema produces a flat schema row from a typed entity. relto
// pointer fields are flattened to their target's primary-key value.
func (gb *DB[T]) NormalizeToSchema(instance T) reflect.Value {
	v := reflect.ValueOf(instance)
	t := v.Type()

	fields := gb.schemaFields
	schemaStruct := gb.createSchemaStruct()

	for idx := range fields {
		fieldName := fields[idx][0]
		schemaFieldName := fields[idx][1]

		fieldVal := v.FieldByName(fieldName)
		fieldType, ok := t.FieldByName(fieldName)
		if !ok {
			log.Println("Error while getting field name:", fieldName)
			continue
		}

		schemaField := schemaStruct.FieldByName(schemaFieldName)
		if !schemaField.IsValid() {
			log.Printf("schemaStruct: %+v\n, schemaStruct type : %s\n", schemaStruct, schemaStruct.Type())
			panic(fmt.Sprintf("Schema field %s not found\n", schemaFieldName))
		}

		currType := fieldVal.Type()
		currValue := fieldVal

		if currType.Kind() == reflect.Pointer {
			currValue = currValue.Elem()
			if !currValue.IsValid() {
				log.Printf("probably nil pointer derefference %s, %+v\n", currValue.Kind(), currValue)
				continue
			}

			relFieldName := fieldType.Tag.Get("relto")
			log.Println("RelVal:", relFieldName)
			if relFieldName == "" {
				panic("You cannot define field with pointer to struct type unless it is a relational field (look on relto tag)")
			}

			relField := currValue.FieldByName(relFieldName)
			if !isScalarKey(relField.Kind()) {
				panic("Field attached to foreign field must be primitive type")
			}

			currValue = relField
		}

		if currValue.Kind() == reflect.Pointer {
			panic("You cannot set a schema's field type as a pointer")
		}

		schemaField.Set(currValue)
	}

	return schemaStruct
}

// createSchemaFields lists (entityFieldName, schemaColumnName) pairs for T.
// relto fields produce a "<fieldName><tagValue>" schema column.
// mapby fields are skipped entirely — they live only in memory.
func (gb *DB[T]) createSchemaFields() [][2]string {
	zero := new(T)

	t := reflect.TypeOf(zero).Elem()
	if t.Kind() != reflect.Struct {
		panic("Entity must be a Struct")
	}

	fields := make([][2]string, 0)

	for idx := range t.NumField() {
		f := t.Field(idx)

		name := f.Name
		fName := f.Name

		if relTag := f.Tag.Get("relto"); relTag != "" {
			fName = fName + relTag
		}

		if mapTag := f.Tag.Get("mapby"); mapTag != "" {
			continue
		}

		fields = append(fields, [2]string{name, fName})
	}

	return fields
}

// createSchemaStruct builds the dynamic struct used as a "row" of T.
// relto pointer fields collapse to their primary-key field's type.
func (gb *DB[T]) createSchemaStruct() reflect.Value {
	fields := gb.schemaFields

	zero := new(T)
	f := []reflect.StructField{}

	v := reflect.ValueOf(zero).Elem()
	t := v.Type()

	for idx := range fields {
		fieldName := fields[idx][0]
		schemaFieldName := fields[idx][1]

		fieldVal := v.FieldByName(fieldName)
		fieldType, ok := t.FieldByName(fieldName)
		if !ok {
			continue
		}

		currType := fieldVal.Type()
		kind := fieldVal.Kind()

		if kind == reflect.Pointer {
			currType = currType.Elem()

			relFieldName := fieldType.Tag.Get("relto")
			if relFieldName == "" {
				log.Print("Error while getting relto in ", currType.Name())
				panic("You cannot define field with pointer to struct type unless it is a relational field (look on relto tag)")
			}

			relField, ok := currType.FieldByName(relFieldName)
			if !ok {
				panic("Foreign field does not exist")
			}

			if relField.Type.Kind() == reflect.Struct || relField.Type.Kind() == reflect.Pointer {
				panic("Field attached to foreign field must be primitive type")
			}

			currType = relField.Type
		}

		if currType.Kind() == reflect.Pointer {
			panic("You cannot set a schema's field type as a pointer")
		}

		f = append(f, reflect.StructField{
			Name: schemaFieldName,
			Type: currType,
		})
	}

	refStruct := reflect.StructOf(f)
	return reflect.New(refStruct).Elem()
}

func (gb *DB[T]) isInterfaceSchemaCompliant(instance any) bool {
	t := reflect.TypeOf(instance)

	interfaceFields := make(map[string]bool)
	for i := range t.NumField() {
		interfaceFields[t.Field(i).Name] = true
	}

	for _, pair := range gb.schemaFields {
		if _, ok := interfaceFields[pair[1]]; !ok {
			return false
		}
	}
	return true
}

// --- untyped twins, used during initial file load ---------------------

func (gbc *goBaseCreator) createSchemaStruct(entity any) reflect.Value {
	fields := gbc.getSchemaFields(entity)

	f := []reflect.StructField{}

	v := reflect.ValueOf(entity)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	t := v.Type()

	for idx := range fields {
		fieldName := fields[idx][0]
		schemaFieldName := fields[idx][1]

		fieldVal := v.FieldByName(fieldName)
		fieldType, ok := t.FieldByName(fieldName)
		if !ok {
			continue
		}

		currType := fieldVal.Type()
		kind := fieldVal.Kind()

		if kind == reflect.Pointer {
			currType = currType.Elem()

			relFieldName := fieldType.Tag.Get("relto")
			if relFieldName == "" {
				log.Print("Error while getting relto in ", currType.Name())
				panic("You cannot define field with pointer to struct type unless it is a relational field (look on relto tag)")
			}

			relField, ok := currType.FieldByName(relFieldName)
			if !ok {
				panic("Foreign field does not exist")
			}

			if relField.Type.Kind() == reflect.Struct || relField.Type.Kind() == reflect.Pointer {
				panic("Field attached to foreign field must be primitive type")
			}

			currType = relField.Type
		}

		if currType.Kind() == reflect.Pointer {
			panic("You cannot set a schema's field type as a pointer")
		}

		f = append(f, reflect.StructField{
			Name: schemaFieldName,
			Type: currType,
		})
	}

	refStruct := reflect.StructOf(f)
	return reflect.New(refStruct).Elem()
}

func (gbc *goBaseCreator) getSchemaFields(entity any) [][2]string {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		panic("Entity must be a Struct")
	}

	fields := make([][2]string, 0)

	for idx := range t.NumField() {
		f := t.Field(idx)

		name := f.Name
		fName := f.Name

		if relTag := f.Tag.Get("relto"); relTag != "" {
			fName = fName + relTag
		}

		if mapTag := f.Tag.Get("mapby"); mapTag != "" {
			continue
		}

		fields = append(fields, [2]string{name, fName})
	}

	return fields
}
