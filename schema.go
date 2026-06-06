package nestory

import (
	"fmt"
	"reflect"
)

// isScalarKey reports whether a foreign-key column type is a string or any int.
func isScalarKey(k reflect.Kind) bool {
	switch k {
	case reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

// dbCreator is the untyped sibling of [DB], used during load before T is bound.
// Reflection only — no generic methods.
type dbCreator struct{}

// inflateSlice turns decoded flat schema rows into []T. Relation fields become
// hollow pointers carrying only the foreign Id; fillRelation rewires them once
// every type is registered. One slice = one chunk.
func inflateSlice[T Entity](creator *dbCreator, bv reflect.Value) ([]T, error) {
	if bv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("Provided entity can only be a slice")
	}

	out := make([]T, 0, bv.Len())

	for idx := range bv.Len() {
		zero := new(T)

		zeroStruct := reflect.ValueOf(zero).Elem()
		schemaStruct := bv.Index(idx)

		fields := creator.getSchemaFields(zero)

		for fi := range fields {
			fieldName := fields[fi][0]
			schemaFieldName := fields[fi][1]

			schemaField := schemaStruct.FieldByName(schemaFieldName)
			if !schemaField.IsValid() {
				return nil,
					fmt.Errorf("ReadingEntityError: Interface instance field: '%s' is not valid", schemaFieldName)
			}

			zeroField := zeroStruct.FieldByName(fieldName)
			if !zeroField.IsValid() {
				return nil, fmt.Errorf("ReadingEntityError: Entity 'zero' instance field: '%s' is not valid", fieldName)
			}

			zeroFieldType, ok := zeroStruct.Type().FieldByName(fieldName)
			if !ok {
				return nil, fmt.Errorf("ReadingEntityError: 'Zero' instance field name type reflection failed")
			}

			relFieldName := zeroFieldType.Tag.Get("relto")
			if relFieldName != "" {
				if schemaField.IsZero() {
					continue
				}

				if zeroField.Type().Kind() != reflect.Pointer {
					return nil, fmt.Errorf("In concrete instance of an entity, rel field must a pointer")
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

		out = append(out, *zero)
	}

	return out, nil
}

// NormalizeToSchema flattens a typed entity to a schema row — relto pointers
// become their target's primary-key value.
func (db *DB[T]) NormalizeToSchema(instance T) reflect.Value {
	v := reflect.ValueOf(instance)
	t := v.Type()

	fields := db.schemaFields
	schemaStruct := db.createSchemaStruct()

	for idx := range fields {
		fieldName := fields[idx][0]
		schemaFieldName := fields[idx][1]

		fieldVal := v.FieldByName(fieldName)
		fieldType, ok := t.FieldByName(fieldName)
		if !ok {
			continue
		}

		schemaField := schemaStruct.FieldByName(schemaFieldName)
		if !schemaField.IsValid() {
			panic(fmt.Sprintf("Schema field %s not found\n", schemaFieldName))
		}

		currType := fieldVal.Type()
		currValue := fieldVal

		if currType.Kind() == reflect.Pointer {
			currValue = currValue.Elem()
			if !currValue.IsValid() {
				continue
			}

			relFieldName := fieldType.Tag.Get("relto")
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

// createSchemaFields lists (entityField, schemaColumn) pairs for T. relto fields
// become "<field><tag>"; mapby fields are skipped (memory only).
func (db *DB[T]) createSchemaFields() [][2]string {
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

// createSchemaStruct builds the dynamic row struct for T. relto pointers
// collapse to their primary-key type.
func (db *DB[T]) createSchemaStruct() reflect.Value {
	fields := db.schemaFields

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

// untyped twins, used during load

func (c *dbCreator) createSchemaStruct(entity any) reflect.Value {
	fields := c.getSchemaFields(entity)

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

func (c *dbCreator) getSchemaFields(entity any) [][2]string {
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
