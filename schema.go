package nestory

import (
	"fmt"
	"reflect"
	"sync"
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

// dbCreator is the untyped sibling of DB, used while loading before T is bound.
type dbCreator struct{}

type cachedSpecsByField struct {
	specs map[string]relationSpec
	err   error
}

var specsByFieldCache sync.Map

func specsByField(t reflect.Type) (map[string]relationSpec, error) {
	if cached, ok := specsByFieldCache.Load(t); ok {
		result := cached.(cachedSpecsByField)
		return result.specs, result.err
	}

	specs, err := relationSpecs(t)
	if err != nil {
		specsByFieldCache.Store(t, cachedSpecsByField{err: err})
		return nil, err
	}

	out := make(map[string]relationSpec, len(specs))
	for _, r := range specs {
		out[r.fieldName] = r
	}

	specsByFieldCache.Store(t, cachedSpecsByField{specs: out})
	return out, nil
}

func persistedRelation(r relationSpec) bool {
	return !r.many || r.kind == borrowRelation || r.kind == optionRelation
}

func relationColumnName(r relationSpec) string { return r.fieldName + r.matchField }

func relationColumnType(r relationSpec) reflect.Type {
	f, ok := r.target.FieldByName(r.matchField)
	if !ok {
		panic(fmt.Sprintf("nestory: relation target field %s.%s does not exist", r.target, r.matchField))
	}

	if r.many {
		return reflect.SliceOf(f.Type)
	}

	return f.Type
}

// inflateSlice turns decoded flat rows into entities. Persisted relation keys
// become hollow pointers; fillRelation later replaces them with stable pointers.
func inflateSlice[T Entity](creator *dbCreator, rows reflect.Value) ([]T, error) {
	if rows.Kind() != reflect.Slice {
		return nil, fmt.Errorf("nestory: persisted entity data must be a slice")
	}

	t := reflect.TypeFor[T]()
	specs, err := specsByField(t)
	if err != nil {
		return nil, err
	}

	fields := creator.getSchemaFields(*new(T))
	out := make([]T, 0, rows.Len())

	for i := range rows.Len() {
		entity := new(T)
		dst := reflect.ValueOf(entity).Elem()
		row := rows.Index(i)

		for _, pair := range fields {
			fieldName, columnName := pair[0], pair[1]
			column := row.FieldByName(columnName)
			if !column.IsValid() {
				return nil, fmt.Errorf("nestory: persisted column %q is missing", columnName)
			}

			field := dst.FieldByName(fieldName)
			if !field.IsValid() || !field.CanSet() {
				return nil, fmt.Errorf("nestory: entity field %q is missing or unsettable", fieldName)
			}

			r, relational := specs[fieldName]
			if !relational {
				field.Set(column)
				continue
			}

			if !persistedRelation(r) {
				continue
			}

			if !r.many {
				if column.IsZero() {
					continue
				}

				hollow := reflect.New(r.target)
				hollow.Elem().FieldByName(r.matchField).Set(column)
				field.Set(hollow)
				continue
			}

			hollows := reflect.MakeSlice(field.Type(), 0, column.Len())
			for j := range column.Len() {
				hollow := reflect.New(r.target)
				hollow.Elem().FieldByName(r.matchField).Set(column.Index(j))
				hollows = reflect.Append(hollows, hollow)
			}

			field.Set(hollows)
		}

		out = append(out, *entity)
	}

	return out, nil
}

func (db *DB[T]) normalizeToSchema(instance T) reflect.Value {
	v := reflect.ValueOf(instance)
	specs, err := specsByField(v.Type())
	if err != nil {
		panic(err)
	}
	row := db.createSchemaStruct()

	for _, pair := range db.schemaFields {
		fieldName, columnName := pair[0], pair[1]
		field := v.FieldByName(fieldName)
		column := row.FieldByName(columnName)
		r, relational := specs[fieldName]
		if !relational {
			column.Set(field)
			continue
		}

		if !r.many {
			key, ok := relationKey(field, r.matchField)
			if ok {
				column.Set(key)
			}

			continue
		}

		keys := reflect.MakeSlice(column.Type(), 0, field.Len())
		for i := range field.Len() {
			key, ok := relationKey(field.Index(i), r.matchField)
			if ok {
				keys = reflect.Append(keys, key)
			}
		}
		column.Set(keys)
	}
	return row
}

func schemaFieldsFor(t reflect.Type) [][2]string {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	specs, err := specsByField(t)
	if err != nil {
		panic(err)
	}

	fields := make([][2]string, 0, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		if r, ok := specs[f.Name]; ok {
			if !persistedRelation(r) {
				continue
			}
			fields = append(fields, [2]string{f.Name, relationColumnName(r)})
			continue
		}
		fields = append(fields, [2]string{f.Name, f.Name})
	}
	return fields
}

func (db *DB[T]) createSchemaFields() [][2]string {
	return schemaFieldsFor(reflect.TypeFor[T]())
}

func schemaStructFor(t reflect.Type) reflect.Value {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	specs, err := specsByField(t)
	if err != nil {
		panic(err)
	}
	fields := schemaFieldsFor(t)
	columns := make([]reflect.StructField, 0, len(fields))
	for _, pair := range fields {
		entityField, _ := t.FieldByName(pair[0])
		columnType := entityField.Type
		if r, ok := specs[pair[0]]; ok {
			columnType = relationColumnType(r)
		}
		columns = append(columns, reflect.StructField{Name: pair[1], Type: columnType})
	}

	return reflect.New(reflect.StructOf(columns)).Elem()
}

func (db *DB[T]) createSchemaStruct() reflect.Value {
	return schemaStructFor(reflect.TypeFor[T]())
}

func (c *dbCreator) createSchemaStruct(entity any) reflect.Value {
	return schemaStructFor(reflect.TypeOf(entity))
}

func (c *dbCreator) getSchemaFields(entity any) [][2]string {
	return schemaFieldsFor(reflect.TypeOf(entity))
}
