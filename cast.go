package nestory

import (
	"fmt"
	"log"
	"reflect"
)

func readEntity[T Entity](base any) []T {
    creator := &goBaseCreator{}

    concreteEntity := make([]T, 0)
    if base == nil {
        return concreteEntity
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

        concreteEntity = append(concreteEntity, *zero)
    }

    return concreteEntity
}

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
            if  relFieldName == "" {
                panic("You cannot define field with pointer to struct type unless it is a relational field (look on relto tag)")
            }

            relField := currValue.FieldByName(relFieldName)
            if relField.Kind() != reflect.String && relField.Kind() != reflect.Int {
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

        relTag := f.Tag.Get("relto")
        if relTag != "" {
            fName = fName + relTag
        }

        mapTag := f.Tag.Get("mapby")
        if mapTag != "" {
            continue
        }

        field := [2]string{name, fName}
        fields = append(fields, field)
    }

    return fields
}

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
            if  relFieldName == "" {
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

		dynamicStruct := reflect.StructField{
			Name: schemaFieldName,
			Type: currType,
		}

		f = append(f, dynamicStruct)
	}

	refStruct := reflect.StructOf(f)

	schemaStruct := reflect.New(refStruct).Elem()

	return schemaStruct 
}

func (gb *DB[T]) isInterfaceSchemaCompliant(instance any) bool {
    t := reflect.TypeOf(instance)

    interfaceFields := make(map[string]bool)

    for i := range t.NumField() {
        fieldName := t.Field(i).Name
        interfaceFields[fieldName] = true
    }

    fields := gb.schemaFields

    for idx := range fields {
        schemaFieldName := fields[idx][1]

        if _, ok := interfaceFields[schemaFieldName]; !ok {
            return false
        }
    }

    return true
}
