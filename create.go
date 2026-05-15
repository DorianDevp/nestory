package nestory

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"reflect"
)

type goBaseCreator struct {}

func (gbc *goBaseCreator) CreateDB(entity any) any {
    schemaStruct := gbc.createSchemaStruct(entity)
    typeName := reflect.TypeOf(entity).Name()

    initBaseType := reflect.SliceOf(schemaStruct.Type())
    initBaseValue := reflect.MakeSlice(initBaseType, 0, 0)
    
    gob.Register(initBaseType)

    fileName := typeName + ".gob"
    filePath := fmt.Sprintf("%s/%s", DataDir, fileName)

    file, err := os.Open(filePath)
    if err != nil {
        fmt.Println("Error while opening a ", fileName, " with Error: ", err)

        if _, sErr := os.Stat(DataDir); os.IsNotExist(sErr) {
            os.MkdirAll(DataDir, 0700) 
            fmt.Println("Created dir: ", DataDir)
        }

        if file, createErr := os.Create(filePath); createErr != nil {
            file.Close()
            fmt.Println("Error while creating", fileName, " with Error: ", createErr)
            
            panic("CreatingBaseError")
        }

        fmt.Println("Successfully Created DB named: ", fileName)
        
        return initBaseValue.Interface()
    }
    defer file.Close()

    fileInfo, err := file.Stat()
    if err != nil {
        fmt.Println("Error getting file info:", err)

        panic("FileStatError")
    }

    if fileInfo.Size() == 0 {
        fmt.Println("DB file is empty. No data to decode.")

        return initBaseValue.Interface()
    }

    decoder := gob.NewDecoder(file)
    slicePtr := reflect.New(initBaseType) // This gives you a *slice

    derr := decoder.DecodeValue(slicePtr)
    if derr != nil {
        log.Panicln("Decoding failed during", initBaseType, "Error: ", derr)

        panic("DecodingBaseError")
    }

    return slicePtr.Elem().Interface()
}

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

        relTag := f.Tag.Get("relto")
        if relTag != "" {
            fName = fName + relTag
        }

        mapTag := f.Tag.Get("mapby")
        if mapTag != "" {
            continue
        }


        fields = append(fields, [2]string{name, fName})
    }

    return fields
}
