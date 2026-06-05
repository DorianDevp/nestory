package nestory

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"reflect"
)

// CreateDB opens the on-disk file for `entity` and decodes its content
// into a slice of flat schema rows. If the file does not exist, the file
// (and [DataDir] if needed) is created and an empty slice is returned.
func (gbc *dbCreator) CreateDB(entity any) any {
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
	slicePtr := reflect.New(initBaseType)

	if derr := decoder.DecodeValue(slicePtr); derr != nil {
		log.Panicln("Decoding failed during", initBaseType, "Error: ", derr)
	}

	return slicePtr.Elem().Interface()
}

// save writes the entity slice to disk atomically. The entire encoded
// payload goes to "<filepath>.tmp"; only after a successful fsync + close
// is the temp file renamed onto the target path. A crash mid-encode
// leaves the previous good file untouched.
func (gb *DB[T]) save() error {
	log.Println("Saving entity:", gb.TypeName())

	finalPath := gb.Filepath()
	tmpPath := finalPath + ".tmp"

	entitySchemaStruct := gb.createSchemaStruct()
	baseType := reflect.SliceOf(entitySchemaStruct.Type())
	baseValue := reflect.MakeSlice(baseType, 0, 0)

	gb.store.Range(func(p *T) {
		baseValue = reflect.Append(baseValue, gb.NormalizeToSchema(*p))
	})

	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("nestory: create %s: %w", tmpPath, err)
	}

	if err := gob.NewEncoder(file).Encode(baseValue.Interface()); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("nestory: encode %s: %w", tmpPath, err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("nestory: fsync %s: %w", tmpPath, err)
	}

	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("nestory: close %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("nestory: rename %s -> %s: %w", tmpPath, finalPath, err)
	}

	return nil
}
