package nestory

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"slices"
	"sync"
)

type Entity interface{
    GetId() int
}

type IndexMap[T any] map[string]map[any]T

type GoEntity[T Entity] []T

type DB[T Entity] struct {
	Name         string
	Identifier   string
	Type         string
	GoEntity     *[]T
	persistQueue []*T              `gob:"-"`
	deleteQueue  []*T              `gob:"-"`
	Indices 	 IndexMap[[]*T] `gob:"-"`
	Index 	     IndexMap[*T] `gob:"-"`
	schemaFields [][2]string       `gob:"-"`
	schemaStruct reflect.Value     `gob:"-"`
	mu           sync.RWMutex      `gob:"-"`
	creator      *goBaseCreator    `gob:"-"`
}

var DataDir = "./data"

var ErrEmptyEntity = errors.New("empty entity")

func SetId[T any](entity *T, id int) {
	val := reflect.ValueOf(entity)

	if val.Kind() != reflect.Ptr || val.IsNil() {
		panic("expected a non-nil pointer")
	}

	val = val.Elem()
	idField := val.FieldByName("Id")
	if !idField.IsValid() {
		panic("ID field not found")
	}
	if idField.Kind() != reflect.Int {
		panic("ID field is not of type int")
	}
	if idField.CanSet() {
		idField.SetInt(int64(id))
		return
	}
	panic("ID field is not settable")
}

func (gb *DB[T]) Filename() string {
	filename := fmt.Sprint(gb.Name, ".gob")

	return filename
}

func (gb *DB[T]) Filepath() string {
	filePath := fmt.Sprintf("%s/%s", DataDir, gb.Filename())

	return filePath
}

func (gb *DB[T]) TypeName() string {
	var t T
	typeOf := reflect.TypeOf(t).Name()

	return typeOf
}

var baseRegistry = make(map[string]any)
var entityRegistry = make(map[string]any) // []T

func GetEntityRegistry() map[string]any {
    return entityRegistry
}

func Register[T Entity]() {
    instance := new(T)
    name := reflect.TypeOf(instance).Elem().Name()

    if _, ok := entityRegistry[name]; ok {
        log.Panicln("Base for that type already exist", name)
    }

    creator := &goBaseCreator{}

    baseSchema := creator.CreateDB(*new(T))
    entity := readEntity[T](baseSchema)

    entityRegistry[name] = &entity
}

func Open[T Entity]() *DB[T] {
	initBase := DB[T]{Identifier: "Id"}

	initBase.Name = initBase.TypeName()
	initBase.Indices = make(IndexMap[[]*T])
	initBase.Index = make(IndexMap[*T])

	initBase.schemaFields = initBase.createSchemaFields()

    if entity, ok := entityRegistry[initBase.Name]; ok {
        test := entity.(*[]T)
        initBase.GoEntity = test
    } else {
        log.Panicln("You cannot create base without registering a one")
    }

    initBase.fillRelation()
	initBase.initIndices()
	initBase.syncIdIndex()

	return &initBase
}

func (gb *DB[T]) fillRelation() {
    relFields := make(map[string]string)
    mapFields := make(map[string]string)

    t := reflect.TypeOf(*new(T))
    for idx := range t.NumField() {
        f := t.Field(idx)
        relto := f.Tag.Get("relto")
        if relto != "" {
            relFields[f.Name] = relto
        }

        mapby := f.Tag.Get("mapby")
        if mapby != "" {
            mapFields[f.Name] = mapby 
        }
    }

    for idx := range *gb.GoEntity {
        el := &(*gb.GoEntity)[idx]
        v :=  reflect.ValueOf(el)
        t :=  reflect.TypeOf(*el)

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
                log.Println("No relation found for field", tf.Name, "with value", f.Interface(), "; ", " - skipping")

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
				log.Panicf("Field's value does not match to any of related field in the specified entity\n Rel field: %s \n; Entity fields value %s\n", relto, f.Elem().FieldByName(relto),
				)
			}
        }

        for fieldName, mapby := range mapFields {
			log.Printf("\n\n Many To One \n\n")

			id := v.Elem().FieldByName("Id").Interface()

            f := v.Elem().FieldByName(fieldName)
            tf, ok := t.FieldByName(fieldName)
            if !ok {
                log.Panicln("No field", fieldName)
            }

            if f.Kind() != reflect.Slice && f.Type().Elem().Kind() != reflect.Pointer {
                log.Panic("All mapby fields must by a slice of pointers")
            }

            if f.IsZero() {
				log.Println("test", tf.Name, " - skipping")
            }

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
				log.Printf("MapBy Field value does not match to any of related field in the specified entity. Leaving value empty.\n MapBy field: %s \n; Entity fields value %i\n", mapby, id,
				)
			}
        }
    }
}

func (gb *DB[T]) Add(entity *T) {
	gb.mu.Lock()
	defer gb.mu.Unlock()

    _ent := *entity
	*gb.GoEntity = append(*gb.GoEntity, _ent)
}

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

func (gb *DB[T]) QueueDelete(id any)  (error) {
	if instance, ok := gb.Index["Id"][id]; !ok {
		return fmt.Errorf("There is no item with given Id %d\n", id)
	} else {
        for _, q := range gb.deleteQueue {
            if (*q).GetId() == id {
                return nil
            }
        }

        gb.deleteQueue = append(gb.deleteQueue, instance)

        return nil
    }
}

func (gb *DB[T]) PatchById(id any, item T) (*T, error) {
	baseInstance, ok := gb.Index["Id"][id]
	if !ok {
		return nil, fmt.Errorf("There is no item with given Id %d\n", id)
	}

	err := merge(baseInstance, item)
	if err != nil {
		return nil, err
	}

	return baseInstance, nil
}

func (gb *DB[T]) resetDeleteQueue() {
    gb.deleteQueue = []*T{}
}

func (gb *DB[T]) Flush() error {
	for _, ent := range gb.persistQueue {
		entity := *ent
		id := entity.GetId()

		if _ent, err := gb.FindOneBy("Id", id); _ent != nil{
			gb.PatchById(id, entity)
			continue
		} else if err != nil && err != ErrEmptyEntity {
			log.Panicln(err)
		}

		gb.Add(ent)
	}

	gb.ResetpersistQueue()
	gb.syncIdIndex()

    _ent := gb.GoEntity
    for _, instance := range gb.deleteQueue {
        i := *instance
        for idx, _instance := range *_ent {
            if _instance.GetId() == i.GetId() {
                *_ent = slices.Delete(*_ent, idx, idx+1)
                break
            }
        }
    }

    gb.resetDeleteQueue()

    // End of rewrite
    gb.GoEntity = _ent    

    log.Println("Saved entity", gb.TypeName(), *gb.GoEntity)

	fmt.Printf("PERSIST QUEUE (should be empty): %+v\n", gb.persistQueue)

    err := gb.save()
    if err != nil {
        log.Panicln("Panic during saving a base", gb.TypeName(), err)
    }

	fmt.Println("Data successfully written to file:", gb.Filepath())

	return nil
}

func (gb *DB[T]) save() error {
    log.Println("Saving entity:", gb.TypeName())

	file, err := os.Create(gb.Filepath())
	if err != nil {
		fmt.Println("Error while creating/opening file:", err)

		return err
	}
	defer file.Close()

    // @TODO: think why []any would not work

    entitySchemaStruct := gb.createSchemaStruct()
    baseType := reflect.SliceOf(entitySchemaStruct.Type())
    baseValue := reflect.MakeSlice(baseType, 0, 0)

    for _, entity := range *gb.GoEntity {
        abstractStruct := gb.NormalizeToSchema(entity) // reflect.Value

        baseValue = reflect.Append(baseValue, abstractStruct)
    }

	if err := gob.NewEncoder(file).Encode(baseValue.Interface()); err != nil {
		fmt.Println("Error while encoding data to file:", err)

		return err
	}

    return nil
}

func (gb *DB[T]) AddToPersistQueue(entity *T) error {
	gb.persistQueue = append(gb.persistQueue, entity)

	id := (*entity).GetId()
	if id != 0 {
		fmt.Println("Duplicate")
		e, _ := gb.FindOneBy("Id", id)
		if e != nil {
			return nil
		}
	}

	entityLen := len(*gb.GoEntity) 
	if entityLen == 0 {
		SetId(entity, entityLen+1)
		gb.Index["Id"][(*entity).GetId()] = entity

		return nil
	} 

	lastEl := (*gb.GoEntity)[entityLen - 1]

	if lastEl.GetId() > entityLen {
		SetId(entity, lastEl.GetId() + 1)
		gb.Index["Id"][(*entity).GetId()] = entity
	} else {
		SetId(entity, entityLen + 1)
		gb.Index["Id"][(*entity).GetId()] = entity
	}

	return nil
}

func (gb *DB[T]) ResetpersistQueue() {
	gb.persistQueue = []*T{}
}

func (gb *DB[T]) Filter(filterFn func(T) bool) []T {
	var filteredSlice []T

	for _, item := range *gb.GoEntity {
		if filterFn(item) {
			filteredSlice = append(filteredSlice, item)
		}
	}

	return filteredSlice
}

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

func (gb *DB[T]) initIndices() {
	baseType := reflect.TypeOf(*gb.GoEntity).Elem()
	if baseType.Kind() == reflect.Ptr {
		baseType = baseType.Elem()
	}

    for i := range baseType.NumField() {
		fieldName := baseType.Field(i).Name
		gb.Index[fieldName] = make(map[any]*T)
	}
}

func (gb *DB[T]) syncIdIndex() {
	for idx, el := range *gb.GoEntity {
		if _, ok := gb.Index["Id"][el.GetId()]; !ok {
			gb.Index["Id"][el.GetId()] = &(*gb.GoEntity)[idx]

			continue
		}
	}	

	for key, val := range gb.Index["Id"]  {
		if val == nil {
			delete(gb.Index["Id"], key)
		}
	}
}

// merger passed value to mergee
func merge[T any](target *T, merger T) error {
    targetVal := reflect.ValueOf(target).Elem()

    mergerVal := reflect.ValueOf(merger)
    mergerType  := reflect.TypeOf(merger)

    if targetVal.Kind() != reflect.Struct || mergerVal.Kind() != reflect.Struct {
        return fmt.Errorf("merge: target and merger must be structs")
    }

    for i := range mergerVal.NumField() {
        mergerField := mergerVal.Field(i)
		log.Println("Fieldname:", mergerType.Field(i).Name)
        targetField := targetVal.FieldByName(mergerType.Field(i).Name)

		mergerFieldType := mergerType.Field(i)
		fieldKeyType := mergerFieldType.Tag.Get("key") 

		if fieldKeyType == "primary" || !targetField.CanSet() {
			log.Println("Cannot merge field", mergerFieldType.Name, "because it is primary or not settable")
			continue
		}

        if (targetField.Kind() == reflect.Ptr && !mergerField.IsNil()) {
			log.Println("Merged field with name:", mergerType.Field(i).Name)
			targetField.Set(mergerField)
			continue
        } 

        if (!mergerField.IsZero()) {
			log.Println("Merged field with name:", targetField.Type().Name())
			targetField.Set(mergerField)
        } 
    }

    return nil
}
