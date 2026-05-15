package nestory

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
)

// Entity is the contract every nestory-persisted struct must satisfy.
// The Id is the primary key; nestory auto-assigns one on AddToPersistQueue
// if you leave it zero.
type Entity interface {
	GetId() int
}

// IndexMap is a two-level lookup: field name → field value → result.
// DB exposes two of these: Indices for one-to-many, Index for one-to-one.
type IndexMap[T any] map[string]map[any]T

// GoEntity is a typed slice alias — currently unused externally, kept
// for backwards compat. Will be removed in 1.0.
type GoEntity[T Entity] []T

// DB is the in-memory state for one entity type. Construct via [Open].
type DB[T Entity] struct {
	Name         string
	Identifier   string
	Type         string
	GoEntity     *[]T
	persistQueue []*T              `gob:"-"`
	deleteQueue  []*T              `gob:"-"`
	Indices      IndexMap[[]*T]    `gob:"-"`
	Index        IndexMap[*T]      `gob:"-"`
	schemaFields [][2]string       `gob:"-"`
	schemaStruct reflect.Value     `gob:"-"`
	mu           sync.RWMutex      `gob:"-"`
	creator      *goBaseCreator    `gob:"-"`
}

// DataDir is the folder where every base writes its .gob file. Override
// before the first call to [Register].
var DataDir = "./data"

// ErrEmptyEntity is returned by FindOneBy when the base has no entries.
var ErrEmptyEntity = errors.New("empty entity")

var baseRegistry = make(map[string]any)
var entityRegistry = make(map[string]any) // map[typeName]*[]T

// GetEntityRegistry exposes the internal registry for debugging.
// Not part of the stable API.
func GetEntityRegistry() map[string]any { return entityRegistry }

// Register loads (or creates) the on-disk file for T and inflates each row
// into a *T entity. Must be called once per entity type, BEFORE any [Open]
// call — fillRelation needs every type registered to wire pointers.
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

// Open returns a typed [DB] over the entities previously loaded by [Register].
// Call after every type used by relto / mapby tags has been registered.
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

// SetId writes id into the entity's Id field via reflection.
// Used internally by AddToPersistQueue; exported for callers that want to
// pre-assign Ids.
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
	if !idField.CanSet() {
		panic("ID field is not settable")
	}
	idField.SetInt(int64(id))
}

// Filename returns "<TypeName>.gob".
func (gb *DB[T]) Filename() string {
	return fmt.Sprint(gb.Name, ".gob")
}

// Filepath returns "<DataDir>/<TypeName>.gob".
func (gb *DB[T]) Filepath() string {
	return fmt.Sprintf("%s/%s", DataDir, gb.Filename())
}

// TypeName returns the Go type name of T without package prefix.
func (gb *DB[T]) TypeName() string {
	var t T
	return reflect.TypeOf(t).Name()
}
