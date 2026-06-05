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

type Tx[T any] struct {
	pre map[int]*Resource[T]
	work map[int]*T
	n int
}

// DB is the in-memory state for one entity type. Construct via [Open].
type DB[T Entity] struct {
	Name         string
	Identifier   string
	Type         string
	counter      int
	store        *chunkStore[T] `gob:"-"`
	persistQueue []*T           `gob:"-"`
	deleteQueue  []*T           `gob:"-"`
	Indices      IndexMap[[]*T] `gob:"-"`
	Index        IndexMap[*T]   `gob:"-"`
	schemaFields [][2]string    `gob:"-"`
	schemaStruct reflect.Value  `gob:"-"`
	mu           sync.RWMutex   `gob:"-"`
	tx			 Tx[T] 			`gob:"-"`
	creator      *dbCreator 	`gob:"-"`
}

// Len returns the number of live (non-deleted) entities in the base.
func (gb *DB[T]) Len() int { return gb.store.Len() }

// All returns stable pointers to every live entity. The pointers remain
// valid for the life of the base (relations point at these same slots).
func (gb *DB[T]) All() []*T { return gb.store.Live() }

// DataDir is the folder where every base writes its .gob file. Override
// before the first call to [Register].
var DataDir = "./data"

// ErrEmptyEntity is returned by FindOneBy when the base has no entries.
var ErrEmptyEntity = errors.New("empty entity")

var baseRegistry = make(map[string]any)
var entityRegistry = make(map[string]any) // map[typeName]*chunkStore[T]

// GetEntityRegistry exposes the internal registry for debugging.
// Not part of the stable API.
func GetEntityRegistry() map[string]any { return entityRegistry }

// Register loads (or creates) the on-disk file for T and inflates each row
// into a *T entity. Must be called once per entity type, BEFORE any [Open]
// call — fillRelation needs every type registered to wire pointers.
func Register[T Entity]() error {
	name := reflect.TypeFor[T]().Name()

	if _, ok := entityRegistry[name]; ok {
		return fmt.Errorf("Base for that type already exist")
	}

	creator := &dbCreator{}
	baseSchema := creator.CreateDB(*new(T))

	res, err := readEntity[T](baseSchema)

	if err != nil {
		return err
	}

	entityRegistry[name] = res

	return nil
}

// Open returns a typed [DB] over the entities previously loaded by [Register].
// Call after every type used by relto / mapby tags has been registered.
func Open[T Entity]() *DB[T] {
	name := reflect.TypeFor[T]().Name()

	// One canonical DB per type: the first Open builds and registers it, later
	// calls hand back the same pointer. The engine reaches a type's metadata
	// and store by name through baseRegistry, so identity must be stable.
	if existing, ok := baseRegistry[name]; ok {
		return existing.(*DB[T])
	}

	initBase := &DB[T]{Identifier: "Id"}

	initBase.Name = name
	initBase.Indices = make(IndexMap[[]*T])
	initBase.Index = make(IndexMap[*T])
	initBase.schemaFields = initBase.createSchemaFields()

	if entity, ok := entityRegistry[name]; ok {
		initBase.store = entity.(*chunkStore[T])
	} else {
		log.Panicln("You cannot create base without registering a one")
	}

	initBase.fillRelation()
	initBase.initIndices()
	initBase.syncIdIndex()
	initBase.seedCounter()

	baseRegistry[name] = initBase

	return initBase
}

// seedCounter sets the auto-id counter to the largest id currently in the
// store, so inserts after a reload continue past the persisted ids instead of
// restarting at 1 (which would collide with — and silently patch — existing
// entities). One O(n) scan at load; inserts stay O(1) thereafter.
func (gb *DB[T]) seedCounter() {
	var maxId int
	gb.store.Range(func(p *T) {
		if id := (*p).GetId(); id > maxId {
			maxId = id
		}
	})
	gb.counter = maxId
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
	if !idField.CanSet() {
		panic("ID field is not settable")
	}
	switch idField.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		idField.SetInt(int64(id))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		idField.SetUint(uint64(id))
	default:
		panic("ID field must be an integer type")
	}
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
