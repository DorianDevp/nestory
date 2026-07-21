package nestory

import (
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"sync"
)

// Entity is the contract every persisted struct must satisfy. Id is the primary
// key; auto-assigned on Create if left zero.
type Entity interface {
	GetId() int
}

type indexMap[T any] map[string]map[any]T

type detachedRoot[T Entity] struct {
	id       int
	version  int
	original T
}

// DB is the in-memory state for one entity type. Construct via [Open].
type DB[T Entity] struct {
	name         string
	identifier   string
	counter      int
	store        *chunkStore[T]
	persistQueue []*T
	deleteQueue  []*T
	indices      indexMap[[]*T] // o2m index (not populated yet)
	index        indexMap[*T]   // o2o index, keyed by field then value
	schemaFields [][2]string
	directSchema bool
	mu           sync.RWMutex
	resById      map[int]*resourceSlot[T] // id → stable resourceSlot slot
	wal          *wal                     // durability log for commits
	snapshotMu   sync.Mutex
	snapshots    map[*T]detachedRoot[T]
}

var _ committer = (*DB[Entity])(nil)

// Len returns the number of live entities.
func (db *DB[T]) Len() int {
	graphMu.RLock()
	defer graphMu.RUnlock()

	return db.store.Len()
}

// DataDir is where every base writes its files. Override before [Register].
var DataDir = "./data"

var (
	baseRegistry  = make(map[string]any)
	storeRegistry = make(map[string]any) // typeName → *chunkStore[T]
)

// Register loads T's chunk files and inflates each row into a *T. Call once per
// type, before any [Open] — fillRelation needs every type registered to wire
// pointers.
func Register[T Entity]() error {
	t := reflect.TypeFor[T]()
	name := t.Name()

	if _, ok := storeRegistry[name]; ok {
		return fmt.Errorf("nestory: base for %s is already registered", t)
	}

	if _, err := buildRelationSchema(t); err != nil {
		return err
	}

	store, err := loadStore[T]()
	if err != nil {
		return err
	}

	storeRegistry[name] = store

	return nil
}

// Open returns a typed [DB] over the entities loaded by [Register]. Call after
// every type used by rel has been registered.
func Open[T Entity]() *DB[T] {
	name := reflect.TypeFor[T]().Name()

	// One canonical DB per type — the engine reaches a type by name through
	// baseRegistry, so identity must be stable.
	if existing, ok := baseRegistry[name]; ok {
		return existing.(*DB[T])
	}

	initBase := &DB[T]{identifier: "Id"}

	initBase.name = name
	initBase.indices = make(indexMap[[]*T])
	initBase.index = make(indexMap[*T])
	initBase.resById = make(map[int]*resourceSlot[T])
	initBase.snapshots = make(map[*T]detachedRoot[T])
	initBase.schemaFields = initBase.createSchemaFields()
	specs, err := relationSpecs(reflect.TypeFor[T]())
	if err != nil {
		panic(err)
	}

	initBase.directSchema = len(specs) == 0

	if entity, ok := storeRegistry[name]; ok {
		initBase.store = entity.(*chunkStore[T])
	} else {
		log.Panicln("You cannot create base without registering a one")
	}

	initBase.wal = openWAL(filepath.Join(initBase.chunkDir(), "wal.log"))

	initBase.fillRelation()
	initBase.initIndices()
	initBase.syncIdIndex()
	initBase.syncResById()
	initBase.seedCounter()

	baseRegistry[name] = initBase
	resetCommittedOwnership()

	return initBase
}

// syncResById rebuilds resById from the store. Called once from Open; commits
// keep it in step thereafter and structural deletes drop removed ids.
func (db *DB[T]) syncResById() {
	db.store.rangeResources(func(r *resourceSlot[T]) {
		db.resById[(*r.item).GetId()] = r
	})
}

// seedCounter sets the auto-id counter to the largest persisted id, so inserts
// after a reload continue past it instead of colliding from 1. One O(n) scan.
func (db *DB[T]) seedCounter() {
	var maxId int
	db.store.Range(func(p *T) {
		if id := (*p).GetId(); id > maxId {
			maxId = id
		}
	})

	db.counter = maxId
}

func setID[T any](entity *T, id int) {
	val := reflect.ValueOf(entity)

	if val.Kind() != reflect.Pointer || val.IsNil() {
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

// TypeName returns the Go type name of T without package prefix.
func (db *DB[T]) TypeName() string {
	var t T
	return reflect.TypeOf(t).Name()
}

// resource resolves the stable resourceSlot slot for id, guarding resById against
// a concurrent Add. The returned pointer is stable for the store's life.
func (db *DB[T]) resource(id int) (*resourceSlot[T], bool) {
	db.mu.RLock()
	r, ok := db.resById[id]
	db.mu.RUnlock()

	return r, ok
}

// committer — driven by the transactionEngine. lockResource/unlockResource own the per-row
// mutex; the rest assume it's already held.

func (db *DB[T]) lockResource(id int) {
	if r, ok := db.resource(id); ok {
		r.mu.Lock()
	}
}

func (db *DB[T]) unlockResource(id int) {
	if r, ok := db.resource(id); ok {
		r.mu.Unlock()
	}
}

func (db *DB[T]) readLockResource(id int) {
	if resource, found := db.resource(id); found {
		resource.mu.RLock()
	}
}

func (db *DB[T]) readUnlockResource(id int) {
	if resource, found := db.resource(id); found {
		resource.mu.RUnlock()
	}
}

func (db *DB[T]) resourceVersion(id int) (int, bool) {
	if r, ok := db.resource(id); ok {
		return r.version, true
	}

	return 0, false
}

func (db *DB[T]) snapshotResource(id int) (reflect.Value, reflect.Value, int, bool) {
	r, ok := db.resource(id)
	if !ok {
		return reflect.Value{}, reflect.Value{}, 0, false
	}

	source := reflect.ValueOf(r.item)

	return cloneEntityPointer(source), cloneEntityPointer(source), r.version, true
}

func (db *DB[T]) applyWrite(id int, work any) {
	if r, ok := db.resource(id); ok {
		*r.item = *work.(*T) // write through the stable pointer — never moves
		r.version++
		db.store.markDirty(r.chunk)
	}
}

func (db *DB[T]) refreshSnapshot(id int, work any) {
	if r, ok := db.resource(id); ok {
		target := work.(*T)
		*target = *r.item
		cloneSliceFields(reflect.ValueOf(target).Elem())
	}
}

func cloneSliceFields(value reflect.Value) {
	for i := range value.NumField() {
		field := value.Field(i)
		if field.Kind() != reflect.Slice || field.IsNil() || !field.CanSet() {
			continue
		}

		clone := reflect.MakeSlice(field.Type(), field.Len(), field.Len())
		reflect.Copy(clone, field)
		field.Set(clone)
	}
}

func (db *DB[T]) logWrites(items []pendingWrite) error {
	rec := walFrame{Rows: make([]walRow, 0, len(items))}

	for _, it := range items {
		entity := *it.work.(*T)
		row := reflect.ValueOf(entity)
		if !db.directSchema {
			row = db.normalizeToSchema(entity)
		}

		rowBytes, err := encodeRow(row)
		if err != nil {
			return err
		}

		rec.Rows = append(rec.Rows, walRow{Id: int64(it.id), Row: rowBytes})
	}

	return db.wal.appendFrame(rec)
}
