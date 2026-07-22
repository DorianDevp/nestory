package nestory

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var ErrUniqueViolation = errors.New("nestory: unique index violation")

const secondaryIndexBatchThreshold = 64

type secondaryIndexField struct {
	name  string
	index int
	typ   reflect.Type
	order int
}

type secondaryIndexSpec struct {
	name        string
	fields      []secondaryIndexField
	unique      bool
	lookupField string
}

type secondaryIndexBuilder struct {
	name      string
	fields    map[int]secondaryIndexField
	unique    bool
	uniqueSet bool
}

type cachedSecondaryIndexSpecs struct {
	specs []secondaryIndexSpec
	err   error
}

var secondaryIndexCache sync.Map

func secondaryIndexSpecs(typ reflect.Type) ([]secondaryIndexSpec, error) {
	if cached, found := secondaryIndexCache.Load(typ); found {
		result := cached.(cachedSecondaryIndexSpecs)

		return result.specs, result.err
	}

	specs, err := parseSecondaryIndexSpecs(typ)
	secondaryIndexCache.Store(typ, cachedSecondaryIndexSpecs{specs: specs, err: err})

	return specs, err
}

func parseSecondaryIndexSpecs(typ reflect.Type) ([]secondaryIndexSpec, error) {
	builders := make(map[string]*secondaryIndexBuilder)
	var specs []secondaryIndexSpec
	for index := range typ.NumField() {
		field := typ.Field(index)
		if field.Tag.Get("key") == "unique" {
			if err := validateSecondaryIndexField(typ, field); err != nil {
				return nil, err
			}

			specs = append(specs, secondaryIndexSpec{
				name: field.Name, unique: true, lookupField: field.Name,
				fields: []secondaryIndexField{{name: field.Name, index: index, typ: field.Type, order: 1}},
			})
		}

		raw := field.Tag.Get("index")
		if raw == "" {
			continue
		}

		parts := strings.Split(raw, ",")
		if len(parts) < 2 || len(parts) > 3 || parts[0] == "" {
			return nil, fmt.Errorf("nestory: %s.%s has invalid index tag %q", typ, field.Name, raw)
		}

		position, err := strconv.Atoi(parts[1])
		if err != nil || position < 1 {
			return nil, fmt.Errorf("nestory: %s.%s has invalid index position %q", typ, field.Name, parts[1])
		}

		if err := validateSecondaryIndexField(typ, field); err != nil {
			return nil, err
		}

		unique := len(parts) == 3 && parts[2] == "unique"
		if len(parts) == 3 && !unique {
			return nil, fmt.Errorf("nestory: %s.%s has invalid index option %q", typ, field.Name, parts[2])
		}

		builder := builders[parts[0]]
		if builder == nil {
			builder = &secondaryIndexBuilder{name: parts[0], fields: make(map[int]secondaryIndexField)}
			builders[parts[0]] = builder
		}

		if _, duplicate := builder.fields[position]; duplicate {
			return nil, fmt.Errorf("nestory: %s index %q repeats position %d", typ, builder.name, position)
		}

		if builder.uniqueSet && builder.unique != unique {
			return nil, fmt.Errorf("nestory: %s index %q has inconsistent unique options", typ, builder.name)
		}

		builder.unique = unique
		builder.uniqueSet = true
		builder.fields[position] = secondaryIndexField{
			name: field.Name, index: index, typ: field.Type, order: position,
		}
	}

	for _, builder := range builders {
		fields := make([]secondaryIndexField, 0, len(builder.fields))
		for _, field := range builder.fields {
			fields = append(fields, field)
		}

		slices.SortFunc(fields, func(left, right secondaryIndexField) int {
			return cmp.Compare(left.order, right.order)
		})
		for index, field := range fields {
			if field.order != index+1 {
				return nil, fmt.Errorf("nestory: %s index %q is missing position %d", typ, builder.name, index+1)
			}
		}

		lookupField := ""
		if builder.unique && len(fields) == 1 {
			lookupField = fields[0].name
		}

		specs = append(specs, secondaryIndexSpec{
			name: builder.name, fields: fields, unique: builder.unique, lookupField: lookupField,
		})
	}

	slices.SortFunc(specs, func(left, right secondaryIndexSpec) int {
		return cmp.Compare(left.name, right.name)
	})

	for index := 1; index < len(specs); index++ {
		if specs[index-1].name == specs[index].name {
			return nil, fmt.Errorf("nestory: %s repeats index name %q", typ, specs[index].name)
		}
	}

	return specs, nil
}

func validateSecondaryIndexField(owner reflect.Type, field reflect.StructField) error {
	switch field.Type.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.String:
		return nil
	default:
		return fmt.Errorf("nestory: %s.%s cannot be indexed", owner, field.Name)
	}
}

type secondaryIndex[T Entity] struct {
	spec    secondaryIndexSpec
	entries []*T
	keys    map[string]int
}

func newSecondaryIndex[T Entity](spec secondaryIndexSpec) *secondaryIndex[T] {
	index := &secondaryIndex[T]{spec: spec}
	if spec.unique {
		index.keys = make(map[string]int)
	}

	return index
}

func (db *DB[T]) hasSecondaryIndexes() bool {
	return len(db.secondary) > 0
}

func (db *DB[T]) rebuildSecondaryIndices() error {
	indices := make(map[string]*secondaryIndex[T], len(db.secondary))
	for name, current := range db.secondary {
		indices[name] = newSecondaryIndex[T](current.spec)
	}

	db.store.Range(func(entity *T) {
		for _, index := range indices {
			index.entries = append(index.entries, entity)
		}
	})

	for _, index := range indices {
		slices.SortFunc(index.entries, func(left, right *T) int {
			return index.compare(left, right)
		})
		if err := index.rebuildKeys(); err != nil {
			return err
		}
	}

	db.secondary = indices
	for field := range db.index {
		if field != "Id" {
			delete(db.index, field)
		}
	}

	for _, index := range db.secondary {
		if index.spec.lookupField == "" {
			continue
		}

		lookup := make(map[any]*T, len(index.entries))
		for _, entity := range index.entries {
			lookup[reflect.ValueOf(*entity).Field(index.spec.fields[0].index).Interface()] = entity
		}

		db.index[index.spec.lookupField] = lookup
	}

	return nil
}

func (index *secondaryIndex[T]) rebuildKeys() error {
	if !index.spec.unique {
		return nil
	}

	for _, entity := range index.entries {
		key := index.key(entity)
		id := (*entity).GetId()
		if existing, duplicate := index.keys[key]; duplicate && existing != id {
			return fmt.Errorf("%w: index %s", ErrUniqueViolation, index.spec.name)
		}

		index.keys[key] = id
	}

	return nil
}

func (index *secondaryIndex[T]) compare(left, right *T) int {
	leftValue := reflect.ValueOf(*left)
	rightValue := reflect.ValueOf(*right)
	for _, field := range index.spec.fields {
		if order := compareIndexValues(leftValue.Field(field.index), rightValue.Field(field.index)); order != 0 {
			return order
		}
	}

	return cmp.Compare((*left).GetId(), (*right).GetId())
}

func compareIndexValues(left, right reflect.Value) int {
	switch left.Kind() {
	case reflect.Bool:
		return cmp.Compare(boolByte(left.Bool()), boolByte(right.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return cmp.Compare(left.Int(), right.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return cmp.Compare(left.Uint(), right.Uint())
	case reflect.String:
		return cmp.Compare(left.String(), right.String())
	default:
		panic("nestory: unsupported secondary index field")
	}
}

func boolByte(value bool) byte {
	if value {
		return 1
	}

	return 0
}

func (index *secondaryIndex[T]) key(entity *T) string {
	value := reflect.ValueOf(*entity)
	var buffer bytes.Buffer
	for _, field := range index.spec.fields {
		writeIndexKey(&buffer, value.Field(field.index))
	}

	return buffer.String()
}

func writeIndexKey(buffer *bytes.Buffer, value reflect.Value) {
	var encoded [8]byte
	switch value.Kind() {
	case reflect.Bool:
		buffer.WriteByte(boolByte(value.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		binary.BigEndian.PutUint64(encoded[:], uint64(value.Int()))
		buffer.Write(encoded[:])
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		binary.BigEndian.PutUint64(encoded[:], value.Uint())
		buffer.Write(encoded[:])
	case reflect.String:
		binary.BigEndian.PutUint64(encoded[:], uint64(value.Len()))
		buffer.Write(encoded[:])
		buffer.WriteString(value.String())
	default:
		panic("nestory: unsupported secondary index field")
	}
}

func (db *DB[T]) validateIndexes(items []pendingWrite) error {
	if len(db.secondary) == 0 {
		return nil
	}

	changed := make(map[int]struct{}, len(items))
	for _, item := range items {
		changed[item.id] = struct{}{}
	}

	for _, index := range db.secondary {
		if !index.spec.unique {
			continue
		}

		pending := make(map[string]int, len(items))
		for _, item := range items {
			if item.deleted {
				continue
			}

			entity := item.work.(*T)
			key := index.key(entity)
			if existing, found := index.keys[key]; found && existing != item.id {
				if _, moving := changed[existing]; !moving {
					return fmt.Errorf("%w: index %s", ErrUniqueViolation, index.spec.name)
				}
			}

			if existing, found := pending[key]; found && existing != item.id {
				return fmt.Errorf("%w: index %s", ErrUniqueViolation, index.spec.name)
			}

			pending[key] = item.id
		}
	}

	return nil
}

func (db *DB[T]) prepareIndexes(items []pendingWrite) {
	if len(db.secondary) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if len(items) >= secondaryIndexBatchThreshold {
		db.prepareIndexBatch(items)

		return
	}

	for _, item := range items {
		if resource, found := db.resById[item.id]; found {
			db.removeSecondaryIndices(resource.item)
		}
	}
}

func (db *DB[T]) prepareIndexBatch(items []pendingWrite) {
	changed := make(map[int]struct{}, len(items))
	for _, item := range items {
		changed[item.id] = struct{}{}
		if resource, found := db.resById[item.id]; found {
			db.removeSecondaryIndexLookups(resource.item)
		}
	}

	for _, index := range db.secondary {
		index.entries = slices.DeleteFunc(index.entries, func(entity *T) bool {
			_, remove := changed[(*entity).GetId()]

			return remove
		})
	}
}

func (db *DB[T]) finishIndexes(items []pendingWrite) {
	if len(db.secondary) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if len(items) >= secondaryIndexBatchThreshold {
		db.finishIndexBatch(items)

		return
	}

	for _, item := range items {
		if item.deleted {
			continue
		}

		if resource, found := db.resById[item.id]; found {
			db.addSecondaryIndices(resource.item)
		}
	}
}

func (db *DB[T]) finishIndexBatch(items []pendingWrite) {
	additions := make([]*T, 0, len(items))
	for _, item := range items {
		if item.deleted {
			continue
		}

		resource, found := db.resById[item.id]
		if !found {
			continue
		}

		additions = append(additions, resource.item)
		db.addSecondaryIndexLookups(resource.item)
	}

	for _, index := range db.secondary {
		ordered := append([]*T(nil), additions...)
		slices.SortFunc(ordered, index.compare)
		index.entries = mergeSecondaryIndexEntries(index, ordered)
	}
}

func mergeSecondaryIndexEntries[T Entity](index *secondaryIndex[T], additions []*T) []*T {
	if len(additions) == 0 {
		return index.entries
	}

	if len(index.entries) == 0 {
		return additions
	}

	merged := make([]*T, 0, len(index.entries)+len(additions))
	existingPosition := 0
	additionPosition := 0
	for existingPosition < len(index.entries) && additionPosition < len(additions) {
		if index.compare(index.entries[existingPosition], additions[additionPosition]) <= 0 {
			merged = append(merged, index.entries[existingPosition])
			existingPosition++
			continue
		}

		merged = append(merged, additions[additionPosition])
		additionPosition++
	}

	merged = append(merged, index.entries[existingPosition:]...)
	merged = append(merged, additions[additionPosition:]...)

	return merged
}

func (db *DB[T]) removeSecondaryIndices(entity *T) {
	db.removeSecondaryIndexLookups(entity)
	for _, index := range db.secondary {
		id := (*entity).GetId()
		position := sort.Search(len(index.entries), func(position int) bool {
			return index.compare(index.entries[position], entity) >= 0
		})
		for position < len(index.entries) && index.compare(index.entries[position], entity) == 0 {
			if (*index.entries[position]).GetId() == id {
				index.entries = slices.Delete(index.entries, position, position+1)
				break
			}

			position++
		}
	}
}

func (db *DB[T]) removeSecondaryIndexLookups(entity *T) {
	for _, index := range db.secondary {
		if index.spec.unique {
			delete(index.keys, index.key(entity))
		}

		if index.spec.lookupField != "" {
			value := reflect.ValueOf(*entity).Field(index.spec.fields[0].index).Interface()
			delete(db.index[index.spec.lookupField], value)
		}
	}
}

func (db *DB[T]) addSecondaryIndices(entity *T) {
	db.addSecondaryIndexLookups(entity)
	for _, index := range db.secondary {
		position := sort.Search(len(index.entries), func(position int) bool {
			return index.compare(index.entries[position], entity) >= 0
		})
		index.entries = slices.Insert(index.entries, position, entity)
	}
}

func (db *DB[T]) addSecondaryIndexLookups(entity *T) {
	for _, index := range db.secondary {
		if index.spec.unique {
			index.keys[index.key(entity)] = (*entity).GetId()
		}

		if index.spec.lookupField != "" {
			value := reflect.ValueOf(*entity).Field(index.spec.fields[0].index).Interface()
			db.index[index.spec.lookupField][value] = entity
		}
	}
}

func (index *secondaryIndex[T]) rangePrefix(prefix []any) ([]*T, error) {
	start, end, err := index.prefixBounds(prefix)
	if err != nil {
		return nil, err
	}

	return index.entries[start:end], nil
}

func (index *secondaryIndex[T]) rangeAfter(prefix []any, after any) ([]*T, error) {
	if len(prefix) >= len(index.spec.fields) {
		return nil, fmt.Errorf(
			"nestory: index %s has no cursor field after a %d-value prefix",
			index.spec.name,
			len(prefix),
		)
	}

	cursor, err := index.indexValue(after, len(prefix), "cursor")
	if err != nil {
		return nil, err
	}

	start, end, err := index.prefixBounds(prefix)
	if err != nil {
		return nil, err
	}

	field := index.spec.fields[len(prefix)]
	offset := sort.Search(end-start, func(position int) bool {
		row := reflect.ValueOf(*index.entries[start+position])

		return compareIndexValues(row.Field(field.index), cursor) > 0
	})

	return index.entries[start+offset : end], nil
}

func (index *secondaryIndex[T]) prefixBounds(prefix []any) (int, int, error) {
	if len(prefix) > len(index.spec.fields) {
		return 0, 0, fmt.Errorf(
			"nestory: index %s accepts at most %d prefix values",
			index.spec.name,
			len(index.spec.fields),
		)
	}

	values := make([]reflect.Value, len(prefix))
	for position, raw := range prefix {
		value, err := index.indexValue(raw, position, "prefix")
		if err != nil {
			return 0, 0, err
		}

		values[position] = value
	}

	comparePrefix := func(entity *T) int {
		row := reflect.ValueOf(*entity)
		for position, value := range values {
			if order := compareIndexValues(row.Field(index.spec.fields[position].index), value); order != 0 {
				return order
			}
		}

		return 0
	}
	start := sort.Search(len(index.entries), func(position int) bool {
		return comparePrefix(index.entries[position]) >= 0
	})
	end := sort.Search(len(index.entries), func(position int) bool {
		return comparePrefix(index.entries[position]) > 0
	})

	return start, end, nil
}

func (index *secondaryIndex[T]) indexValue(raw any, position int, label string) (reflect.Value, error) {
	if raw == nil {
		return reflect.Value{}, fmt.Errorf("nestory: index %s %s %d is nil", index.spec.name, label, position)
	}

	value := reflect.ValueOf(raw)
	want := index.spec.fields[position].typ
	if value.Type() != want {
		return reflect.Value{}, fmt.Errorf(
			"nestory: index %s %s %d has type %s, want %s",
			index.spec.name,
			label,
			position,
			value.Type(),
			want,
		)
	}

	return value, nil
}
