package nestory

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// relationKind describes the lifecycle role of a relation. Cardinality is
// deliberately not part of the kind: it is derived from *T versus []*T.
type relationKind string

const (
	ownRelation     relationKind = "own"
	ownedByRelation relationKind = "ownedby"
	borrowRelation  relationKind = "borrow"
	optionRelation  relationKind = "option"
	inverseRelation relationKind = "inverse"
)

var (
	ErrRelationSchema    = errors.New("nestory: invalid relation schema")
	ErrRelationInvariant = errors.New("nestory: relation invariant violated")
	ErrDeleteRestricted  = errors.New("nestory: delete restricted by relation")
)

// relationRoot is the normalized relation schema reachable from one entity
// type. Cycles are represented by pointers back to an already-created root.
type relationRoot struct {
	Type     reflect.Type
	Children []relationPoint
}

// relationPoint is one declared relation field.
type relationPoint struct {
	FieldName string
	FieldType reflect.Type // target struct type (never *T or []*T)
	Field     string       // second argument of rel:"role,field"
	Many      bool
	To        *relationNode
}

type relationNode struct {
	RelationType relationKind
	To           *relationRoot
}

type relationSpec struct {
	owner      reflect.Type
	fieldIndex int
	fieldName  string
	target     reflect.Type
	kind       relationKind
	matchField string
	many       bool
}

type relationFieldKey struct {
	owner reflect.Type
	field int
}

// relationWireIndex is a short-lived index for one graph rewire. Building it
// once keeps rehydration linear in nodes and edges instead of scanning a
// foreign store for every relation field.
type relationWireIndex struct {
	targets map[relationTargetKey]reflect.Value
	slices  map[relationFieldKey]map[any][]reflect.Value
}

var entityInterface = reflect.TypeFor[Entity]()

type cachedRelationSpecs struct {
	specs []relationSpec
	err   error
}

var relationSpecsCache sync.Map

func relationTarget(t reflect.Type) (target reflect.Type, many, ok bool) {
	switch t.Kind() {
	case reflect.Pointer:
		target = t.Elem()
	case reflect.Slice:
		if t.Elem().Kind() != reflect.Pointer {
			return nil, false, false
		}

		target, many = t.Elem().Elem(), true
	default:
		return nil, false, false
	}

	return target, many, target.Kind() == reflect.Struct
}

func isEntityType(t reflect.Type) bool {
	return t.Implements(entityInterface) || reflect.PointerTo(t).Implements(entityInterface)
}

func parseRelationField(owner reflect.Type, index int) (relationSpec, bool, error) {
	f := owner.Field(index)
	target, many, relationShape := relationTarget(f.Type)
	raw, hasRel := f.Tag.Lookup("rel")

	if !hasRel {
		// A pointer to an Entity is never neutral. Slices of entity pointers are
		// likewise either authoritative own lists or computed inverses.
		if relationShape && isEntityType(target) {
			return relationSpec{}, false, fmt.Errorf("%w: %s.%s points to entity %s but has no rel tag", ErrRelationSchema, owner, f.Name, target)
		}

		return relationSpec{}, false, nil
	}

	if !relationShape {
		return relationSpec{}, false, fmt.Errorf("%w: %s.%s: rel is only valid on *T or []*T", ErrRelationSchema, owner, f.Name)
	}

	if !isEntityType(target) {
		return relationSpec{}, false, fmt.Errorf("%w: %s.%s targets %s, which does not implement Entity", ErrRelationSchema, owner, f.Name, target)
	}

	parts := strings.Split(raw, ",")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return relationSpec{}, false, fmt.Errorf("%w: %s.%s: rel must have the form role,field", ErrRelationSchema, owner, f.Name)
	}

	kind := relationKind(strings.TrimSpace(parts[0]))
	match := strings.TrimSpace(parts[1])
	switch kind {
	case ownRelation, ownedByRelation, borrowRelation, optionRelation, inverseRelation:
	default:
		return relationSpec{}, false, fmt.Errorf("%w: %s.%s: unknown role %q", ErrRelationSchema, owner, f.Name, kind)
	}

	if kind == ownedByRelation && many {
		return relationSpec{}, false, fmt.Errorf("%w: %s.%s: ownedby requires *T", ErrRelationSchema, owner, f.Name)
	}

	if kind == inverseRelation && !many {
		return relationSpec{}, false, fmt.Errorf("%w: %s.%s: inverse requires []*T", ErrRelationSchema, owner, f.Name)
	}

	return relationSpec{
		owner: owner, fieldIndex: index, fieldName: f.Name, target: target,
		kind: kind, matchField: match, many: many,
	}, true, nil
}

func relationSpecs(t reflect.Type) ([]relationSpec, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if cached, ok := relationSpecsCache.Load(t); ok {
		result := cached.(cachedRelationSpecs)
		return result.specs, result.err
	}

	if t.Kind() != reflect.Struct {
		err := fmt.Errorf("%w: entity %s is not a struct", ErrRelationSchema, t)
		relationSpecsCache.Store(t, cachedRelationSpecs{err: err})
		return nil, err
	}

	out := make([]relationSpec, 0)
	for i := range t.NumField() {
		r, ok, err := parseRelationField(t, i)
		if err != nil {
			relationSpecsCache.Store(t, cachedRelationSpecs{err: err})
			return nil, err
		}

		if ok {
			out = append(out, r)
		}
	}

	relationSpecsCache.Store(t, cachedRelationSpecs{specs: out})
	return out, nil
}

func validateRelationSpec(r relationSpec) error {
	back, ok := r.target.FieldByName(r.matchField)
	if !ok {
		return fmt.Errorf("%w: %s.%s names missing field %s.%s", ErrRelationSchema, r.owner, r.fieldName, r.target, r.matchField)
	}

	if !r.many || (r.kind != ownRelation && r.kind != inverseRelation) {
		return validateScalarMatchField(r, back)
	}

	if r.kind == ownRelation {
		return validateOwnBackField(r, back)
	}

	return validateInverseBackField(r, back)
}

func validateScalarMatchField(r relationSpec, field reflect.StructField) error {
	if !isScalarKey(field.Type.Kind()) {
		return fmt.Errorf("%w: %s.%s target field %s.%s must be a scalar key", ErrRelationSchema, r.owner, r.fieldName, r.target, r.matchField)
	}

	return nil
}

func validateOwnBackField(r relationSpec, back reflect.StructField) error {
	if back.Type.Kind() != reflect.Pointer {
		if isScalarKey(back.Type.Kind()) {
			return nil
		}

		return fmt.Errorf("%w: %s.%s back field %s.%s must be a scalar FK or ownedby pointer", ErrRelationSchema, r.owner, r.fieldName, r.target, r.matchField)
	}

	complement, tagged, err := parseRelationField(r.target, back.Index[0])
	if err != nil {
		return err
	}

	if tagged && complement.kind == ownedByRelation && complement.target == r.owner {
		return nil
	}

	return fmt.Errorf("%w: %s.%s must be a raw scalar FK or ownedby pointer to %s", ErrRelationSchema, r.owner, r.fieldName, r.owner)
}

func validateInverseBackField(r relationSpec, back reflect.StructField) error {
	complement, tagged, err := parseRelationField(r.target, back.Index[0])
	if err != nil {
		return err
	}

	validKind := complement.kind == borrowRelation || complement.kind == optionRelation
	if tagged && validKind && complement.target == r.owner {
		return nil
	}

	return fmt.Errorf("%w: %s.%s inverse must name a borrow/option field on %s targeting %s", ErrRelationSchema, r.owner, r.fieldName, r.target, r.owner)
}

type relationSchemaBuilder struct {
	roots       map[reflect.Type]*relationRoot
	specsByType map[reflect.Type][]relationSpec
}

func newRelationSchemaBuilder() *relationSchemaBuilder {
	return &relationSchemaBuilder{
		roots:       make(map[reflect.Type]*relationRoot),
		specsByType: make(map[reflect.Type][]relationSpec),
	}
}

func (b *relationSchemaBuilder) visit(t reflect.Type) (*relationRoot, error) {
	if found := b.roots[t]; found != nil {
		return found, nil
	}

	head := &relationRoot{Type: t}
	b.roots[t] = head
	specs, err := relationSpecs(t)
	if err != nil {
		return nil, err
	}

	b.specsByType[t] = specs

	if countRelationKind(specs, ownedByRelation) > 1 {
		return nil, fmt.Errorf("%w: %s declares more than one ownedby field", ErrRelationSchema, t)
	}

	for _, spec := range specs {
		point, err := b.buildPoint(spec)
		if err != nil {
			return nil, err
		}

		head.Children = append(head.Children, point)
	}

	return head, nil
}

func countRelationKind(specs []relationSpec, kind relationKind) int {
	count := 0
	for _, spec := range specs {
		if spec.kind == kind {
			count++
		}
	}

	return count
}

func (b *relationSchemaBuilder) buildPoint(spec relationSpec) (relationPoint, error) {
	if err := validateRelationSpec(spec); err != nil {
		return relationPoint{}, err
	}

	child, err := b.visit(spec.target)
	if err != nil {
		return relationPoint{}, err
	}

	return relationPoint{
		FieldName: spec.fieldName,
		FieldType: spec.target,
		Field:     spec.matchField,
		Many:      spec.many,
		To:        &relationNode{RelationType: spec.kind, To: child},
	}, nil
}

func (b *relationSchemaBuilder) validateRequiredCycles(kind relationKind) error {
	state := make(map[reflect.Type]uint8)
	for typ := range b.specsByType {
		if err := b.visitRequiredType(typ, kind, state); err != nil {
			return err
		}
	}

	return nil
}

func (b *relationSchemaBuilder) visitRequiredType(t reflect.Type, kind relationKind, state map[reflect.Type]uint8) error {
	if state[t] == 1 {
		return fmt.Errorf("%w: unsatisfiable %s cycle involving %s", ErrRelationSchema, kind, t)
	}

	if state[t] == 2 {
		return nil
	}

	state[t] = 1
	for _, spec := range b.specsByType[t] {
		if !isRequiredTypeEdge(spec, kind) {
			continue
		}

		if err := b.visitRequiredType(spec.target, kind, state); err != nil {
			return err
		}
	}

	state[t] = 2
	return nil
}

func isRequiredTypeEdge(spec relationSpec, kind relationKind) bool {
	if spec.kind != kind {
		return false
	}

	return kind != ownRelation || !spec.many
}

// buildRelationSchema validates every type reachable from root. Reflection can
// inspect not-yet-registered types, so registration order does not weaken the
// schema checks.
func buildRelationSchema(root reflect.Type) (*relationRoot, error) {
	if root.Kind() == reflect.Pointer {
		root = root.Elem()
	}

	builder := newRelationSchemaBuilder()
	head, err := builder.visit(root)
	if err != nil {
		return nil, err
	}

	// I3: mandatory ownedby edges and mandatory to-one own edges must each be
	// acyclic in the type graph. Slice own is excluded because an empty slice
	// terminates the descent.
	for _, edgeKind := range []relationKind{ownedByRelation, ownRelation} {
		if err := builder.validateRequiredCycles(edgeKind); err != nil {
			return nil, err
		}
	}

	return head, nil
}

func scalarEqual(a, b reflect.Value) bool {
	if !a.IsValid() || !b.IsValid() || !a.CanInterface() || !b.CanInterface() {
		return false
	}

	if a.Type() == b.Type() {
		return a.Interface() == b.Interface()
	}

	return false
}

func relationKey(v reflect.Value, field string) (reflect.Value, bool) {
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return reflect.Value{}, false
		}

		v = v.Elem()
	}

	if !v.IsValid() || v.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}

	f := v.FieldByName(field)
	return f, f.IsValid() && !f.IsZero()
}

// fillRelation rewires persisted hollow pointers to stable store pointers and
// rebuilds computed own/inverse slices. It is idempotent.
func (db *DB[T]) fillRelation() {
	index, err := buildRelationWireIndex()
	if err != nil {
		panic(err)
	}

	db.fillRelationFrom(index)
}

func (db *DB[T]) fillRelationFrom(index *relationWireIndex) {
	specs, err := relationSpecs(reflect.TypeFor[T]())
	if err != nil {
		panic(err)
	}

	db.store.Range(func(entity *T) {
		wireEntityRelations(reflect.ValueOf(entity).Elem(), specs, index)
	})
}

func rewireRelations(runtimes []relationRuntime) {
	index, err := buildRelationWireIndex()
	if err != nil {
		panic(err)
	}

	for _, runtime := range runtimes {
		runtime.relationRewire(index)
	}
}

func wireEntityRelations(entity reflect.Value, specs []relationSpec, index *relationWireIndex) {
	for _, spec := range specs {
		wireRelation(entity, spec, index)
	}
}

func wireRelation(entity reflect.Value, spec relationSpec, index *relationWireIndex) {
	field := entity.Field(spec.fieldIndex)
	if !spec.many {
		wireToOne(field, spec, index)
		return
	}

	switch spec.kind {
	case borrowRelation, optionRelation:
		wireReferenceSlice(field, spec, index)
	case ownRelation, inverseRelation:
		wireIndexedSlice(entity, field, spec, index)
	}
}

func wireToOne(field reflect.Value, spec relationSpec, index *relationWireIndex) {
	key, present := relationKey(field, spec.matchField)
	if !present {
		field.SetZero()
		return
	}

	target, found := index.targets[relationTargetKey{typ: spec.target, field: spec.matchField, value: key.Interface()}]
	if !found {
		field.SetZero()
		return
	}

	field.Set(target)
}

func wireReferenceSlice(field reflect.Value, spec relationSpec, index *relationWireIndex) {
	wired := reflect.MakeSlice(field.Type(), 0, field.Len())
	for i := range field.Len() {
		target, found := canonicalSliceElement(field.Index(i), spec, index)
		if found {
			wired = reflect.Append(wired, target)
		}
	}

	field.Set(wired)
}

func canonicalSliceElement(element reflect.Value, spec relationSpec, index *relationWireIndex) (reflect.Value, bool) {
	key, present := relationKey(element, spec.matchField)
	if !present {
		return reflect.Value{}, false
	}

	target, found := index.targets[relationTargetKey{typ: spec.target, field: spec.matchField, value: key.Interface()}]
	return target, found
}

func wireIndexedSlice(owner, field reflect.Value, spec relationSpec, index *relationWireIndex) {
	key, present := relationKey(owner, entityIDField)
	if !present {
		field.Set(reflect.MakeSlice(field.Type(), 0, 0))
		return
	}

	values := index.slices[relationFieldKey{owner: spec.owner, field: spec.fieldIndex}][key.Interface()]
	wired := reflect.MakeSlice(field.Type(), len(values), len(values))
	for i := range values {
		wired.Index(i).Set(values[i])
	}

	field.Set(wired)
}

func namedRelationSpec(owner reflect.Type, fieldName string) (relationSpec, bool) {
	specs, err := relationSpecs(owner)
	if err != nil {
		return relationSpec{}, false
	}

	for _, spec := range specs {
		if spec.fieldName == fieldName {
			return spec, true
		}
	}

	return relationSpec{}, false
}

func buildRelationWireIndex() (*relationWireIndex, error) {
	index := &relationWireIndex{
		targets: make(map[relationTargetKey]reflect.Value),
		slices:  make(map[relationFieldKey]map[any][]reflect.Value),
	}
	targetFields := make(map[reflect.Type]map[string]int)

	for _, rawStore := range storeRegistry {
		store, ok := rawStore.(pointerStoreIterator)
		if !ok {
			continue
		}

		values := reflect.ValueOf(store.iterateStorePointers())
		typ := values.Type().Elem().Elem()
		specs, err := relationSpecs(typ)
		if err != nil {
			return nil, err
		}

		for _, spec := range specs {
			switch {
			case spec.kind == ownRelation && spec.many:
				indexOwnSlice(index, spec)
			case spec.kind == inverseRelation:
				indexInverseSlice(index, spec)
			default:
				field, found := spec.target.FieldByName(spec.matchField)
				if found {
					if targetFields[spec.target] == nil {
						targetFields[spec.target] = make(map[string]int)
					}

					targetFields[spec.target][spec.matchField] = field.Index[0]
				}
			}
		}
	}

	for _, rawStore := range storeRegistry {
		store, ok := rawStore.(pointerStoreIterator)
		if !ok {
			continue
		}

		values := reflect.ValueOf(store.iterateStorePointers())
		typ := values.Type().Elem().Elem()
		for i := range values.Len() {
			indexEntityTargets(index, typ, values.Index(i), targetFields[typ])
		}
	}

	return index, nil
}

func indexEntityTargets(index *relationWireIndex, typ reflect.Type, entity reflect.Value, fields map[string]int) {
	for fieldName, fieldIndex := range fields {
		value := entity.Elem().Field(fieldIndex)
		if value.IsZero() || !value.CanInterface() {
			continue
		}

		key := relationTargetKey{typ: typ, field: fieldName, value: value.Interface()}
		if _, exists := index.targets[key]; !exists {
			index.targets[key] = entity
		}
	}
}

func indexOwnSlice(index *relationWireIndex, spec relationSpec) {
	foreign, found := getForeignStoreByType(spec.target.Name())
	if !found {
		return
	}

	byOwner := make(map[any][]reflect.Value)
	back, _ := spec.target.FieldByName(spec.matchField)
	for i := range foreign.Len() {
		candidate := foreign.Index(i)
		backValue := candidate.Elem().FieldByIndex(back.Index)
		key := backValue
		if backValue.Kind() == reflect.Pointer {
			var present bool
			key, present = relationKey(backValue, entityIDField)
			if !present {
				continue
			}
		}

		if key.IsZero() || !key.CanInterface() {
			continue
		}

		value := key.Interface()
		byOwner[value] = append(byOwner[value], candidate)
	}

	index.slices[relationFieldKey{owner: spec.owner, field: spec.fieldIndex}] = byOwner
}

func indexInverseSlice(index *relationWireIndex, spec relationSpec) {
	foreign, found := getForeignStoreByType(spec.target.Name())
	if !found {
		return
	}

	back, found := namedRelationSpec(spec.target, spec.matchField)
	if !found {
		return
	}

	byTarget := make(map[any][]reflect.Value)
	for i := range foreign.Len() {
		candidate := foreign.Index(i)
		field := candidate.Elem().Field(back.fieldIndex)
		if !back.many {
			indexInverseReference(byTarget, candidate, field, back.matchField)
			continue
		}

		seen := make(map[any]struct{}, field.Len())
		for j := range field.Len() {
			key, present := relationKey(field.Index(j), back.matchField)
			if !present || !key.CanInterface() {
				continue
			}

			value := key.Interface()
			if _, duplicate := seen[value]; duplicate {
				continue
			}

			seen[value] = struct{}{}
			byTarget[value] = append(byTarget[value], candidate)
		}
	}

	index.slices[relationFieldKey{owner: spec.owner, field: spec.fieldIndex}] = byTarget
}

func indexInverseReference(byTarget map[any][]reflect.Value, holder, pointer reflect.Value, matchField string) {
	key, present := relationKey(pointer, matchField)
	if !present || !key.CanInterface() {
		return
	}

	value := key.Interface()
	byTarget[value] = append(byTarget[value], holder)
}

// getForeignStoreByType returns the live []*T for typeName as a reflect.Value.
func getForeignStoreByType(typeName string) (reflect.Value, bool) {
	e, ok := storeRegistry[typeName]
	if !ok {
		return reflect.Value{}, false
	}

	iter, ok := e.(pointerStoreIterator)
	if !ok {
		return reflect.Value{}, false
	}

	return reflect.ValueOf(iter.iterateStorePointers()), true
}
