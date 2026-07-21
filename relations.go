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

func canonicalTarget(target reflect.Type, match string, key reflect.Value) (reflect.Value, bool) {
	foreign, ok := getForeignStoreByType(target.Name())
	if !ok {
		return reflect.Value{}, false
	}

	for i := range foreign.Len() {
		candidate := foreign.Index(i)
		candidateKey, valid := relationKey(candidate, match)
		if valid && scalarEqual(candidateKey, key) {
			return candidate, true
		}
	}

	return reflect.Value{}, false
}

func valueReferences(v reflect.Value, r relationSpec, target reflect.Value) bool {
	want, ok := relationKey(target, "Id")
	if !ok {
		return false
	}

	f := v.Field(r.fieldIndex)
	if r.many {
		for i := range f.Len() {
			got, valid := relationKey(f.Index(i), r.matchField)
			if valid && scalarEqual(got, want) {
				return true
			}
		}
		return false
	}

	got, valid := relationKey(f, r.matchField)
	return valid && scalarEqual(got, want)
}

// fillRelation rewires persisted hollow pointers to stable store pointers and
// rebuilds computed own/inverse slices. It is idempotent.
func (db *DB[T]) fillRelation() {
	specs, err := relationSpecs(reflect.TypeFor[T]())
	if err != nil {
		panic(err)
	}

	db.store.Range(func(entity *T) {
		wireEntityRelations(reflect.ValueOf(entity).Elem(), specs)
	})
}

func wireEntityRelations(entity reflect.Value, specs []relationSpec) {
	for _, spec := range specs {
		wireRelation(entity, spec)
	}
}

func wireRelation(entity reflect.Value, spec relationSpec) {
	field := entity.Field(spec.fieldIndex)
	if !spec.many {
		wireToOne(field, spec)
		return
	}

	switch spec.kind {
	case borrowRelation, optionRelation:
		wireReferenceSlice(field, spec)
	case ownRelation:
		wireOwnSlice(entity, field, spec)
	case inverseRelation:
		wireInverseSlice(entity, field, spec)
	}
}

func wireToOne(field reflect.Value, spec relationSpec) {
	key, present := relationKey(field, spec.matchField)
	if !present {
		field.SetZero()
		return
	}

	target, found := canonicalTarget(spec.target, spec.matchField, key)
	if !found {
		field.SetZero()
		return
	}

	field.Set(target)
}

func wireReferenceSlice(field reflect.Value, spec relationSpec) {
	wired := reflect.MakeSlice(field.Type(), 0, field.Len())
	for i := range field.Len() {
		target, found := canonicalSliceElement(field.Index(i), spec)
		if found {
			wired = reflect.Append(wired, target)
		}
	}

	field.Set(wired)
}

func canonicalSliceElement(element reflect.Value, spec relationSpec) (reflect.Value, bool) {
	key, present := relationKey(element, spec.matchField)
	if !present {
		return reflect.Value{}, false
	}

	return canonicalTarget(spec.target, spec.matchField, key)
}

func wireOwnSlice(owner, field reflect.Value, spec relationSpec) {
	wired := reflect.MakeSlice(field.Type(), 0, 0)
	foreign, found := getForeignStoreByType(spec.target.Name())
	if !found {
		field.Set(wired)
		return
	}

	ownerID, _ := relationKey(owner, "Id")
	back, _ := spec.target.FieldByName(spec.matchField)
	for i := range foreign.Len() {
		candidate := foreign.Index(i)
		if candidateBelongsToOwner(candidate, back, ownerID) {
			wired = reflect.Append(wired, candidate)
		}
	}

	field.Set(wired)
}

func candidateBelongsToOwner(candidate reflect.Value, back reflect.StructField, ownerID reflect.Value) bool {
	backValue := candidate.Elem().FieldByIndex(back.Index)
	if backValue.Kind() != reflect.Pointer {
		return scalarEqual(backValue, ownerID)
	}

	key, present := relationKey(backValue, "Id")
	return present && scalarEqual(key, ownerID)
}

func wireInverseSlice(target, field reflect.Value, spec relationSpec) {
	wired := reflect.MakeSlice(field.Type(), 0, 0)
	foreign, found := getForeignStoreByType(spec.target.Name())
	if !found {
		field.Set(wired)
		return
	}

	back, found := namedRelationSpec(spec.target, spec.matchField)
	if !found {
		field.Set(wired)
		return
	}

	for i := range foreign.Len() {
		candidate := foreign.Index(i)
		if valueReferences(candidate.Elem(), back, target.Addr()) {
			wired = reflect.Append(wired, candidate)
		}
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
