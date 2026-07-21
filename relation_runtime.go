package nestory

import (
	"fmt"
	"reflect"
	"sort"
)

type relationRuntime interface {
	relationType() reflect.Type
	relationLive() []reflect.Value
	relationPending() []reflect.Value
	relationDeleteIDs() []int
	relationApplyPending() error
	relationDelete(map[int]struct{})
	relationMarkDirty(int)
	relationRewire()
	relationSave() error
	relationClearQueues()
}

func (db *DB[T]) relationType() reflect.Type { return reflect.TypeFor[T]() }

func (db *DB[T]) relationLive() []reflect.Value {
	items := db.store.StorePointers()
	out := make([]reflect.Value, len(items))
	for i := range items {
		out[i] = reflect.ValueOf(items[i])
	}

	return out
}

func (db *DB[T]) relationPending() []reflect.Value {
	out := make([]reflect.Value, len(db.persistQueue))
	for i := range db.persistQueue {
		out[i] = reflect.ValueOf(db.persistQueue[i])
	}

	return out
}

func (db *DB[T]) relationDeleteIDs() []int {
	out := make([]int, len(db.deleteQueue))
	for i := range db.deleteQueue {
		out[i] = (*db.deleteQueue[i]).GetId()
	}

	return out
}

func (db *DB[T]) relationApplyPending() error {
	existing := make(map[int]bool, db.store.Len())
	db.store.Range(func(p *T) { existing[(*p).GetId()] = true })
	for _, entity := range db.persistQueue {
		id := (*entity).GetId()
		if existing[id] {
			if _, err := db.PatchById(id, *entity); err != nil {
				return err
			}

			continue
		}

		db.Add(entity)
		existing[id] = true
	}

	return nil
}

func (db *DB[T]) relationDelete(ids map[int]struct{}) {
	if len(ids) == 0 {
		return
	}

	removed := db.store.DeleteFunc(func(p *T) bool {
		_, ok := ids[(*p).GetId()]
		return ok
	})
	for _, p := range removed {
		id := (*p).GetId()
		delete(db.index["Id"], id)
		delete(db.resById, id)
	}
}

func (db *DB[T]) relationMarkDirty(id int) {
	if r, ok := db.resById[id]; ok {
		db.store.markDirty(r.chunk)
	}
}

func (db *DB[T]) relationRewire() { db.fillRelation() }

func (db *DB[T]) relationSave() error {
	if err := db.save(); err != nil {
		return err
	}

	if db.wal != nil {
		return db.wal.truncate()
	}

	return nil
}

func (db *DB[T]) relationClearQueues() {
	db.ResetpersistQueue()
	db.resetDeleteQueue()
}

type nodeKey struct {
	typ reflect.Type
	id  int
}

func (k nodeKey) String() string { return fmt.Sprintf("%s(%d)", k.typ, k.id) }

type relationGraphNode struct {
	key   nodeKey
	value reflect.Value // *struct
}

type resolvedRelation struct {
	holder nodeKey
	target nodeKey
	spec   relationSpec
}

type relationModel struct {
	nodes    map[nodeKey]relationGraphNode
	refs     []resolvedRelation
	owners   map[nodeKey]nodeKey
	outgoing map[nodeKey][]nodeKey
}

type incomingOwn struct {
	owner nodeKey
	spec  relationSpec
}

func valueID(v reflect.Value) (int, bool) {
	if v.IsValid() && v.CanInterface() {
		if entity, ok := v.Interface().(Entity); ok {
			return entity.GetId(), true
		}
	}

	return 0, false
}

func relationRuntimes() []relationRuntime {
	names := make([]string, 0, len(baseRegistry))
	for name := range baseRegistry {
		names = append(names, name)
	}

	sort.Strings(names)
	out := make([]relationRuntime, 0, len(names))
	for _, name := range names {
		if runtime, ok := baseRegistry[name].(relationRuntime); ok {
			out = append(out, runtime)
		}
	}

	return out
}

func collectRelationNodes(includePending bool, override *relationGraphNode) (map[nodeKey]relationGraphNode, error) {
	nodes := make(map[nodeKey]relationGraphNode)
	for _, rawStore := range storeRegistry {
		if err := collectStoreNodes(nodes, rawStore); err != nil {
			return nil, err
		}
	}

	if includePending {
		if err := collectPendingNodes(nodes); err != nil {
			return nil, err
		}
	}

	if override != nil {
		nodes[override.key] = *override
	}

	return nodes, nil
}

func collectStoreNodes(nodes map[nodeKey]relationGraphNode, rawStore any) error {
	store, ok := rawStore.(pointerStoreIterator)
	if !ok {
		return nil
	}

	values := reflect.ValueOf(store.iterateStorePointers())
	for i := range values.Len() {
		if err := addGraphNode(nodes, values.Index(i), values.Index(i).Type().Elem(), false); err != nil {
			return err
		}
	}

	return nil
}

func collectPendingNodes(nodes map[nodeKey]relationGraphNode) error {
	for _, runtime := range relationRuntimes() {
		for _, value := range runtime.relationPending() {
			if err := addGraphNode(nodes, value, runtime.relationType(), true); err != nil {
				return err
			}
		}
	}

	return nil
}

func addGraphNode(nodes map[nodeKey]relationGraphNode, value reflect.Value, typ reflect.Type, replace bool) error {
	id, ok := valueID(value)
	if !ok || id <= 0 {
		return fmt.Errorf("%w: entity %s has invalid Id", ErrRelationInvariant, value.Type())
	}

	key := nodeKey{typ: typ, id: id}
	if _, duplicate := nodes[key]; duplicate && !replace {
		return fmt.Errorf("%w: duplicate entity %s", ErrRelationInvariant, key)
	}

	nodes[key] = relationGraphNode{key: key, value: value}
	return nil
}

func resolveGraphTarget(nodes map[nodeKey]relationGraphNode, r relationSpec, pointer reflect.Value) (nodeKey, bool, error) {
	matchField := r.matchField
	if r.kind == Own && r.many {
		matchField = "Id"
	}

	key, present := relationKey(pointer, matchField)
	if !present {
		return nodeKey{}, false, nil
	}

	var found nodeKey
	matches := 0
	for candidateKey, candidate := range nodes {
		if candidateKey.typ != r.target {
			continue
		}

		candidateValue, ok := relationKey(candidate.value, matchField)
		if ok && scalarEqual(key, candidateValue) {
			found = candidateKey
			matches++
		}
	}

	if matches > 1 {
		return nodeKey{}, false, fmt.Errorf("%w: %s.%s does not uniquely identify a target", ErrRelationInvariant, r.owner, r.fieldName)
	}

	return found, matches == 1, nil
}

func buildRelationModel(nodes map[nodeKey]relationGraphNode) (*relationModel, error) {
	model := &relationModel{
		nodes: nodes, owners: make(map[nodeKey]nodeKey), outgoing: make(map[nodeKey][]nodeKey),
	}
	incoming := make(map[nodeKey][]incomingOwn)
	ownedBy := make(map[nodeKey]incomingOwn)

	for _, node := range nodes {
		if err := scanNodeRelations(model, node, incoming, ownedBy); err != nil {
			return nil, err
		}
	}

	for child := range nodes {
		if err := attachNodeOwner(model, child, incoming[child], ownedBy); err != nil {
			return nil, err
		}
	}

	if err := validateMandatoryComplements(model); err != nil {
		return nil, err
	}

	if err := validateOwnershipCycles(model); err != nil {
		return nil, err
	}

	return model, nil
}

func scanNodeRelations(model *relationModel, node relationGraphNode, incoming map[nodeKey][]incomingOwn, ownedBy map[nodeKey]incomingOwn) error {
	specs, err := relationSpecs(node.key.typ)
	if err != nil {
		return err
	}

	for _, spec := range specs {
		if spec.kind == Inverse {
			continue
		}

		if err := scanRelationField(model, node, spec, incoming, ownedBy); err != nil {
			return err
		}
	}

	return nil
}

func scanRelationField(model *relationModel, node relationGraphNode, spec relationSpec, incoming map[nodeKey][]incomingOwn, ownedBy map[nodeKey]incomingOwn) error {
	field := node.value.Elem().Field(spec.fieldIndex)
	values := relationFieldValues(field, spec.many)
	seen := make(map[nodeKey]struct{}, len(values))
	for _, pointer := range values {
		target, found, err := resolveGraphTarget(model.nodes, spec, pointer)
		if err != nil {
			return err
		}

		if !found {
			if relationMayBeMissing(spec) {
				continue
			}

			return fmt.Errorf("%w: required relation %s.%s on %s has no live target", ErrRelationInvariant, node.key.typ, spec.fieldName, node.key)
		}

		if _, duplicate := seen[target]; duplicate && spec.kind == Own {
			return fmt.Errorf("%w: %s.%s contains owned child %s more than once", ErrRelationInvariant, node.key.typ, spec.fieldName, target)
		}

		seen[target] = struct{}{}
		recordResolvedRelation(model, node.key, target, spec, incoming, ownedBy)
	}

	return nil
}

func relationFieldValues(field reflect.Value, many bool) []reflect.Value {
	if !many {
		return []reflect.Value{field}
	}

	values := make([]reflect.Value, field.Len())
	for i := range field.Len() {
		values[i] = field.Index(i)
	}

	return values
}

func relationMayBeMissing(spec relationSpec) bool {
	if spec.kind == Option {
		return true
	}

	if !spec.many {
		return spec.kind == Own || spec.kind == OwnedBy
	}

	return spec.kind == Borrow || spec.kind == Own
}

func recordResolvedRelation(model *relationModel, holder, target nodeKey, spec relationSpec, incoming map[nodeKey][]incomingOwn, ownedBy map[nodeKey]incomingOwn) {
	model.refs = append(model.refs, resolvedRelation{holder: holder, target: target, spec: spec})
	edge := incomingOwn{owner: holder, spec: spec}
	if spec.kind == Own {
		incoming[target] = append(incoming[target], edge)
	}

	if spec.kind == OwnedBy {
		edge.owner = target
		ownedBy[holder] = edge
	}
}

func attachNodeOwner(model *relationModel, child nodeKey, raw []incomingOwn, ownedBy map[nodeKey]incomingOwn) error {
	back, hasBack := ownedBy[child]
	if len(raw) > 1 {
		return fmt.Errorf("%w: %s has more than one owner", ErrRelationInvariant, child)
	}

	if len(raw) == 1 && hasBack && raw[0].owner != back.owner {
		return fmt.Errorf("%w: own and ownedby disagree for %s (%s vs %s)", ErrRelationInvariant, child, raw[0].owner, back.owner)
	}

	owner, found := resolvedOwner(raw, back, hasBack)
	if !found {
		return nil
	}

	model.owners[child] = owner
	model.outgoing[owner] = append(model.outgoing[owner], child)
	return nil
}

func resolvedOwner(raw []incomingOwn, back incomingOwn, hasBack bool) (nodeKey, bool) {
	if len(raw) == 1 {
		return raw[0].owner, true
	}

	if hasBack {
		return back.owner, true
	}

	return nodeKey{}, false
}

func validateMandatoryComplements(model *relationModel) error {
	for key, node := range model.nodes {
		specs, _ := relationSpecs(key.typ)
		for _, spec := range specs {
			if mandatoryComplementSatisfied(model, node, spec) {
				continue
			}

			return fmt.Errorf("%w: required relation %s.%s on %s is nil", ErrRelationInvariant, key.typ, spec.fieldName, key)
		}
	}

	return nil
}

func mandatoryComplementSatisfied(model *relationModel, node relationGraphNode, spec relationSpec) bool {
	if spec.many || (spec.kind != Own && spec.kind != OwnedBy) {
		return true
	}

	if !node.value.Elem().Field(spec.fieldIndex).IsNil() {
		return true
	}

	if spec.kind == OwnedBy {
		_, supplied := model.owners[node.key]
		return supplied
	}

	return childCountOfType(model.outgoing[node.key], spec.target) == 1
}

func childCountOfType(children []nodeKey, typ reflect.Type) int {
	count := 0
	for _, child := range children {
		if child.typ == typ {
			count++
		}
	}

	return count
}

func validateOwnershipCycles(model *relationModel) error {
	for start := range model.nodes {
		if ownershipChainCycles(model.owners, start) {
			return fmt.Errorf("%w: ownership cycle involving %s", ErrRelationInvariant, start)
		}
	}

	return nil
}

func ownershipChainCycles(owners map[nodeKey]nodeKey, start nodeKey) bool {
	seen := make(map[nodeKey]struct{})
	for at := start; ; {
		owner, found := owners[at]
		if !found {
			return false
		}

		if _, duplicate := seen[owner]; duplicate || owner == start {
			return true
		}

		seen[owner] = struct{}{}
		at = owner
	}
}

func explicitDeletes() map[nodeKey]struct{} {
	out := make(map[nodeKey]struct{})
	for _, runtime := range relationRuntimes() {
		for _, id := range runtime.relationDeleteIDs() {
			out[nodeKey{typ: runtime.relationType(), id: id}] = struct{}{}
		}
	}

	return out
}

func deletionClosure(model *relationModel, explicit map[nodeKey]struct{}) (map[nodeKey]struct{}, error) {
	deleted := make(map[nodeKey]struct{}, len(explicit))
	queue := make([]nodeKey, 0, len(explicit))
	for key := range explicit {
		if _, exists := model.nodes[key]; !exists {
			return nil, fmt.Errorf("%w: cannot delete missing %s", ErrRelationInvariant, key)
		}

		deleted[key] = struct{}{}
		queue = append(queue, key)
	}

	for len(queue) > 0 {
		owner := queue[0]
		queue = queue[1:]
		for _, child := range model.outgoing[owner] {
			if _, seen := deleted[child]; seen {
				continue
			}

			deleted[child] = struct{}{}
			queue = append(queue, child)
		}
	}

	for _, ref := range model.refs {
		_, holderDies := deleted[ref.holder]
		_, targetDies := deleted[ref.target]
		if holderDies || !targetDies {
			continue
		}

		switch ref.spec.kind {
		case Borrow:
			return nil, fmt.Errorf("%w: %s borrows doomed %s through %s", ErrDeleteRestricted, ref.holder, ref.target, ref.spec.fieldName)
		case Own:
			if ref.spec.many {
				continue
			}

			return nil, fmt.Errorf("%w: %s owns doomed %s through required to-one field %s", ErrDeleteRestricted, ref.holder, ref.target, ref.spec.fieldName)
		}
	}

	return deleted, nil
}

func setRelationField(holder relationGraphNode, r relationSpec, target reflect.Value) bool {
	field := holder.value.Elem().Field(r.fieldIndex)
	if !field.CanSet() || r.many {
		return false
	}

	if !target.IsValid() {
		return clearPointer(field)
	}

	if !field.IsNil() && field.Pointer() == target.Pointer() {
		return false
	}

	field.Set(target)
	return true
}

func clearPointer(field reflect.Value) bool {
	if field.IsNil() {
		return false
	}

	field.SetZero()
	return true
}

// reconcileRelations canonicalizes references, fills own/ownedby complements,
// applies option set-null, and synchronizes the child-side FK of own slices.
func reconcileRelations(model *relationModel, deleted map[nodeKey]struct{}) {
	changed := make(map[nodeKey]struct{})
	canonicalizeRelationPointers(model, deleted, changed)
	fillOwnedByComplements(model, deleted, changed)
	fillOwnComplements(model, changed)
	syncOwnSliceBackReferences(model, deleted, changed)
	markChangedRelations(changed)
}

func canonicalizeRelationPointers(model *relationModel, deleted, changed map[nodeKey]struct{}) {
	for _, ref := range model.refs {
		holder := model.nodes[ref.holder]
		target := model.nodes[ref.target]
		if _, holderDies := deleted[ref.holder]; holderDies {
			continue
		}

		if _, targetDies := deleted[ref.target]; targetDies {
			if ref.spec.kind == Option && !ref.spec.many && setRelationField(holder, ref.spec, reflect.Value{}) {
				changed[ref.holder] = struct{}{}
			}

			continue
		}

		if !ref.spec.many && setRelationField(holder, ref.spec, target.value) {
			changed[ref.holder] = struct{}{}
		}
	}
}

func fillOwnedByComplements(model *relationModel, deleted, changed map[nodeKey]struct{}) {
	for child, owner := range model.owners {
		if _, dies := deleted[child]; dies {
			continue
		}

		complement, found := relationOfKindTo(child.typ, OwnedBy, owner.typ, false)
		if !found {
			continue
		}

		if setRelationField(model.nodes[child], complement, model.nodes[owner].value) {
			changed[child] = struct{}{}
		}
	}
}

func fillOwnComplements(model *relationModel, changed map[nodeKey]struct{}) {
	for owner, children := range model.outgoing {
		specs, _ := relationSpecs(owner.typ)
		for _, spec := range specs {
			fillOwnComplement(model, owner, children, spec, changed)
		}
	}
}

func fillOwnComplement(model *relationModel, owner nodeKey, children []nodeKey, spec relationSpec, changed map[nodeKey]struct{}) {
	if spec.kind != Own || spec.many {
		return
	}

	match, count := onlyChildOfType(children, spec.target)
	if count != 1 {
		return
	}

	if setRelationField(model.nodes[owner], spec, model.nodes[match].value) {
		changed[owner] = struct{}{}
	}
}

func onlyChildOfType(children []nodeKey, typ reflect.Type) (nodeKey, int) {
	var match nodeKey
	count := 0
	for _, child := range children {
		if child.typ == typ {
			match = child
			count++
		}
	}

	return match, count
}

func syncOwnSliceBackReferences(model *relationModel, deleted, changed map[nodeKey]struct{}) {
	for _, ref := range model.refs {
		if ref.spec.kind != Own || !ref.spec.many {
			continue
		}

		if _, dies := deleted[ref.target]; dies {
			continue
		}

		if syncOwnSliceBackReference(model, ref) {
			changed[ref.target] = struct{}{}
		}
	}
}

func syncOwnSliceBackReference(model *relationModel, ref resolvedRelation) bool {
	owner := model.nodes[ref.holder]
	child := model.nodes[ref.target]
	back := child.value.Elem().FieldByName(ref.spec.matchField)
	if back.Kind() == reflect.Pointer {
		if !back.IsNil() && back.Pointer() == owner.value.Pointer() {
			return false
		}

		back.Set(owner.value)
		return true
	}

	id := owner.value.Elem().FieldByName("Id")
	if scalarEqual(back, id) {
		return false
	}

	back.Set(id)
	return true
}

func markChangedRelations(changed map[nodeKey]struct{}) {
	for key := range changed {
		runtime, ok := baseRegistry[key.typ.Name()].(relationRuntime)
		if !ok {
			continue
		}

		runtime.relationMarkDirty(key.id)
	}
}

func relationOfKindTo(owner reflect.Type, kind RelationKind, target reflect.Type, many bool) (relationSpec, bool) {
	specs, err := relationSpecs(owner)
	if err != nil {
		return relationSpec{}, false
	}

	for _, spec := range specs {
		if spec.kind == kind && spec.target == target && spec.many == many {
			return spec, true
		}
	}

	return relationSpec{}, false
}

func flushRelations() error {
	nodes, err := collectRelationNodes(true, nil)
	if err != nil {
		return err
	}

	model, err := buildRelationModel(nodes)
	if err != nil {
		return err
	}

	deleted, err := deletionClosure(model, explicitDeletes())
	if err != nil {
		return err
	}

	runtimes := relationRuntimes()
	for _, runtime := range runtimes {
		if err := runtime.relationApplyPending(); err != nil {
			return err
		}
	}

	// Rebuild the model with canonical store slots after inserts/patches.
	nodes, err = collectRelationNodes(false, nil)
	if err != nil {
		return err
	}

	model, err = buildRelationModel(nodes)
	if err != nil {
		return err
	}

	reconcileRelations(model, deleted)

	byType := make(map[reflect.Type]map[int]struct{})
	for key := range deleted {
		if byType[key.typ] == nil {
			byType[key.typ] = make(map[int]struct{})
		}

		byType[key.typ][key.id] = struct{}{}
	}

	for _, runtime := range runtimes {
		runtime.relationDelete(byType[runtime.relationType()])
	}

	for _, runtime := range runtimes {
		runtime.relationRewire()
	}

	for _, runtime := range runtimes {
		if err := runtime.relationSave(); err != nil {
			return err
		}
	}

	for _, runtime := range runtimes {
		runtime.relationClearQueues()
	}

	return nil
}

func validateRelationUpdate(t reflect.Type, id int, work reflect.Value) error {
	nodes, err := collectRelationNodes(false, &relationGraphNode{key: nodeKey{typ: t, id: id}, value: work})
	if err != nil {
		return err
	}

	_, err = buildRelationModel(nodes)
	return err
}
