package nestory

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
)

type relationRuntime interface {
	relationType() reflect.Type
	relationLive() []reflect.Value
	relationPending() []reflect.Value
	relationDeleteIDs() []int
	relationApplyPending() error
	relationPrepareCreate(reflect.Value) error
	relationApplyCreate(reflect.Value)
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

func (db *DB[T]) relationPrepareCreate(value reflect.Value) error {
	entity := value.Interface().(*T)
	db.mu.Lock()
	defer db.mu.Unlock()

	id := (*entity).GetId()
	if id == 0 {
		db.counter++
		SetId(entity, db.counter)
		return nil
	}
	if db.index[db.identifier][id] != nil {
		return fmt.Errorf("%w: %s(%d)", ErrAlreadyExists, db.name, id)
	}
	if id > db.counter {
		db.counter = id
	}

	return nil
}

func (db *DB[T]) relationApplyCreate(value reflect.Value) {
	db.Add(value.Interface().(*T))
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
	missing  []missingRelation
	owners   map[nodeKey]nodeKey
	outgoing map[nodeKey][]nodeKey
}

type missingRelation struct {
	holder nodeKey
	spec   relationSpec
}

type incomingOwn struct {
	owner nodeKey
	spec  relationSpec
}

var committedOwnership = struct {
	sync.RWMutex
	ready    bool
	outgoing map[nodeKey][]nodeKey
}{outgoing: make(map[nodeKey][]nodeKey)}

// graphMu protects live relation pointers while a branch is copied or a commit
// publishes and rewires a new graph. User callbacks run entirely outside it.
var graphMu sync.RWMutex

func resetCommittedOwnership() {
	committedOwnership.Lock()
	defer committedOwnership.Unlock()

	committedOwnership.ready = false
	committedOwnership.outgoing = make(map[nodeKey][]nodeKey)
}

func ensureCommittedOwnership() error {
	committedOwnership.RLock()
	ready := committedOwnership.ready
	committedOwnership.RUnlock()
	if ready {
		return nil
	}

	return refreshCommittedOwnership()
}

func refreshCommittedOwnership() error {
	nodes, err := collectRelationNodes(false, nil)
	if err != nil {
		return err
	}

	model, err := buildRelationModel(nodes)
	if err != nil {
		return err
	}

	if err := validateRequiredRelations(model, nil); err != nil {
		return err
	}

	storeCommittedOwnership(model)

	return nil
}

func storeCommittedOwnership(model *relationModel) {
	outgoing := make(map[nodeKey][]nodeKey, len(model.outgoing))
	for owner, children := range model.outgoing {
		outgoing[owner] = append([]nodeKey(nil), children...)
	}

	committedOwnership.Lock()
	committedOwnership.ready = true
	committedOwnership.outgoing = outgoing
	committedOwnership.Unlock()
}

func committedChildren(owner nodeKey) []nodeKey {
	committedOwnership.RLock()
	defer committedOwnership.RUnlock()

	return append([]nodeKey(nil), committedOwnership.outgoing[owner]...)
}

func valueID(v reflect.Value) (int, bool) {
	if !v.IsValid() || v.Kind() == reflect.Pointer && v.IsNil() {
		return 0, false
	}

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
	overrides := make(map[nodeKey]relationGraphNode)
	if override != nil {
		overrides[override.key] = *override
	}

	return collectRelationNodesWithOverrides(includePending, overrides)
}

func collectRelationNodesWithOverrides(includePending bool, overrides map[nodeKey]relationGraphNode) (map[nodeKey]relationGraphNode, error) {
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

	for key, override := range overrides {
		nodes[key] = override
	}

	return nodes, nil
}

// cloneOwnershipAggregate creates a transaction-local copy of root and every
// node it transitively owns. Relation pointers inside that aggregate are
// rewired to the copies, preserving pointer identity without touching live
// store objects.
func cloneOwnershipAggregate(root nodeKey) (map[nodeKey]reflect.Value, map[nodeKey]reflect.Value, map[nodeKey]int, error) {
	graphMu.RLock()
	defer graphMu.RUnlock()

	nodes, err := collectRelationNodes(false, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	model, err := buildRelationModel(nodes)
	if err != nil {
		return nil, nil, nil, err
	}

	if _, found := nodes[root]; !found {
		return nil, nil, nil, ErrNotFound
	}

	keys := ownershipClosure(model, root)
	ordered := make([]nodeKey, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].typ.Name() != ordered[j].typ.Name() {
			return ordered[i].typ.Name() < ordered[j].typ.Name()
		}

		return ordered[i].id < ordered[j].id
	})

	for _, key := range ordered {
		committerFor(key.typ.Name()).lockResource(key.id)
	}
	defer func() {
		for i := len(ordered) - 1; i >= 0; i-- {
			committerFor(ordered[i].typ.Name()).unlockResource(ordered[i].id)
		}
	}()

	work := make(map[nodeKey]reflect.Value, len(keys))
	original := make(map[nodeKey]reflect.Value, len(keys))
	versions := make(map[nodeKey]int, len(keys))
	for _, key := range ordered {
		work[key] = cloneEntityPointer(nodes[key].value)
		original[key] = cloneEntityPointer(nodes[key].value)
		version, found := committerFor(key.typ.Name()).resourceVersion(key.id)
		if !found {
			return nil, nil, nil, ErrNotFound
		}
		versions[key] = version
	}

	rewireAggregateCopies(model, work)
	rewireAggregateCopies(model, original)

	return work, original, versions, nil
}

func ownershipClosure(model *relationModel, root nodeKey) map[nodeKey]struct{} {
	out := map[nodeKey]struct{}{root: {}}
	queue := []nodeKey{root}
	for len(queue) > 0 {
		owner := queue[0]
		queue = queue[1:]
		for _, child := range model.outgoing[owner] {
			if _, seen := out[child]; seen {
				continue
			}

			out[child] = struct{}{}
			queue = append(queue, child)
		}
	}

	return out
}

func cloneEntityPointer(source reflect.Value) reflect.Value {
	clone := reflect.New(source.Type().Elem())
	clone.Elem().Set(source.Elem())
	cloneSliceFields(clone.Elem())

	return clone
}

func rewireTouchedCopies(resources []touchedResource) {
	copies := make(map[nodeKey]reflect.Value, len(resources))
	for _, resource := range resources {
		runtime, ok := baseRegistry[resource.dbName].(relationRuntime)
		if !ok {
			continue
		}

		copies[nodeKey{typ: runtime.relationType(), id: resource.id}] = reflect.ValueOf(resource.work)
	}

	nodes, err := collectRelationNodes(false, nil)
	if err != nil {
		return
	}
	model, err := buildRelationModel(nodes)
	if err != nil {
		return
	}

	rewireAggregateCopies(model, copies)
}

func rewireAggregateCopies(model *relationModel, copies map[nodeKey]reflect.Value) {
	for _, ref := range model.refs {
		holder, holderCopied := copies[ref.holder]
		target, targetCopied := copies[ref.target]
		if !holderCopied || !targetCopied {
			continue
		}

		field := holder.Elem().Field(ref.spec.fieldIndex)
		if !ref.spec.many {
			field.Set(target)
			continue
		}

		for i := range field.Len() {
			id, ok := valueID(field.Index(i))
			if ok && id == ref.target.id {
				field.Index(i).Set(target)
			}
		}
	}
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

			model.missing = append(model.missing, missingRelation{holder: node.key, spec: spec})
			continue
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

	return spec.many && (spec.kind == Borrow || spec.kind == Own)
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

func validateRequiredRelations(model *relationModel, deleted map[nodeKey]struct{}) error {
	for _, missing := range model.missing {
		if _, dies := deleted[missing.holder]; dies {
			continue
		}

		return fmt.Errorf("%w: required relation %s.%s on %s is nil or has no live target", ErrRelationInvariant, missing.holder.typ, missing.spec.fieldName, missing.holder)
	}

	return nil
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
		for _, child := range deletionChildren(model, owner) {
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

func deletionChildren(model *relationModel, owner nodeKey) []nodeKey {
	seen := make(map[nodeKey]struct{})
	children := make([]nodeKey, 0)
	for _, child := range model.outgoing[owner] {
		seen[child] = struct{}{}
		children = append(children, child)
	}

	for _, child := range committedChildren(owner) {
		if _, exists := model.nodes[child]; !exists {
			continue
		}

		if proposedOwner, reparented := model.owners[child]; reparented && proposedOwner != owner {
			continue
		}

		if _, duplicate := seen[child]; duplicate {
			continue
		}

		seen[child] = struct{}{}
		children = append(children, child)
	}

	return children
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

// reconcileRelations canonicalizes references, applies option set-null, and
// synchronizes the child-side FK of own slices.
func reconcileRelations(model *relationModel, deleted map[nodeKey]struct{}) {
	changed := make(map[nodeKey]struct{})
	canonicalizeRelationPointers(model, deleted, changed)
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

func flushRelations() error {
	graphMu.Lock()
	defer graphMu.Unlock()

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
	if err := validateRequiredRelations(model, deleted); err != nil {
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
	if err := validateRequiredRelations(model, deleted); err != nil {
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
	if err := refreshCommittedOwnership(); err != nil {
		return err
	}

	return nil
}

func applyDeletedNodes(deleted map[nodeKey]struct{}) {
	byType := make(map[reflect.Type]map[int]struct{})
	for key := range deleted {
		if byType[key.typ] == nil {
			byType[key.typ] = make(map[int]struct{})
		}
		byType[key.typ][key.id] = struct{}{}
	}
	for _, runtime := range relationRuntimes() {
		runtime.relationDelete(byType[runtime.relationType()])
	}
}

func validateRelationUpdate(t reflect.Type, id int, work reflect.Value) error {
	nodes, err := collectRelationNodes(false, &relationGraphNode{key: nodeKey{typ: t, id: id}, value: work})
	if err != nil {
		return err
	}

	model, err := buildRelationModel(nodes)
	if err != nil {
		return err
	}

	return validateRequiredRelations(model, nil)
}
