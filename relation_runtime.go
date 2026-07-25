package nestory

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
)

type relationRuntime interface {
	relationType() reflect.Type
	relationLive() []reflect.Value
	relationPending() []reflect.Value
	relationDeleteIDs() []int
	relationValue(int) (reflect.Value, bool)
	relationApplyPending() error
	relationPrepareCreate(reflect.Value) error
	relationApplyCreate(reflect.Value)
	relationDelete(map[int]struct{})
	relationMarkDirty(int)
	relationReindex() error
	relationRewire(*relationWireIndex)
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

func (db *DB[T]) relationValue(id int) (reflect.Value, bool) {
	resource, found := db.resource(id)
	if !found {
		return reflect.Value{}, false
	}

	return reflect.ValueOf(resource.item), true
}

func (db *DB[T]) relationApplyPending() error {
	if len(db.persistQueue) == 0 {
		return nil
	}

	// resById is already the id set this used to copy into a throwaway map —
	// one map per flush, sized like the table. add inserts under db.mu before
	// the next lookup, so an intra-batch duplicate is caught the same way a
	// live one is.
	for _, entity := range db.persistQueue {
		id := (*entity).GetId()
		if _, exists := db.resource(id); exists {
			return fmt.Errorf("%w: %s(%d)", ErrAlreadyExists, db.name, id)
		}

		db.add(entity)
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
		setID(entity, db.counter)
		return nil
	}

	if _, exists := db.resById[id]; exists {
		return fmt.Errorf("%w: %s(%d)", ErrAlreadyExists, db.name, id)
	}

	if id > db.counter {
		db.counter = id
	}

	return nil
}

func (db *DB[T]) relationApplyCreate(value reflect.Value) {
	db.add(value.Interface().(*T))
}

func (db *DB[T]) relationDelete(ids map[int]struct{}) {
	if len(ids) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	removed := db.store.DeleteFunc(func(p *T) bool {
		_, ok := ids[(*p).GetId()]
		return ok
	})
	for _, p := range removed {
		id := (*p).GetId()
		db.removeSecondaryIndices(p)
		delete(db.resById, id)
	}
	if len(removed) > 0 {
		db.markChanged()
	}
}

var (
	relationParticipantsMu sync.RWMutex
	relationParticipants   = make(map[reflect.Type]struct{})
	// relationParticipantsPresent mirrors len(relationParticipants) > 0 so the
	// relation-free fast path costs an atomic load, not an RWMutex round trip —
	// ensureCommittedOwnership sits on every Unsafe.Get.
	relationParticipantsPresent atomic.Bool
)

func registerRelationParticipants(holder reflect.Type, specs []relationSpec) {
	if len(specs) == 0 {
		return
	}

	relationParticipantsMu.Lock()
	defer relationParticipantsMu.Unlock()

	relationParticipants[holder] = struct{}{}
	for _, spec := range specs {
		relationParticipants[spec.target] = struct{}{}
	}

	relationParticipantsPresent.Store(true)
}

func relationGraphParticipant(typ reflect.Type) bool {
	relationParticipantsMu.RLock()
	defer relationParticipantsMu.RUnlock()

	_, found := relationParticipants[typ]

	return found
}

func (db *DB[T]) relationMarkDirty(id int) {
	if r, ok := db.resById[id]; ok {
		db.store.markDirty(r.chunk)
	}
}

func (db *DB[T]) relationReindex() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.rebuildSecondaryIndices()
}

func (db *DB[T]) relationRewire(index *relationWireIndex) { db.fillRelationFrom(index) }

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
	db.resetPersistQueue()
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

// spec points into the slice relationSpecs caches per type, which is built once
// and never mutated. Holding it by value cost 96 of this struct's 144 bytes, on
// a slice that holds one element per edge in the project.
type resolvedRelation struct {
	holder nodeKey
	target nodeKey
	spec   *relationSpec
}

type relationModel struct {
	nodes        map[nodeKey]relationGraphNode
	targets      map[relationTargetKey]indexedRelationTarget
	targetFields map[reflect.Type]map[string]struct{}
	refs         []resolvedRelation
	unresolved   []unresolvedRelation
	missing      []missingRelation
	owners       map[nodeKey]nodeKey
	outgoing     map[nodeKey][]nodeKey
}

type relationTargetKey struct {
	typ   reflect.Type
	field string
	value any
}

type indexedRelationTarget struct {
	key       nodeKey
	duplicate bool
}

type missingRelation struct {
	holder nodeKey
	spec   relationSpec
}

type unresolvedRelation struct {
	holder nodeKey
	spec   relationSpec
	lookup relationTargetKey
}

type incomingOwn struct {
	owner nodeKey
	spec  *relationSpec
}

// incomingOwns records the own edges pointing at one node. attachNodeOwner
// rejects a second one, so the legal population is zero or one — a slice per
// node allocated once per node per Flush to hold at most a single element.
// count still distinguishes "none" from "too many" so the error survives.
type incomingOwns struct {
	first incomingOwn
	count int
}

func (owns *incomingOwns) add(edge incomingOwn) {
	if owns.count == 0 {
		owns.first = edge
	}

	owns.count++
}

var committedOwnership = struct {
	sync.RWMutex
	ready    atomic.Bool
	outgoing map[nodeKey][]nodeKey
	branches map[nodeKey][]nodeKey
	graph    *relationModel
	sizes    committedSizes
	index    *committedRelationIndex
}{outgoing: make(map[nodeKey][]nodeKey), branches: make(map[nodeKey][]nodeKey)}

// graphMu protects live relation pointers while a branch is copied or a commit
// publishes and rewires a new graph. User callbacks run entirely outside it.
var graphMu sync.RWMutex

// committedSizes remembers how large each rebuilt collection came out last time.
// A rebuild lands within a few entries of it, so these are tight hints and skip
// the fifteen-odd rehashes a hintless map of this size pays. Hints taken from a
// differently-keyed collection are not safe this way: the slack they leave stays
// resident.
type committedSizes struct {
	nodes    int
	targets  int
	owners   int
	outgoing int
	fields   int
}

func committedSizeHints() committedSizes {
	committedOwnership.RLock()
	defer committedOwnership.RUnlock()

	return committedOwnership.sizes
}

func resetCommittedOwnership() {
	committedOwnership.Lock()
	defer committedOwnership.Unlock()

	committedOwnership.ready.Store(false)
	committedOwnership.outgoing = make(map[nodeKey][]nodeKey)
	committedOwnership.branches = make(map[nodeKey][]nodeKey)
	committedOwnership.graph = nil
	committedOwnership.sizes = committedSizes{}
	committedOwnership.index = nil
}

func ensureCommittedOwnership() error {
	if committedOwnership.ready.Load() {
		return nil
	}

	// A project where no type declares a relation has no relation graph to
	// index. Building one anyway walks every node of every table and copies it
	// into a model and an index — the complete cost of a graph with no edges.
	// The check is re-evaluated per call rather than cached in ready, so a
	// relation-carrying type registered later still gets its index built. Every
	// consumer of committedOwnership.index already handles a nil index.
	if !relationGraphRegistered() {
		return nil
	}

	return refreshCommittedOwnership()
}

// committedRelationRefCount reports how many edges the last published model had,
// as a sizing hint for the next rebuild. It is a hint, never a correctness input.
func committedRelationRefCount() int {
	committedOwnership.RLock()
	defer committedOwnership.RUnlock()

	if committedOwnership.graph == nil {
		return 0
	}

	return len(committedOwnership.graph.refs)
}

func relationGraphRegistered() bool {
	return relationParticipantsPresent.Load()
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

	storeCommittedOwnership(model, nil)

	return nil
}

func storeCommittedOwnership(model *relationModel, deleted map[nodeKey]struct{}) {
	outgoing := make(map[nodeKey][]nodeKey, len(model.outgoing))
	for owner, children := range model.outgoing {
		if _, dies := deleted[owner]; dies {
			continue
		}

		for _, child := range children {
			if _, dies := deleted[child]; !dies {
				outgoing[owner] = append(outgoing[owner], child)
			}
		}
	}

	committedOwnership.Lock()
	committedOwnership.outgoing = outgoing
	committedOwnership.branches = make(map[nodeKey][]nodeKey)
	if len(deleted) == 0 {
		committedOwnership.graph = model
	} else {
		committedOwnership.graph = nil
	}

	committedOwnership.index = buildCommittedRelationIndex(model, deleted, committedOwnership.sizes.fields)
	committedOwnership.sizes = committedSizes{
		nodes:    len(model.nodes),
		targets:  len(model.targets),
		owners:   len(model.owners),
		outgoing: len(model.outgoing),
		fields:   len(committedOwnership.index.fields),
	}

	committedOwnership.Unlock()
	committedOwnership.ready.Store(true)
}

func committedRelationIndexSnapshot() *committedRelationIndex {
	committedOwnership.RLock()
	defer committedOwnership.RUnlock()

	return committedOwnership.index
}

func committedRelationModel() *relationModel {
	committedOwnership.RLock()
	defer committedOwnership.RUnlock()

	return committedOwnership.graph
}

func committedChildren(owner nodeKey) []nodeKey {
	committedOwnership.RLock()
	defer committedOwnership.RUnlock()

	return append([]nodeKey(nil), committedOwnership.outgoing[owner]...)
}

func committedOwnerHasChildren(owner nodeKey) bool {
	committedOwnership.RLock()
	defer committedOwnership.RUnlock()

	return len(committedOwnership.outgoing[owner]) > 0
}

// committedOwnershipRoot walks node up to the owner that nothing owns. Two
// nodes share that owner exactly when their ownership branches can overlap,
// which is what tells the Tower which callbacks may run side by side.
func committedOwnershipRoot(node nodeKey) nodeKey {
	committedOwnership.RLock()
	defer committedOwnership.RUnlock()

	index := committedOwnership.index
	if index == nil {
		return node
	}

	// Ownership is acyclic, but the bound keeps a broken index from wedging
	// every writer on a lock this call is holding.
	for range len(index.owners) + 1 {
		owner, found := index.owners[node]
		if !found {
			break
		}

		node = owner
	}

	return node
}

func committedOwnershipKeys(root nodeKey) []nodeKey {
	committedOwnership.RLock()
	keys := committedOwnership.branches[root]
	committedOwnership.RUnlock()
	if keys != nil {
		return keys
	}

	committedOwnership.Lock()
	defer committedOwnership.Unlock()
	if keys = committedOwnership.branches[root]; keys != nil {
		return keys
	}

	out := make([]nodeKey, 1, 1+len(committedOwnership.outgoing[root]))
	out[0] = root
	for position := 0; position < len(out); position++ {
		owner := out[position]
		out = append(out, committedOwnership.outgoing[owner]...)
	}

	if len(out) > 1 {
		slices.SortFunc(out, func(a, b nodeKey) int {
			if byName := cmp.Compare(a.typ.Name(), b.typ.Name()); byName != 0 {
				return byName
			}

			return cmp.Compare(a.id, b.id)
		})
	}

	committedOwnership.branches[root] = out

	return out
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
	nodes := make(map[nodeKey]relationGraphNode, committedSizeHints().nodes)
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
func cloneOwnershipAggregate(root nodeKey) (any, []touchedResource, error) {
	graphMu.RLock()
	defer graphMu.RUnlock()

	if err := ensureCommittedOwnership(); err != nil {
		return nil, nil, err
	}

	if !committedOwnerHasChildren(root) {
		return cloneSingleOwnershipResource(root)
	}

	ordered := committedOwnershipKeys(root)

	for _, key := range ordered {
		committerFor(key.typ.Name()).lockResource(key.id)
	}

	defer func() {
		for i := len(ordered) - 1; i >= 0; i-- {
			committerFor(ordered[i].typ.Name()).unlockResource(ordered[i].id)
		}
	}()

	resources := make([]touchedResource, 0, len(ordered))
	var rootWork any
	for _, key := range ordered {
		workCopy, originalCopy, version, found := committerFor(key.typ.Name()).snapshotResource(key.id)
		if !found {
			return nil, nil, ErrNotFound
		}

		resource := touchedResource{
			dbName: key.typ.Name(), id: key.id, ver: version,
			work: workCopy.Interface(), original: originalCopy.Interface(),
		}
		resources = append(resources, resource)
		if key == root {
			rootWork = resource.work
		}
	}

	if err := rewireOwnershipResources(resources); err != nil {
		return nil, nil, err
	}

	return rootWork, resources, nil
}

func cloneSingleOwnershipResource(root nodeKey) (any, []touchedResource, error) {
	committer := committerFor(root.typ.Name())
	committer.lockResource(root.id)
	defer committer.unlockResource(root.id)

	work, original, version, found := committer.snapshotResource(root.id)
	if !found {
		return nil, nil, ErrNotFound
	}

	resource := touchedResource{
		dbName: root.typ.Name(), id: root.id, ver: version,
		work: work.Interface(), original: original.Interface(),
	}
	if err := rewireSingleOwnershipResource(root, &resource); err != nil {
		return nil, nil, err
	}

	return resource.work, []touchedResource{resource}, nil
}

func rewireSingleOwnershipResource(key nodeKey, resource *touchedResource) error {
	specs, err := relationSpecs(key.typ)
	if err != nil {
		return err
	}

	for _, spec := range specs {
		rewireSelfRelationField(reflect.ValueOf(resource.work).Elem().Field(spec.fieldIndex), spec, key, resource.work)
		rewireSelfRelationField(reflect.ValueOf(resource.original).Elem().Field(spec.fieldIndex), spec, key, resource.original)
	}

	return nil
}

func rewireSelfRelationField(field reflect.Value, spec relationSpec, key nodeKey, target any) {
	if !spec.many {
		rewireSelfRelationPointer(field, spec.target, key, target)
		return
	}

	for i := range field.Len() {
		rewireSelfRelationPointer(field.Index(i), spec.target, key, target)
	}
}

func rewireSelfRelationPointer(pointer reflect.Value, targetType reflect.Type, key nodeKey, target any) {
	id, ok := valueID(pointer)
	if ok && targetType == key.typ && id == key.id {
		pointer.Set(reflect.ValueOf(target))
	}
}

func rewireOwnershipResources(resources []touchedResource) error {
	var positions map[nodeKey]int
	if len(resources) > 1 {
		positions = make(map[nodeKey]int, len(resources))
		for i, resource := range resources {
			typ := baseRegistry[resource.dbName].(relationRuntime).relationType()
			positions[nodeKey{typ: typ, id: resource.id}] = i
		}
	}

	for i := range resources {
		resource := &resources[i]
		typ := baseRegistry[resource.dbName].(relationRuntime).relationType()
		specs, err := relationSpecs(typ)
		if err != nil {
			return err
		}

		for _, spec := range specs {
			rewireResourceField(reflect.ValueOf(resource.work).Elem().Field(spec.fieldIndex), spec, resources, positions, false)
			rewireResourceField(reflect.ValueOf(resource.original).Elem().Field(spec.fieldIndex), spec, resources, positions, true)
		}
	}

	return nil
}

func rewireResourceField(field reflect.Value, spec relationSpec, resources []touchedResource, positions map[nodeKey]int, original bool) {
	if !spec.many {
		rewireResourcePointer(field, spec.target, resources, positions, original)
		return
	}

	for i := range field.Len() {
		rewireResourcePointer(field.Index(i), spec.target, resources, positions, original)
	}
}

func rewireResourcePointer(pointer reflect.Value, targetType reflect.Type, resources []touchedResource, positions map[nodeKey]int, original bool) {
	id, ok := valueID(pointer)
	if !ok {
		return
	}

	position, copied := positions[nodeKey{typ: targetType, id: id}]
	if positions == nil {
		copied = len(resources) == 1 && resources[0].dbName == targetType.Name() && resources[0].id == id
	}

	if !copied {
		return
	}

	target := resources[position].work
	if original {
		target = resources[position].original
	}

	pointer.Set(reflect.ValueOf(target))
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

func resolveGraphTarget(model *relationModel, r relationSpec, pointer reflect.Value) (nodeKey, bool, error) {
	key, present := graphRelationTargetKey(r, pointer)
	if !present {
		return nodeKey{}, false, nil
	}

	target, found := model.targets[key]
	if target.duplicate {
		return nodeKey{}, false, fmt.Errorf("%w: %s.%s does not uniquely identify a target", ErrRelationInvariant, r.owner, r.fieldName)
	}

	return target.key, found, nil
}

func graphRelationTargetKey(r relationSpec, pointer reflect.Value) (relationTargetKey, bool) {
	matchField := r.matchField
	if r.kind == ownRelation && r.many {
		matchField = entityIDField
	}

	key, present := relationKey(pointer, matchField)
	if !present {
		return relationTargetKey{}, false
	}

	return relationTargetKey{typ: r.target, field: matchField, value: key.Interface()}, true
}

func buildRelationModel(nodes map[nodeKey]relationGraphNode) (*relationModel, error) {
	fields, err := relationTargetFields(nodes)
	if err != nil {
		return nil, err
	}

	targets := buildRelationTargetIndexFromFields(nodes, fields, committedSizeHints().targets)
	return buildRelationModelFromTargets(nodes, targets, fields)
}

func buildRelationModelFromTargets(
	nodes map[nodeKey]relationGraphNode,
	targets map[relationTargetKey]indexedRelationTarget,
	targetFields map[reflect.Type]map[string]struct{},
) (*relationModel, error) {
	hints := committedSizeHints()

	// refs, owners and outgoing all hold one entry per edge or per owned node, and
	// a full rebuild reaches the same size the committed model already has. Sizing
	// from it turns eighteen doublings — each copying and abandoning the previous
	// array — into a single allocation. A stale hint only costs the usual growth.
	model := &relationModel{
		nodes: nodes, targets: targets, targetFields: targetFields,
		owners: make(map[nodeKey]nodeKey, hints.owners), outgoing: make(map[nodeKey][]nodeKey, hints.outgoing),
	}

	incoming := make(map[nodeKey]incomingOwns, len(nodes))
	ownedBy := make(map[nodeKey]incomingOwn, len(nodes))
	if hint := committedRelationRefCount(); hint > 0 {
		model.refs = make([]resolvedRelation, 0, hint)
	}

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

func buildRelationModelDelta(
	committed *relationModel,
	nodes map[nodeKey]relationGraphNode,
	targets map[relationTargetKey]indexedRelationTarget,
	rescan map[nodeKey]relationGraphNode,
) (*relationModel, error) {
	model := &relationModel{
		nodes: nodes, targets: targets, targetFields: committed.targetFields,
		owners: make(map[nodeKey]nodeKey, len(committed.owners)), outgoing: make(map[nodeKey][]nodeKey),
		refs: make([]resolvedRelation, 0, len(committed.refs)),
	}
	for child, owner := range committed.owners {
		model.owners[child] = owner
	}

	for _, unresolved := range committed.unresolved {
		if _, resolved := targets[unresolved.lookup]; resolved {
			rescan[unresolved.holder] = nodes[unresolved.holder]
		}
	}

	for _, unresolved := range committed.unresolved {
		if _, changed := rescan[unresolved.holder]; changed {
			continue
		}

		model.unresolved = append(model.unresolved, unresolved)
	}

	affected := make(map[nodeKey]struct{})
	for _, ref := range committed.refs {
		if _, changed := rescan[ref.holder]; changed {
			markOwnershipTarget(affected, ref)
			continue
		}

		model.refs = append(model.refs, ref)
	}

	scannedIncoming := make(map[nodeKey]incomingOwns)
	scannedOwnedBy := make(map[nodeKey]incomingOwn)
	for key, node := range rescan {
		if _, exists := nodes[key]; !exists {
			continue
		}

		if err := scanNodeRelations(model, node, scannedIncoming, scannedOwnedBy); err != nil {
			return nil, err
		}
	}

	for child := range scannedIncoming {
		affected[child] = struct{}{}
	}

	for child := range scannedOwnedBy {
		affected[child] = struct{}{}
	}

	incoming := make(map[nodeKey]incomingOwns, len(affected))
	ownedBy := make(map[nodeKey]incomingOwn, len(affected))
	for _, ref := range model.refs {
		if ref.spec.kind == ownRelation {
			if _, changed := affected[ref.target]; changed {
				owns := incoming[ref.target]
				owns.add(incomingOwn{owner: ref.holder, spec: ref.spec})
				incoming[ref.target] = owns
			}
		}

		if ref.spec.kind == ownedByRelation {
			if _, changed := affected[ref.holder]; changed {
				ownedBy[ref.holder] = incomingOwn{owner: ref.target, spec: ref.spec}
			}
		}
	}

	for child := range affected {
		delete(model.owners, child)
		if err := attachNodeOwner(model, child, incoming[child], ownedBy); err != nil {
			return nil, err
		}
	}

	model.outgoing = make(map[nodeKey][]nodeKey)
	for child, owner := range model.owners {
		model.outgoing[owner] = append(model.outgoing[owner], child)
	}

	if err := validateChangedOwnershipCycles(model, affected); err != nil {
		return nil, err
	}

	return model, nil
}

func markOwnershipTarget(affected map[nodeKey]struct{}, ref resolvedRelation) {
	switch ref.spec.kind {
	case ownRelation:
		affected[ref.target] = struct{}{}
	case ownedByRelation:
		affected[ref.holder] = struct{}{}
	}
}

func validateChangedOwnershipCycles(model *relationModel, changed map[nodeKey]struct{}) error {
	for start := range changed {
		seen := make(map[nodeKey]struct{})
		for at := start; ; {
			if _, duplicate := seen[at]; duplicate {
				return fmt.Errorf("%w: ownership cycle involving %s", ErrRelationInvariant, at)
			}

			seen[at] = struct{}{}
			owner, found := model.owners[at]
			if !found {
				break
			}

			at = owner
		}
	}

	return nil
}

// hint is passed in rather than read from committedOwnership: one caller runs
// under committedOwnership.Lock, and Go's RWMutex is not reentrant.
func buildRelationTargetIndexFromFields(
	nodes map[nodeKey]relationGraphNode,
	fieldsByType map[reflect.Type]map[string]struct{},
	hint int,
) map[relationTargetKey]indexedRelationTarget {
	targets := make(map[relationTargetKey]indexedRelationTarget, hint)
	for nodeKey, node := range nodes {
		indexRelationNodeTargets(targets, node, fieldsByType[nodeKey.typ])
	}

	return targets
}

func relationTargetFields(nodes map[nodeKey]relationGraphNode) (map[reflect.Type]map[string]struct{}, error) {
	fieldsByType := make(map[reflect.Type]map[string]struct{})
	visitedTypes := make(map[reflect.Type]struct{})
	for key := range nodes {
		if _, visited := visitedTypes[key.typ]; visited {
			continue
		}

		visitedTypes[key.typ] = struct{}{}
		specs, err := relationSpecs(key.typ)
		if err != nil {
			return nil, err
		}

		for _, spec := range specs {
			if spec.kind == inverseRelation {
				continue
			}

			matchField := spec.matchField
			if spec.kind == ownRelation && spec.many {
				matchField = entityIDField
			}

			if fieldsByType[spec.target] == nil {
				fieldsByType[spec.target] = make(map[string]struct{})
			}

			fieldsByType[spec.target][matchField] = struct{}{}
		}
	}

	return fieldsByType, nil
}

func indexRelationNodeTargets(targets map[relationTargetKey]indexedRelationTarget, node relationGraphNode, fields map[string]struct{}) {
	for field := range fields {
		value, present := relationKey(node.value, field)
		if !present {
			continue
		}

		key := relationTargetKey{typ: node.key.typ, field: field, value: value.Interface()}
		target, duplicate := targets[key]
		if duplicate {
			target.duplicate = true
			targets[key] = target
			continue
		}

		targets[key] = indexedRelationTarget{key: node.key}
	}
}

func scanNodeRelations(model *relationModel, node relationGraphNode, incoming map[nodeKey]incomingOwns, ownedBy map[nodeKey]incomingOwn) error {
	specs, err := relationSpecs(node.key.typ)
	if err != nil {
		return err
	}

	for index := range specs {
		spec := &specs[index]
		if spec.kind == inverseRelation {
			continue
		}

		if err := scanRelationField(model, node, spec, incoming, ownedBy); err != nil {
			return err
		}
	}

	return nil
}

func scanRelationField(model *relationModel, node relationGraphNode, spec *relationSpec, incoming map[nodeKey]incomingOwns, ownedBy map[nodeKey]incomingOwn) error {
	field := node.value.Elem().Field(spec.fieldIndex)
	if !spec.many {
		return scanRelationPointer(model, node, spec, field, nil, incoming, ownedBy)
	}

	var seen map[nodeKey]struct{}
	if spec.kind == ownRelation {
		seen = make(map[nodeKey]struct{}, field.Len())
	}

	for i := range field.Len() {
		if err := scanRelationPointer(model, node, spec, field.Index(i), seen, incoming, ownedBy); err != nil {
			return err
		}
	}

	return nil
}

func scanRelationPointer(
	model *relationModel,
	node relationGraphNode,
	spec *relationSpec,
	pointer reflect.Value,
	seen map[nodeKey]struct{},
	incoming map[nodeKey]incomingOwns,
	ownedBy map[nodeKey]incomingOwn,
) error {
	target, found, err := resolveGraphTarget(model, *spec, pointer)
	if err != nil {
		return err
	}

	if !found {
		if lookup, present := graphRelationTargetKey(*spec, pointer); present {
			model.unresolved = append(model.unresolved, unresolvedRelation{
				holder: node.key, spec: *spec, lookup: lookup,
			})
		}

		if relationMayBeMissing(*spec) {
			return nil
		}

		model.missing = append(model.missing, missingRelation{holder: node.key, spec: *spec})
		return nil
	}

	if _, duplicate := seen[target]; duplicate && spec.kind == ownRelation {
		return fmt.Errorf("%w: %s.%s contains owned child %s more than once", ErrRelationInvariant, node.key.typ, spec.fieldName, target)
	}

	if seen != nil {
		seen[target] = struct{}{}
	}

	recordResolvedRelation(model, node.key, target, spec, incoming, ownedBy)

	return nil
}

func relationMayBeMissing(spec relationSpec) bool {
	if spec.kind == optionRelation {
		return true
	}

	return spec.many && (spec.kind == borrowRelation || spec.kind == ownRelation)
}

func recordResolvedRelation(model *relationModel, holder, target nodeKey, spec *relationSpec, incoming map[nodeKey]incomingOwns, ownedBy map[nodeKey]incomingOwn) {
	model.refs = append(model.refs, resolvedRelation{holder: holder, target: target, spec: spec})
	edge := incomingOwn{owner: holder, spec: spec}
	if spec.kind == ownRelation {
		owns := incoming[target]
		owns.add(edge)
		incoming[target] = owns
	}

	if spec.kind == ownedByRelation {
		edge.owner = target
		ownedBy[holder] = edge
	}
}

func attachNodeOwner(model *relationModel, child nodeKey, raw incomingOwns, ownedBy map[nodeKey]incomingOwn) error {
	back, hasBack := ownedBy[child]
	if raw.count > 1 {
		return fmt.Errorf("%w: %s has more than one owner", ErrRelationInvariant, child)
	}

	if raw.count == 1 && hasBack && raw.first.owner != back.owner {
		return fmt.Errorf("%w: own and ownedby disagree for %s (%s vs %s)", ErrRelationInvariant, child, raw.first.owner, back.owner)
	}

	owner, found := resolvedOwner(raw, back, hasBack)
	if !found {
		return nil
	}

	model.owners[child] = owner
	model.outgoing[owner] = append(model.outgoing[owner], child)
	return nil
}

func resolvedOwner(raw incomingOwns, back incomingOwn, hasBack bool) (nodeKey, bool) {
	if raw.count == 1 {
		return raw.first.owner, true
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
	state := make(map[nodeKey]uint8, len(model.nodes))
	path := make([]nodeKey, 0)
	for start := range model.nodes {
		if state[start] != 0 {
			continue
		}

		path = path[:0]
	walk:
		for at := start; ; {
			switch state[at] {
			case 1:
				return fmt.Errorf("%w: ownership cycle involving %s", ErrRelationInvariant, at)
			case 2:
				break walk
			}

			state[at] = 1
			path = append(path, at)
			owner, found := model.owners[at]
			if found {
				at = owner
				continue
			}

			break walk
		}

		for _, key := range path {
			state[key] = 2
		}
	}

	return nil
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
		case borrowRelation:
			return nil, fmt.Errorf("%w: %s borrows doomed %s through %s", ErrDeleteRestricted, ref.holder, ref.target, ref.spec.fieldName)
		case ownRelation:
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
			if ref.spec.kind == optionRelation && !ref.spec.many && setRelationField(holder, *ref.spec, reflect.Value{}) {
				changed[ref.holder] = struct{}{}
			}

			continue
		}

		if !ref.spec.many && setRelationField(holder, *ref.spec, target.value) {
			changed[ref.holder] = struct{}{}
		}
	}
}

func syncOwnSliceBackReferences(model *relationModel, deleted, changed map[nodeKey]struct{}) {
	for _, ref := range model.refs {
		if ref.spec.kind != ownRelation || !ref.spec.many {
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
	if ownSliceBackReferenceMatches(model, ref) {
		return false
	}

	if back.Kind() == reflect.Pointer {
		back.Set(owner.value)
		return true
	}

	id := owner.value.Elem().FieldByName(entityIDField)
	back.Set(id)
	return true
}

func ownSliceBackReferenceMatches(model *relationModel, ref resolvedRelation) bool {
	owner := model.nodes[ref.holder]
	child := model.nodes[ref.target]
	back := child.value.Elem().FieldByName(ref.spec.matchField)
	if back.Kind() == reflect.Pointer {
		return !back.IsNil() && back.Pointer() == owner.value.Pointer()
	}

	id := owner.value.Elem().FieldByName(entityIDField)
	return scalarEqual(back, id)
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

	runtimes := relationRuntimes()
	hasRelations, err := registeredRelations(runtimes)
	if err != nil {
		return err
	}

	if !hasRelations {
		return flushWithoutRelations(runtimes)
	}

	done, err := flushIncremental(runtimes)
	if done || err != nil {
		return err
	}

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

	for _, runtime := range runtimes {
		if err := runtime.relationApplyPending(); err != nil {
			return err
		}
	}

	if err := bindPendingRelationModelToLive(model, runtimes); err != nil {
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

	rewireRelations(runtimes)
	for _, runtime := range runtimes {
		if err := runtime.relationReindex(); err != nil {
			return err
		}
	}

	for _, runtime := range runtimes {
		if err := runtime.relationSave(); err != nil {
			return err
		}
	}

	if err := sharedTransactionWAL().truncate(); err != nil {
		return err
	}

	for _, runtime := range runtimes {
		runtime.relationClearQueues()
	}

	storeCommittedOwnership(model, deleted)

	// Live and committed agree from here, so the replica can be re-pointed at the
	// index just published and serve as the baseline the next flush compares
	// against.
	projectTower.refreshAfterCommit()

	return nil
}

// flushIncremental settles a flush against the Tower shadow instead of
// rebuilding the model and the index from every node in the project.
//
// The shadow is the last committed state, so comparing against it derives the
// change set — which is what makes this sound under Unsafe's contract, where a
// caller may mutate through a pointer it kept since creation and nothing
// records that. Three outcomes:
//
//   - nothing moved and nothing is queued: persist values and stop;
//   - relations moved between existing nodes, or rows were created: feed the
//     derived change set to the transaction engine's delta builder, the same
//     already-tested route a transactional create takes;
//   - anything the delta cannot prove — deletes, lookup-key changes, or a
//     builder refusal — falls through to the full rebuild, so every rejection
//     lands in today's code path.
func flushIncremental(runtimes []relationRuntime) (bool, error) {
	deletes := explicitDeletes()

	// Identity, not synced(): Unsafe.Flush bumps the table epoch before reaching
	// here, so synced() is always false by now. What matters is that the
	// replica's relation baseline was taken from the index still published — if
	// it was, the baseline is the committed graph and the comparison is exact.
	replica := projectTower.replica.Load()
	if replica == nil || replica.index == nil || replica.index != committedRelationIndexSnapshot() {
		return false, nil
	}

	moved, keyChanged, err := replica.graphMovedNodes()
	if err != nil || keyChanged {
		return false, nil
	}

	if len(deletes) > 0 {
		// A delete flush takes its own, stricter route: it must not coexist with
		// creates or relation moves, or the closure would need the live model the
		// whole point is not to build.
		if len(moved) > 0 {
			return false, nil
		}

		for _, runtime := range runtimes {
			if len(runtime.relationPending()) > 0 {
				return false, nil
			}
		}

		return flushDeletesViaDelta(runtimes, replica, deletes)
	}

	creates := make(map[nodeKey]createdResource)
	for _, runtime := range runtimes {
		typ := runtime.relationType()
		for _, pending := range runtime.relationPending() {
			id, present := valueID(pending)
			if !present {
				return false, nil
			}

			creates[nodeKey{typ: typ, id: id}] = createdResource{
				key:  nodeKey{typ: typ, id: id},
				work: pending,
			}
		}
	}

	if len(moved) == 0 && len(creates) == 0 {
		return true, flushPersist(runtimes)
	}

	touched := make([]touchedResource, 0, len(moved))
	for _, key := range moved {
		touched = append(touched, touchedResource{
			dbName: key.typ.Name(),
			id:     key.id,
			work:   replica.live[key].Interface(),
		})
	}

	delta, err := buildRelationCreateIndexDelta(touched, creates)
	if err != nil || delta == nil {
		return false, nil
	}

	changed, err := delta.materializeOwnBackReferences(nil, touched, creates)
	if err != nil {
		return false, nil
	}

	for _, runtime := range runtimes {
		if err := runtime.relationApplyPending(); err != nil {
			return false, err
		}
	}

	for key := range creates {
		runtime, ok := baseRegistry[key.typ.Name()].(relationRuntime)
		if !ok {
			return false, fmt.Errorf("%w: created node %s has no runtime", ErrRelationInvariant, key)
		}

		live, found := runtime.relationValue(key.id)
		if !found {
			return false, fmt.Errorf("%w: created node %s is missing", ErrRelationInvariant, key)
		}

		delta.bindCreatedNode(key, live)
	}

	// Resources materialize added beyond the derived set carry detached copies
	// whose back references it just set; they must be written to the live rows,
	// exactly as the engine does after a transactional create. The derived
	// resources only need their chunks marked dirty — their work value is the
	// live pointer itself.
	for _, resource := range changed[len(touched):] {
		committerFor(resource.dbName).applyWrite(resource.id, resource.work)
	}

	for _, resource := range touched {
		if runtime, ok := baseRegistry[resource.dbName].(relationRuntime); ok {
			runtime.relationMarkDirty(resource.id)
		}
	}

	if err := flushPersist(runtimes); err != nil {
		return false, err
	}

	delta.publishAndRewire()
	projectTower.refreshAfterCommit()

	return true, nil
}

// flushDeletesViaDelta settles a pure-delete flush against committed state:
// the cascade closure comes from the committed ownership forest, the veto scan
// from the committed incoming counts, and survivor updates ride the same field
// delta a transactional write uses. Anything the committed state cannot prove —
// a doomed node behind a duplicate lookup key, a surviving borrow or required
// owner, a delete of something never committed — falls through to the full
// rebuild, which raises today's canonical errors.
func flushDeletesViaDelta(
	runtimes []relationRuntime,
	replica *towerReplica,
	explicit map[nodeKey]struct{},
) (bool, error) {
	index := replica.index

	// Cascade over the committed forest.
	closure := make(map[nodeKey]struct{}, len(explicit))
	queue := make([]nodeKey, 0, len(explicit))
	for key := range explicit {
		if _, committed := index.nodes[key]; !committed {
			return false, nil
		}

		closure[key] = struct{}{}
		queue = append(queue, key)
	}

	for len(queue) > 0 {
		owner := queue[0]
		queue = queue[1:]
		for _, child := range committedChildren(owner) {
			if _, doomed := closure[child]; doomed {
				continue
			}

			closure[child] = struct{}{}
			queue = append(queue, child)
		}
	}

	// Veto scan, and the survivor work list. Only own-many drops and option
	// clears are expressible as a delta; everything else vetoes or falls back.
	type survivorField struct {
		holder nodeKey
		spec   *relationSpec
	}

	var survivors []survivorField
	seen := make(map[relationHolderField]struct{})
	for doomed := range closure {
		for _, entry := range index.incoming[doomed].entries {
			if entry.count <= 0 {
				continue
			}

			if _, dies := closure[entry.field.holder]; dies {
				continue
			}

			field, indexed := index.fields[entry.field]
			if !indexed || field.spec == nil {
				return false, nil
			}

			kind, many := field.spec.kind, field.spec.many
			if kind == borrowRelation || kind == ownedByRelation || (kind == ownRelation && !many) {
				return false, nil
			}

			if _, duplicate := seen[entry.field]; duplicate {
				continue
			}

			seen[entry.field] = struct{}{}
			survivors = append(survivors, survivorField{holder: entry.field.holder, spec: field.spec})
		}

		// A doomed node behind a duplicate lookup key cannot be removed from the
		// target index without recomputing which collision survives.
		for lookupField := range index.targetFields[doomed.typ] {
			value, present := relationKey(index.nodes[doomed], lookupField)
			if !present {
				continue
			}

			lookup := relationTargetKey{typ: doomed.typ, field: lookupField, value: value.Interface()}
			if index.targets[lookup].duplicate {
				return false, nil
			}
		}
	}

	// Mutate the surviving holders' live fields. From here the live graph is in
	// its post-delete shape, so even a builder fallback below converges: the
	// full rebuild derives the same end state from these same pointers.
	touched := make([]touchedResource, 0, len(survivors))
	for _, survivor := range survivors {
		holder := index.nodes[survivor.holder]
		field := holder.Elem().Field(survivor.spec.fieldIndex)
		if survivor.spec.many {
			dropDoomedPointers(field, survivor.spec.target, closure)
		} else if id, present := valueID(field); present {
			if _, dies := closure[nodeKey{typ: survivor.spec.target, id: id}]; dies {
				clearPointer(field)
			}
		}

		touched = append(touched, touchedResource{
			dbName: survivor.holder.typ.Name(),
			id:     survivor.holder.id,
			work:   holder.Interface(),
		})
	}

	if len(touched) > 0 {
		delta, err := buildRelationIndexDelta(touched)
		if err != nil || delta == nil {
			return false, nil
		}

		delta.publishAndRewire()
		for _, resource := range touched {
			if runtime, ok := baseRegistry[resource.dbName].(relationRuntime); ok {
				runtime.relationMarkDirty(resource.id)
			}
		}
	}

	removeDeletedFromCommittedIndex(index, closure)

	byType := make(map[reflect.Type]map[int]struct{})
	for key := range closure {
		if byType[key.typ] == nil {
			byType[key.typ] = make(map[int]struct{})
		}

		byType[key.typ][key.id] = struct{}{}
	}

	for _, runtime := range runtimes {
		runtime.relationDelete(byType[runtime.relationType()])
	}

	if err := flushPersist(runtimes); err != nil {
		return false, err
	}

	towerForgetNodes(closure)
	projectTower.refreshAfterCommit()

	return true, nil
}

// dropDoomedPointers compacts a live []*T in place, keeping every entry whose
// target survives. In-place, so the survivor keeps its backing array.
func dropDoomedPointers(field reflect.Value, target reflect.Type, closure map[nodeKey]struct{}) {
	kept := 0
	for position := range field.Len() {
		element := field.Index(position)
		if id, present := valueID(element); present {
			if _, dies := closure[nodeKey{typ: target, id: id}]; dies {
				continue
			}
		}

		field.Index(kept).Set(element)
		kept++
	}

	field.SetLen(kept)
}

// removeDeletedFromCommittedIndex erases the closure from every derived
// structure: lookup targets, outgoing relation fields (decrementing surviving
// targets' incoming counts), ownership edges, and finally the nodes themselves.
// Surviving targets of a doomed borrow or option holder get their inverse views
// recomputed, since those views are derived from the counts just decremented.
func removeDeletedFromCommittedIndex(index *committedRelationIndex, closure map[nodeKey]struct{}) {
	inverseRefresh := make(map[nodeKey]struct{})

	committedOwnership.Lock()
	for doomed := range closure {
		specs, err := relationSpecs(doomed.typ)
		if err == nil {
			for specIndex := range specs {
				spec := &specs[specIndex]
				if spec.kind == inverseRelation {
					continue
				}

				holderField := relationHolderField{holder: doomed, field: spec.fieldIndex}
				field, indexed := index.fields[holderField]
				if !indexed {
					continue
				}

				for _, target := range field.targets {
					if _, dies := closure[target]; dies {
						continue
					}

					index.decrementIncoming(target, holderField)
					if spec.kind == borrowRelation || spec.kind == optionRelation {
						inverseRefresh[target] = struct{}{}
					}
				}

				delete(index.fields, holderField)
			}
		}

		for lookupField := range index.targetFields[doomed.typ] {
			value, present := relationKey(index.nodes[doomed], lookupField)
			if !present {
				continue
			}

			delete(index.targets, relationTargetKey{typ: doomed.typ, field: lookupField, value: value.Interface()})
		}

		if owner, owned := index.owners[doomed]; owned {
			removeCommittedChild(owner, doomed)
			delete(index.owners, doomed)
		}

		delete(committedOwnership.outgoing, doomed)
		delete(index.incoming, doomed)
		delete(index.nodes, doomed)
	}

	committedOwnership.graph = nil
	clear(committedOwnership.branches)
	committedOwnership.Unlock()

	for target := range inverseRefresh {
		value, exists := index.nodes[target]
		if !exists {
			continue
		}

		specs, err := relationSpecs(target.typ)
		if err != nil {
			continue
		}

		for _, spec := range specs {
			if spec.kind != inverseRelation {
				continue
			}

			holders := index.inverseHolders(target, spec)
			setIndexedRelationField(value.Elem().Field(spec.fieldIndex), holders, index.nodes)
		}
	}
}

// flushPersist is the tail every incremental outcome shares: field values still
// have to be reindexed and written even when the graph did not change shape.
func flushPersist(runtimes []relationRuntime) error {
	for _, runtime := range runtimes {
		if err := runtime.relationReindex(); err != nil {
			return err
		}
	}

	for _, runtime := range runtimes {
		if err := runtime.relationSave(); err != nil {
			return err
		}
	}

	if err := sharedTransactionWAL().truncate(); err != nil {
		return err
	}

	for _, runtime := range runtimes {
		runtime.relationClearQueues()
	}

	return nil
}

func bindPendingRelationModelToLive(model *relationModel, runtimes []relationRuntime) error {
	for _, runtime := range runtimes {
		for _, pending := range runtime.relationPending() {
			id, present := valueID(pending)
			if !present {
				return fmt.Errorf("%w: pending %s has no id", ErrRelationInvariant, runtime.relationType())
			}

			key := nodeKey{typ: runtime.relationType(), id: id}
			value, found := runtime.relationValue(id)
			if !found {
				return fmt.Errorf("%w: pending node %s is missing", ErrRelationInvariant, key)
			}

			model.nodes[key] = relationGraphNode{key: key, value: value}
		}
	}

	return nil
}

func registeredRelations(runtimes []relationRuntime) (bool, error) {
	for _, runtime := range runtimes {
		specs, err := relationSpecs(runtime.relationType())
		if err != nil {
			return false, err
		}

		if len(specs) > 0 {
			return true, nil
		}
	}

	return false, nil
}

func registryHasRelations() (bool, error) {
	for _, registered := range baseRegistry {
		runtime, ok := registered.(relationRuntime)
		if !ok {
			continue
		}

		specs, err := relationSpecs(runtime.relationType())
		if err != nil {
			return false, err
		}

		if len(specs) > 0 {
			return true, nil
		}
	}

	return false, nil
}

func flushWithoutRelations(runtimes []relationRuntime) error {
	for _, runtime := range runtimes {
		if err := runtime.relationApplyPending(); err != nil {
			return err
		}
	}

	for _, runtime := range runtimes {
		ids := runtime.relationDeleteIDs()
		if len(ids) == 0 {
			continue
		}

		deleted := make(map[int]struct{}, len(ids))
		for _, id := range ids {
			deleted[id] = struct{}{}
		}

		runtime.relationDelete(deleted)
	}

	for _, runtime := range runtimes {
		if err := runtime.relationReindex(); err != nil {
			return err
		}
	}

	for _, runtime := range runtimes {
		if err := runtime.relationSave(); err != nil {
			return err
		}
	}

	if err := sharedTransactionWAL().truncate(); err != nil {
		return err
	}

	for _, runtime := range runtimes {
		runtime.relationClearQueues()
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
