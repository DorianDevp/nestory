package nestory

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
)

// Tower coordinates one persistent shadow graph for the complete project.
// Tables remain separate inside the replica, while relation pointers are wired
// exclusively to nodes from the same shadow world.
type Tower struct {
	// rebuildMu makes a burst of writers produce one replica instead of one
	// each. It is never held across a callback, so a rebuild can never queue
	// behind user code.
	rebuildMu sync.Mutex
	replica   atomic.Pointer[towerReplica]
}

// towerReplica is one reading of the committed graph. Its maps are filled
// before publication and only read afterwards, so a rebuild publishes a
// replacement rather than mutating what running callbacks are looking at —
// which is what keeps callbacks on disjoint branches out of each other's way.
type towerReplica struct {
	// epochs is the per-table counter reading this replica was built from,
	// covering every table that takes part in the relation graph. A commit
	// replaces the whole map, never an entry.
	epochs atomic.Pointer[map[string]uint64]
	nodes  map[nodeKey]reflect.Value
	// blocks owns the storage every shadow node lives in, one contiguous array
	// per table. Nothing reads it — the node pointers alone would keep it alive
	// — but naming the owner is what makes the never-grow invariant visible.
	blocks    []reflect.Value
	live      map[nodeKey]reflect.Value
	relations map[towerRelationKey]towerRelationState
	// index is the committed index this replica's relation baseline was taken
	// from. Identity against the published index is a stronger statement than
	// synced(): it survives an epoch bump that has not changed the graph, which
	// is exactly the case Flush needs to recognise.
	index *committedRelationIndex

	// branchMu guards branches, the only part of a published replica that
	// still fills in lazily.
	branchMu sync.Mutex
	branches map[nodeKey][]towerBranchNode
}

type towerChange struct {
	key      nodeKey
	live     reflect.Value
	shadow   reflect.Value
	fields   []int
	resource touchedResource
}

type towerBranchNode struct {
	key    nodeKey
	live   reflect.Value
	shadow reflect.Value
	// liveEntity and shadowEntity are the same two pointers boxed once, when the
	// branch is cached. towerComparator takes any, and paying Value.Interface
	// per diff would cost more than the comparison it feeds.
	liveEntity   any
	shadowEntity any
}

type towerFieldPlan struct {
	index      int
	relational bool
	many       bool
}

type cachedTowerFieldPlan struct {
	fields []towerFieldPlan
	// equal is non-nil exactly when the whole struct can be settled in one
	// comparison: the type carries no relation and nothing inside it can be
	// incomparable at runtime.
	equal towerComparator
	err   error
}

// towerComparator answers whether two *T hold the same struct. Register builds
// one per entity type, so the comparison the compiler emits for that struct is
// what runs — reflect walks the value field by field on every call instead, and
// that walk was the bulk of a diff.
type towerComparator func(left, right any) bool

// towerSliceComparator answers whether two []*C hold the same pointers in the
// same order. Register builds one per entity type as well, so an owned slice
// compares in a native loop instead of one reflect hop per element.
type towerSliceComparator func(left, right any) bool

type towerRelationKey struct {
	node  nodeKey
	field int
}

type towerRelationState struct {
	present bool
	one     int
	// many is the []*C the shadow held when the replica was built, copied so
	// the callback cannot mutate the baseline it is compared against. Holding
	// the pointers rather than their ids costs the same memory and lets the
	// whole slice settle in one native comparison.
	many any
}

var (
	projectTower    = newTower()
	towerEpochs     sync.Map
	towerFieldPlans sync.Map
	// towerWriteLocks holds one mutex per top-level owner. Roots that share one
	// are the only pairs whose ownership branches can overlap.
	towerWriteLocks sync.Map
	// towerComparators maps reflect.Type to the towerComparator Register built.
	towerComparators sync.Map
	// towerSliceComparators maps the []*T type to its towerSliceComparator.
	towerSliceComparators sync.Map
)

// registerTowerComparator records how to compare two values of T without
// reflect. The == is between two any values rather than two T, because Entity
// carries no comparable constraint and the compiler will not emit == for a bare
// type parameter; the runtime resolves it from the dynamic type instead, which
// costs one indirection and still beats a reflect walk several times over. It
// panics on a type that is not comparable, so only screened types are stored.
func registerTowerComparator[T Entity]() {
	typ := reflect.TypeFor[T]()
	if !towerAlwaysComparable(typ) {
		return
	}

	towerComparators.Store(typ, towerComparator(func(left, right any) bool {
		return any(*left.(*T)) == any(*right.(*T))
	}))
}

// registerTowerSliceComparator records how to compare two owned slices of T.
// Pointers are comparable whatever T is, so this one needs no screening.
func registerTowerSliceComparator[T Entity]() {
	towerSliceComparators.Store(
		reflect.TypeFor[[]*T](),
		towerSliceComparator(func(left, right any) bool {
			return slices.Equal(left.([]*T), right.([]*T))
		}),
	)
}

func newTower() *Tower {
	return &Tower{}
}

// retire drops a replica whose shadow no longer matches committed state. The
// swap is conditional so a writer cannot throw away a newer replica that
// somebody else published while its own callback was running.
func (tower *Tower) retire(replica *towerReplica) {
	tower.replica.CompareAndSwap(replica, nil)
}

// resetTower drops the replica but deliberately keeps towerEpochs: counters
// only ever get compared against a snapshot taken at rebuild, so letting them
// run on across resets is harmless, and it means a table can hold on to its own
// counter instead of looking it up by name on every write.
func resetTower() {
	projectTower = newTower()
	towerFieldPlans = sync.Map{}
	towerWriteLocks = sync.Map{}
}

// towerWriteLock serializes the callbacks whose branches can touch the same
// shadow nodes. Nested roots — a node and one of its own ancestors — share a
// top-level owner and queue behind each other; roots in separate trees never do.
func towerWriteLock(owner nodeKey) *sync.Mutex {
	if lock, found := towerWriteLocks.Load(owner); found {
		return lock.(*sync.Mutex)
	}

	lock, _ := towerWriteLocks.LoadOrStore(owner, &sync.Mutex{})

	return lock.(*sync.Mutex)
}

// towerEpochCounter allocates only on a table's first sighting. LoadOrStore
// would box a fresh counter on every call, and these run on the commit path.
func towerEpochCounter(typeName string) *atomic.Uint64 {
	if counter, found := towerEpochs.Load(typeName); found {
		return counter.(*atomic.Uint64)
	}

	counter, _ := towerEpochs.LoadOrStore(typeName, &atomic.Uint64{})

	return counter.(*atomic.Uint64)
}

func towerEpoch(typeName string) uint64 {
	return towerEpochCounter(typeName).Load()
}

func markTowerTableChanged(typeName string) {
	towerEpochCounter(typeName).Add(1)
}

// towerParticipantEpochs reads the current epoch of every table that can appear
// in an ownership branch. Tables outside the relation graph are skipped on
// purpose: their rows never reach a branch, so writing them cannot stale a
// shadow node and must not cost a rebuild.
func towerParticipantEpochs() map[string]uint64 {
	relationParticipantsMu.RLock()
	defer relationParticipantsMu.RUnlock()

	epochs := make(map[string]uint64, len(relationParticipants))
	for typ := range relationParticipants {
		name := typ.Name()
		epochs[name] = towerEpoch(name)
	}

	return epochs
}

// synced reports whether every participating table still carries the epoch this
// replica was built from. A participant registered after the rebuild is absent
// from the snapshot and compares against zero, which is exactly the epoch a
// table keeps until its first row lands — the first Open or add bumps it. A nil
// replica is never synced, which is how a retired one asks for a rebuild.
func (replica *towerReplica) synced() bool {
	if replica == nil {
		return false
	}

	epochs := *replica.epochs.Load()

	relationParticipantsMu.RLock()
	defer relationParticipantsMu.RUnlock()

	for typ := range relationParticipants {
		name := typ.Name()
		if epochs[name] != towerEpoch(name) {
			return false
		}
	}

	return true
}

// refresh republishes the live replica in place when the committed node set is
// unchanged — same keys, same live pointers. Every shadow then keeps its
// address, so blocks, the nodes map and the live map are all reused and a
// commit that only changed field values allocates nothing per node.
//
// Anything that adds or removes a node falls back to rebuild. That is what
// keeps blocks' never-grow invariant intact: a shadow address is handed to user
// code inside an UpdateWithin callback and cached in branches, so a block that
// reallocated would leave both pointing at a copy nobody publishes.
//
// The caller holds graphMu.Lock, which excludes every reader of the replica, so
// mutating the shadows in place cannot race a diff in progress.
func (tower *Tower) refresh(
	current *towerReplica,
	index *committedRelationIndex,
	epochs map[string]uint64,
) (bool, error) {
	if current == nil || len(index.nodes) != len(current.live) {
		return false, nil
	}

	for key, value := range index.nodes {
		live, ok := current.live[key]
		if !ok || live.Pointer() != value.Pointer() {
			return false, nil
		}
	}

	// Copy only the nodes that actually differ. Comparison is free and reads
	// nothing off-heap; the copy is not, because cloneSliceFields gives every
	// slice field its own backing array. Re-copying an unchanged node allocates
	// one array per slice field to reproduce bytes that already match.
	var currentType reflect.Type
	var plan cachedTowerFieldPlan
	var err error

	for key, value := range index.nodes {
		if key.typ != currentType {
			currentType = key.typ
			plan, err = towerFields(currentType)
			if err != nil {
				return false, err
			}
		}

		shadow := current.nodes[key]
		if towerNodeUnchanged(value.Elem(), shadow.Elem(), plan) {
			continue
		}

		shadow.Elem().Set(value.Elem())
		cloneSliceFields(shadow.Elem())
	}

	if err := wireTowerNodes(current.nodes); err != nil {
		return false, err
	}

	relations, err := snapshotTowerRelations(current.nodes)
	if err != nil {
		return false, err
	}

	current.relations = relations
	current.index = index

	// Ownership may have moved between nodes that all still exist, so the cached
	// branches are the one derived structure a refresh cannot keep.
	current.branchMu.Lock()
	current.branches = make(map[nodeKey][]towerBranchNode)
	current.branchMu.Unlock()

	current.epochs.Store(&epochs)

	return true, nil
}

func (tower *Tower) rebuild() (*towerReplica, error) {
	if err := ensureCommittedOwnership(); err != nil {
		return nil, err
	}

	tower.rebuildMu.Lock()
	defer tower.rebuildMu.Unlock()

	// Somebody else may have published a fresh replica while this writer waited.
	if current := tower.replica.Load(); current.synced() {
		return current, nil
	}

	graphMu.Lock()
	defer graphMu.Unlock()

	// Read the epochs before the graph, not after. A write slipping between the
	// two then leaves the replica looking stale — one wasted rebuild — instead
	// of letting stale nodes look fresh.
	epochs := towerParticipantEpochs()

	committedOwnership.RLock()
	index := committedOwnership.index
	if index == nil {
		committedOwnership.RUnlock()
		return nil, fmt.Errorf("nestory: committed relation index is unavailable")
	}

	if refreshed, err := tower.refresh(tower.replica.Load(), index, epochs); refreshed || err != nil {
		committedOwnership.RUnlock()

		return tower.replica.Load(), err
	}

	liveNodes := make(map[nodeKey]reflect.Value, len(index.nodes))
	for key, value := range index.nodes {
		liveNodes[key] = value
	}

	committedOwnership.RUnlock()

	nodes, blocks := buildTowerShadows(liveNodes)

	if err := wireTowerNodes(nodes); err != nil {
		return nil, err
	}
	relations, err := snapshotTowerRelations(nodes)
	if err != nil {
		return nil, err
	}

	replica := &towerReplica{
		nodes:     nodes,
		blocks:    blocks,
		live:      liveNodes,
		relations: relations,
		index:     index,
		branches:  make(map[nodeKey][]towerBranchNode),
	}
	replica.epochs.Store(&epochs)
	tower.replica.Store(replica)

	return replica, nil
}

// buildTowerShadows lays every shadow node of one table end to end in a single
// array instead of allocating each on its own. The array is sized once and
// never grows, so the address of each element stays valid for the replica's
// life — the same invariant chunkStore relies on for live entities.
//
// Nodes go in ascending id, which is the order committedOwnershipKeys sorts a
// branch into, so diffing walks the array front to back instead of chasing
// pointers all over the heap.
func buildTowerShadows(liveNodes map[nodeKey]reflect.Value) (map[nodeKey]reflect.Value, []reflect.Value) {
	byTable := make(map[reflect.Type][]nodeKey)
	for key := range liveNodes {
		byTable[key.typ] = append(byTable[key.typ], key)
	}

	nodes := make(map[nodeKey]reflect.Value, len(liveNodes))
	blocks := make([]reflect.Value, 0, len(byTable))
	for typ, keys := range byTable {
		slices.SortFunc(keys, func(a, b nodeKey) int { return cmp.Compare(a.id, b.id) })

		block := reflect.MakeSlice(reflect.SliceOf(typ), len(keys), len(keys))
		blocks = append(blocks, block)
		for index, key := range keys {
			shadow := block.Index(index).Addr()
			shadow.Elem().Set(liveNodes[key].Elem())
			cloneSliceFields(shadow.Elem())
			nodes[key] = shadow
		}
	}

	return nodes, blocks
}

func snapshotTowerRelations(nodes map[nodeKey]reflect.Value) (map[towerRelationKey]towerRelationState, error) {
	relations := make(map[towerRelationKey]towerRelationState)
	for key, node := range nodes {
		specs, err := relationSpecs(key.typ)
		if err != nil {
			return nil, err
		}

		for _, spec := range specs {
			field := node.Elem().Field(spec.fieldIndex)
			state := towerRelationState{}
			if !spec.many {
				state.one, state.present = valueID(field)
			} else {
				baseline := reflect.MakeSlice(field.Type(), field.Len(), field.Len())
				reflect.Copy(baseline, field)
				state.many = baseline.Interface()
			}
			relations[towerRelationKey{node: key, field: spec.fieldIndex}] = state
		}
	}

	return relations, nil
}

func wireTowerNodes(nodes map[nodeKey]reflect.Value) error {
	for key, node := range nodes {
		specs, err := relationSpecs(key.typ)
		if err != nil {
			return err
		}

		for _, spec := range specs {
			field := node.Elem().Field(spec.fieldIndex)
			if !spec.many {
				id, present := valueID(field)
				if !present {
					continue
				}

				target := nodes[nodeKey{typ: spec.target, id: id}]
				if target.IsValid() {
					field.Set(target)
				}

				continue
			}

			for position := range field.Len() {
				id, present := valueID(field.Index(position))
				if !present {
					continue
				}

				target := nodes[nodeKey{typ: spec.target, id: id}]
				if target.IsValid() {
					field.Index(position).Set(target)
				}
			}
		}
	}

	return nil
}

func towerUpdateWithin[T Entity](db *DB[T], id int, fn func(*T) error) (bool, error) {
	// Screen the table before building the closure: wrapping fn allocates, and
	// most writes in a project never reach the Tower at all.
	typ := reflect.TypeFor[T]()
	if !relationGraphParticipant(typ) {
		return false, nil
	}

	return towerRun(typ, id, func(shadow reflect.Value, _ nodeKey) ([]nodeKey, error) {
		return nil, fn(shadow.Interface().(*T))
	})
}

// towerEdit runs the user callback against the shadow root and reports which
// nodes it promises to have changed. A nil slice means no promise was made and
// the whole branch has to be diffed. owner is the tree the write is locked on,
// passed in because towerRun has already resolved it.
type towerEdit func(shadow reflect.Value, owner nodeKey) ([]nodeKey, error)

// towerRun owns the parts both write APIs share: the lock keyed on the tree
// being written, the replica handshake, and the retry after a conflict.
func towerRun(typ reflect.Type, id int, edit towerEdit) (bool, error) {
	if !relationGraphParticipant(typ) {
		return false, nil
	}

	if err := ensureCommittedOwnership(); err != nil {
		return true, err
	}

	root := nodeKey{typ: typ, id: id}
	if !committedOwnerHasChildren(root) {
		return false, nil
	}

	var held *sync.Mutex
	defer func() {
		if held != nil {
			held.Unlock()
		}
	}()

	for {
		// Re-read the top-level owner on every attempt: a reparent that lands
		// between two tries moves the root into a different tree, and the
		// callback has to queue behind that tree's writer instead.
		owner := committedOwnershipRoot(root)
		if lock := towerWriteLock(owner); lock != held {
			if held != nil {
				held.Unlock()
			}

			lock.Lock()
			held = lock
		}

		replica := projectTower.replica.Load()
		if !replica.synced() {
			fresh, err := projectTower.rebuild()
			if err != nil {
				return true, err
			}

			replica = fresh
		}

		settled, relationCommit, err := towerAttempt(replica, root, owner, edit)
		if relationCommit != nil {
			err = commitTowerRelationChanges(relationCommit)
			projectTower.retire(replica)
			if err == ErrConflict {
				continue
			}

			return true, err
		}

		if !settled {
			continue
		}

		return true, err
	}
}

// towerAttempt runs one shadow edit against a single reading of the graph. It
// reports whether the attempt settled — a false asks the caller to rebuild and
// retry — and hands back the resources of a relation edit, which cannot be
// published from here because engine.commit needs graphMu exclusively.
func towerAttempt(
	replica *towerReplica,
	root nodeKey,
	owner nodeKey,
	edit towerEdit,
) (bool, []touchedResource, error) {
	shadowValue := replica.nodes[root]
	if !shadowValue.IsValid() {
		return true, nil, ErrNotFound
	}

	declared, err := edit(shadowValue, owner)
	if err != nil {
		projectTower.retire(replica)
		return true, nil, err
	}

	// Shared, not exclusive: a scalar write to a graph participant is exactly
	// what engine.commit classifies as graphAccessRead, and the row itself is
	// covered by the resource locks commitTowerScalarChanges takes.
	graphMu.RLock()
	defer graphMu.RUnlock()

	if !replica.synced() {
		projectTower.retire(replica)
		return false, nil, nil
	}

	changes, err := replica.diffFor(root, declared)
	if err != nil {
		projectTower.retire(replica)
		return true, nil, err
	}

	if len(changes) == 0 {
		return true, nil, nil
	}

	resources, relationChanged, err := materializeTowerChanges(changes)
	if err != nil {
		projectTower.retire(replica)
		return true, nil, err
	}

	if relationChanged {
		return false, resources, nil
	}

	if err := commitTowerScalarChanges(resources); err != nil {
		projectTower.retire(replica)
		return true, nil, err
	}

	replica.captureEpochs()

	return true, nil, nil
}

// diffFor picks how much of the branch a write has to be checked against. A nil
// declaration means no promise was made, so every node is compared; otherwise
// only the root and the nodes Edit named are, which is what makes a tracked
// write cost what it changed rather than what it owns.
func (replica *towerReplica) diffFor(root nodeKey, declared []nodeKey) ([]towerChange, error) {
	if declared == nil {
		return replica.diffBranch(root)
	}

	changes, err := replica.diffNodes(append([]nodeKey{root}, declared...))
	if err != nil || !AuditTrackedWrites {
		return changes, err
	}

	return changes, replica.auditDeclaration(root, changes)
}

// auditDeclaration re-runs the full branch diff and refuses a write that
// changed a node Edit never named. It costs exactly what skipping the
// declaration would have, so it belongs in tests rather than in production.
func (replica *towerReplica) auditDeclaration(root nodeKey, declaredChanges []towerChange) error {
	full, err := replica.diffBranch(root)
	if err != nil {
		return err
	}

	if len(full) == len(declaredChanges) {
		return nil
	}

	seen := make(map[nodeKey]struct{}, len(declaredChanges))
	for _, change := range declaredChanges {
		seen[change.key] = struct{}{}
	}

	for _, change := range full {
		if _, declared := seen[change.key]; !declared {
			return fmt.Errorf("%w: %s", ErrUndeclaredWrite, change.key)
		}
	}

	return nil
}

func (replica *towerReplica) diffNodes(keys []nodeKey) ([]towerChange, error) {
	changes := make([]towerChange, 0, len(keys))
	seen := make(map[nodeKey]struct{}, len(keys))
	for _, key := range keys {
		if _, duplicate := seen[key]; duplicate {
			continue
		}

		seen[key] = struct{}{}
		node, err := replica.branchNode(key)
		if err != nil {
			return nil, err
		}

		plan, err := towerFields(key.typ)
		if err != nil {
			return nil, err
		}

		if fields := replica.diffFields(node, plan); len(fields) > 0 {
			changes = append(changes, towerChange{
				key: key, live: node.live, shadow: node.shadow, fields: fields,
			})
		}
	}

	return changes, nil
}

func (replica *towerReplica) branchNode(key nodeKey) (towerBranchNode, error) {
	shadow := replica.nodes[key]
	if !shadow.IsValid() {
		return towerBranchNode{}, fmt.Errorf("nestory: tower shadow node %s is missing", key)
	}

	live := replica.live[key]
	if !live.IsValid() {
		return towerBranchNode{}, ErrNotFound
	}

	return towerBranchNode{
		key: key, live: live, shadow: shadow,
		liveEntity: live.Interface(), shadowEntity: shadow.Interface(),
	}, nil
}

func (replica *towerReplica) diffBranch(root nodeKey) ([]towerChange, error) {
	branch, err := replica.branchFor(root)
	if err != nil {
		return nil, err
	}

	changes := make([]towerChange, 0, 1)
	var currentType reflect.Type
	var plan cachedTowerFieldPlan
	for _, node := range branch {
		if node.key.typ != currentType {
			currentType = node.key.typ
			plan, err = towerFields(currentType)
			if err != nil {
				return nil, err
			}
		}

		fields := replica.diffFields(node, plan)
		if len(fields) > 0 {
			changes = append(changes, towerChange{
				key: node.key, live: node.live, shadow: node.shadow, fields: fields,
			})
		}
	}

	return changes, nil
}

func (replica *towerReplica) branchFor(root nodeKey) ([]towerBranchNode, error) {
	replica.branchMu.Lock()
	cached, found := replica.branches[root]
	replica.branchMu.Unlock()
	if found {
		return cached, nil
	}

	keys := committedOwnershipKeys(root)
	branch := make([]towerBranchNode, len(keys))
	for index, key := range keys {
		node, err := replica.branchNode(key)
		if err != nil {
			return nil, err
		}

		branch[index] = node
	}

	replica.branchMu.Lock()
	replica.branches[root] = branch
	replica.branchMu.Unlock()

	return branch, nil
}

// towerNodeUnchanged compares a live node against its shadow, ignoring relation
// fields: those hold shadow pointers on one side and live pointers on the other,
// so they always differ bitwise and wireTowerNodes rebuilds them regardless.
func towerNodeUnchanged(live, shadow reflect.Value, plan cachedTowerFieldPlan) bool {
	for _, field := range plan.fields {
		if field.relational {
			continue
		}

		if !relationFieldEqual(live.Field(field.index), shadow.Field(field.index)) {
			return false
		}
	}

	return true
}

// relationsDiverged reports whether any live node's relation fields still match
// the state this replica captured at the last commit.
//
// Flush needs to know which nodes changed, and Unsafe cannot tell it: the
// contract is that the caller holds exclusive access until Flush completes, not
// that it re-acquires every pointer, so a caller may mutate through a pointer it
// kept since creation. Recording what Unsafe.Get hands out would miss exactly
// that. The shadow does not need to be told — it *is* the last committed state,
// so comparing against it derives the answer instead of trusting a declaration.
//
// It allocates nothing: the walk is map iteration plus the same comparisons the
// branch diff already uses.
func (replica *towerReplica) relationsDiverged() (bool, error) {
	var currentType reflect.Type
	var plan cachedTowerFieldPlan
	var err error

	for key, live := range replica.live {
		if key.typ != currentType {
			currentType = key.typ
			plan, err = towerFields(currentType)
			if err != nil {
				return false, err
			}
		}

		value := live.Elem()
		for _, field := range plan.fields {
			if !field.relational {
				continue
			}

			baseline := replica.relations[towerRelationKey{node: key, field: field.index}]
			if !replica.relationEqual(value.Field(field.index), field, baseline) {
				return true, nil
			}
		}
	}

	return false, nil
}

func (replica *towerReplica) diffFields(node towerBranchNode, plan cachedTowerFieldPlan) []int {
	if plan.equal != nil && plan.equal(node.liveEntity, node.shadowEntity) {
		return nil
	}

	key := node.key
	before := node.live.Elem()
	after := node.shadow.Elem()

	var changed []int
	for _, field := range plan.fields {
		left := before.Field(field.index)
		right := after.Field(field.index)
		var equal bool
		if field.relational {
			equal = replica.relationEqual(
				right,
				field,
				replica.relations[towerRelationKey{node: key, field: field.index}],
			)
		} else {
			equal = relationFieldEqual(left, right)
		}

		if !equal {
			changed = append(changed, field.index)
		}
	}

	return changed
}

func (replica *towerReplica) relationEqual(
	value reflect.Value,
	field towerFieldPlan,
	baseline towerRelationState,
) bool {
	if !field.many {
		id, present := valueID(value)
		return present == baseline.present && (!present || id == baseline.one)
	}

	comparator, found := towerSliceComparators.Load(value.Type())
	if !found || baseline.many == nil {
		// An unregistered slice type cannot be settled here; report a change and
		// let the relation commit path work it out.
		return false
	}

	return comparator.(towerSliceComparator)(value.Interface(), baseline.many)
}

// towerAlwaysComparable reports whether reflect.Value.Equal is safe on every
// value of typ, so diffFields can trust the cached plan instead of asking each
// value. reflect calls an interface type comparable even though the dynamic
// value inside it may be a slice, and Equal panics on exactly that case — so a
// type carrying an interface anywhere has to be diffed field by field.
func towerAlwaysComparable(typ reflect.Type) bool {
	if !typ.Comparable() {
		return false
	}

	switch typ.Kind() {
	case reflect.Interface:
		return false
	case reflect.Array:
		return towerAlwaysComparable(typ.Elem())
	case reflect.Struct:
		for index := range typ.NumField() {
			if !towerAlwaysComparable(typ.Field(index).Type) {
				return false
			}
		}
	}

	return true
}

func towerFields(typ reflect.Type) (cachedTowerFieldPlan, error) {
	if cached, found := towerFieldPlans.Load(typ); found {
		entry := cached.(cachedTowerFieldPlan)
		return entry, entry.err
	}

	specs, err := specsByField(typ)
	if err != nil {
		entry := cachedTowerFieldPlan{err: err}
		towerFieldPlans.Store(typ, entry)
		return entry, err
	}

	fields := make([]towerFieldPlan, typ.NumField())
	for index := range typ.NumField() {
		field := towerFieldPlan{index: index}
		if spec, relational := specs[typ.Field(index).Name]; relational {
			field.relational = true
			field.many = spec.many
		}
		fields[index] = field
	}

	// equal is only usable on the struct as a whole, so a type holding a
	// relation is excluded: those fields compare by id against the baseline,
	// not by the pointer the shadow happens to carry.
	entry := cachedTowerFieldPlan{fields: fields}
	if comparator, found := towerComparators.Load(typ); found && len(specs) == 0 {
		entry.equal = comparator.(towerComparator)
	}
	towerFieldPlans.Store(typ, entry)
	return entry, nil
}

func materializeTowerChanges(changes []towerChange) ([]touchedResource, bool, error) {
	resources := make([]touchedResource, 0, len(changes))
	for index := range changes {
		change := &changes[index]
		version, found := committerFor(change.key.typ.Name()).resourceVersion(change.key.id)
		if !found {
			return nil, false, ErrNotFound
		}

		original := cloneTowerPatchBase(change.live)
		work := cloneTowerPatchBase(change.live)
		for _, fieldIndex := range change.fields {
			copyTowerField(work.Elem().Field(fieldIndex), change.shadow.Elem().Field(fieldIndex))
		}

		change.resource = touchedResource{
			dbName:   change.key.typ.Name(),
			id:       change.key.id,
			ver:      version,
			work:     work.Interface(),
			original: original.Interface(),
		}
		resources = append(resources, change.resource)
	}

	relationChanged, err := resourcesChangeRelationGraph(resources)

	return resources, relationChanged, err
}

// cloneTowerPatchBase copies only the struct. Unchanged slice backing arrays
// remain shared with the immutable live state; copyTowerField gives every
// changed slice its own backing storage before publication.
func cloneTowerPatchBase(source reflect.Value) reflect.Value {
	clone := reflect.New(source.Elem().Type())
	clone.Elem().Set(source.Elem())
	return clone
}

func copyTowerField(target, source reflect.Value) {
	if source.Kind() != reflect.Slice || source.IsNil() {
		target.Set(source)
		return
	}

	clone := reflect.MakeSlice(source.Type(), source.Len(), source.Len())
	reflect.Copy(clone, source)
	target.Set(clone)
}

func commitTowerScalarChanges(resources []touchedResource) error {
	structuralDBNames := transactionStructuralDBNames(resources, nil, nil)
	for _, dbName := range structuralDBNames {
		committerFor(dbName).lockStructure()
	}

	defer func() {
		for index := len(structuralDBNames) - 1; index >= 0; index-- {
			committerFor(structuralDBNames[index]).unlockStructure()
		}
	}()

	lockedResources := transactionResourceLocks(resources, nil, nil)
	for _, resource := range lockedResources {
		committerFor(resource.dbName).lockResource(resource.id)
	}

	defer func() {
		for index := len(lockedResources) - 1; index >= 0; index-- {
			resource := lockedResources[index]
			committerFor(resource.dbName).unlockResource(resource.id)
		}
	}()

	for _, resource := range resources {
		version, found := committerFor(resource.dbName).resourceVersion(resource.id)
		if !found || version != resource.ver {
			return ErrConflict
		}
	}

	changes := transactionChanges(resources, nil, nil)
	if err := validateTransactionIndexes(changes); err != nil {
		return err
	}

	if err := logTransactionChanges(changes); err != nil {
		return err
	}

	prepareTransactionIndexes(changes)
	for _, resource := range resources {
		committerFor(resource.dbName).applyWrite(resource.id, resource.work)
	}

	finishTransactionIndexes(changes)

	return nil
}

func commitTowerRelationChanges(resources []touchedResource) error {
	state := engine.begin()
	for _, resource := range resources {
		engine.record(state, resource)
	}

	return engine.commit(state)
}

// captureEpochs absorbs the bumps the replica's own commit just produced. It
// runs after the write, unlike rebuild's snapshot, because here the shadow
// already holds exactly the values that were published.
func (replica *towerReplica) captureEpochs() {
	epochs := towerParticipantEpochs()
	replica.epochs.Store(&epochs)
}
