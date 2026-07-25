package nestory

import (
	"bytes"
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"
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
	// still fills in lazily. branchNodes counts the cached entries so the cache
	// stays bounded — it used to grow by every root ever written, for the
	// replica's whole life.
	branchMu    sync.Mutex
	branches    map[nodeKey][]towerBranchNode
	branchNodes int
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
	// slice marks a non-relational slice field. Its shadow clone owns a fresh
	// backing array, so its header always differs bitwise and it must stay out
	// of the memory segments below.
	slice bool
	// targetIDIndex is the field index of Id inside the relation target, so an
	// identity comparison reads the int directly instead of paying valueID's
	// Interface round trip per element — a fifth of the flush gate.
	targetIDIndex int
}

func (field towerFieldPlan) segmentEligible() bool {
	return !field.relational && !field.slice
}

// entityIDFieldIndex caches where Id sits inside a type, so identity checks can
// read it as an int field instead of boxing the value into an interface.
var entityIDIndexes sync.Map

func entityIDFieldIndex(typ reflect.Type) int {
	if cached, found := entityIDIndexes.Load(typ); found {
		return cached.(int)
	}

	index := -1
	if field, found := typ.FieldByName(entityIDField); found && len(field.Index) == 1 && field.Type.Kind() == reflect.Int {
		index = field.Index[0]
	}

	entityIDIndexes.Store(typ, index)

	return index
}

// fieldPointerID reads a relation pointer's target id through the cached field
// index. It falls back to valueID for the rare shape the cache cannot serve.
func fieldPointerID(value reflect.Value, idIndex int) (int, bool) {
	if idIndex < 0 {
		return valueID(value)
	}

	if value.Kind() != reflect.Pointer || value.IsNil() {
		return 0, false
	}

	return int(value.Elem().Field(idIndex).Int()), true
}

// towerSegmentsEqual compares the scalar runs of two nodes byte for byte. Both
// values are addressable structs — live rows sit in chunk blocks and shadows in
// replica blocks — so their base pointers are stable for the duration.
func towerSegmentsEqual(live, shadow reflect.Value, segments []towerSegment) bool {
	if len(segments) == 0 {
		return true
	}

	liveBase := live.Addr().UnsafePointer()
	shadowBase := shadow.Addr().UnsafePointer()
	for _, segment := range segments {
		liveBytes := unsafe.Slice((*byte)(unsafe.Add(liveBase, segment.offset)), segment.size)
		shadowBytes := unsafe.Slice((*byte)(unsafe.Add(shadowBase, segment.offset)), segment.size)
		if !bytes.Equal(liveBytes, shadowBytes) {
			return false
		}
	}

	return true
}

// towerSegment is one contiguous byte range of scalar fields. Bitwise equality
// implies semantic equality for every field kind (equal headers mean shared
// backing), so a segment hit settles the whole run in one comparison; the
// reverse is not true, and a miss falls back to field-by-field. Padding inside
// a run only ever produces that safe false positive.
type towerSegment struct {
	offset uintptr
	size   uintptr
}

type cachedTowerFieldPlan struct {
	fields   []towerFieldPlan
	segments []towerSegment
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
	if current == nil || len(index.nodes) < len(current.live) {
		return false, nil
	}

	// Committed keys the replica has never seen are additions; they get fresh
	// storage below, so existing shadow addresses never move. A live pointer
	// that changed identity, or a key the replica has that the index lost, is a
	// replacement or a removal — those still take the full rebuild.
	var added []nodeKey
	for key, value := range index.nodes {
		live, ok := current.live[key]
		if !ok {
			added = append(added, key)
			continue
		}

		if live.Pointer() != value.Pointer() {
			return false, nil
		}
	}

	if len(index.nodes)-len(added) != len(current.live) {
		return false, nil
	}

	// New nodes go into one fresh block per table — never into an existing
	// block, whose never-grow guarantee is what keeps every published shadow
	// address stable. Appending a block moves no element of any other block.
	if len(added) > 0 {
		grown, blocks := buildTowerShadows(addedTowerNodes(index, added))
		current.blocks = append(current.blocks, blocks...)
		for key, shadow := range grown {
			current.nodes[key] = shadow
			current.live[key] = index.nodes[key]
		}
	}

	// Copy only the nodes that actually differ. Comparison is free and reads
	// nothing off-heap; the copy is not, because cloneSliceFields gives every
	// slice field its own backing array. Re-copying an unchanged node allocates
	// one array per slice field to reproduce bytes that already match.
	var currentType reflect.Type
	var plan cachedTowerFieldPlan
	var err error

	moved := make([]nodeKey, 0, 8)
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
		moved = append(moved, key)
	}

	// An added node compares as unchanged — its shadow was copied from live a
	// moment ago — but it still needs wiring into the shadow world and a
	// relation baseline, so it joins the moved set here.
	moved = append(moved, added...)

	// A node nobody touched keeps its wiring and its relation baseline: refresh
	// reuses the shadow addresses, so a pointer into an untouched node is still
	// the right pointer. Rebuilding both for the whole project allocated a
	// baseline slice per owner and a map entry per node, every time.
	for _, key := range moved {
		if err := wireTowerNode(current.nodes, key, current.nodes[key]); err != nil {
			return false, err
		}

		if err := snapshotTowerRelationsInto(current.relations, key, current.nodes[key]); err != nil {
			return false, err
		}
	}
	current.index = index

	// Ownership may have moved between nodes that all still exist, so the cached
	// branches are the one derived structure a refresh cannot keep.
	current.branchMu.Lock()
	current.branches = make(map[nodeKey][]towerBranchNode)
	current.branchNodes = 0
	current.branchMu.Unlock()

	current.epochs.Store(&epochs)

	return true, nil
}

// towerForgetNodes drops deleted nodes from the live replica so the following
// refresh sees matching node sets. Shadow storage is not reclaimed — the block
// slots stay allocated until the next full rebuild — because compacting a block
// would move surviving shadows, and their addresses are what the whole design
// promises never to move. The caller holds graphMu.Lock.
func towerForgetNodes(closure map[nodeKey]struct{}) {
	replica := projectTower.replica.Load()
	if replica == nil || len(closure) == 0 {
		return
	}

	for key := range closure {
		delete(replica.live, key)
		delete(replica.nodes, key)
	}

	for relation := range replica.relations {
		if _, dies := closure[relation.node]; dies {
			delete(replica.relations, relation)
		}
	}

	replica.branchMu.Lock()
	replica.branches = make(map[nodeKey][]towerBranchNode)
	replica.branchNodes = 0
	replica.branchMu.Unlock()
}

// refreshAfterCommit re-points a live replica at the index a commit just
// published, so the next Flush has a baseline to compare against.
//
// It must run after the commit, never before a comparison: a replica built from
// the current live graph is by construction equal to it, so comparing against
// one would report "nothing moved" even when the graph had. Here live and
// committed agree, which is what makes the baseline meaningful.
//
// It never builds a replica and never retires one. A commit that changed the
// node set simply leaves the replica as it was: its epochs are already stale, so
// synced() sends the next writer to a rebuild, and the flush gate compares
// index identity, which no longer matches. Retiring here would additionally
// throw away a replica that a write to an unrelated table had not invalidated.
//
// The caller holds graphMu.Lock.
func (tower *Tower) refreshAfterCommit() {
	current := tower.replica.Load()
	if current == nil {
		return
	}

	index := committedRelationIndexSnapshot()
	if index == nil {
		return
	}

	_, _ = tower.refresh(current, index, towerParticipantEpochs())
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
func addedTowerNodes(index *committedRelationIndex, added []nodeKey) map[nodeKey]reflect.Value {
	nodes := make(map[nodeKey]reflect.Value, len(added))
	for _, key := range added {
		nodes[key] = index.nodes[key]
	}

	return nodes
}

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
	relations := make(map[towerRelationKey]towerRelationState, len(nodes))
	for key, node := range nodes {
		if err := snapshotTowerRelationsInto(relations, key, node); err != nil {
			return nil, err
		}
	}

	return relations, nil
}

func snapshotTowerRelationsInto(
	relations map[towerRelationKey]towerRelationState,
	key nodeKey,
	node reflect.Value,
) error {
	specs, err := relationSpecs(key.typ)
	if err != nil {
		return err
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

	return nil
}

func wireTowerNodes(nodes map[nodeKey]reflect.Value) error {
	for key, node := range nodes {
		if err := wireTowerNode(nodes, key, node); err != nil {
			return err
		}
	}

	return nil
}

func wireTowerNode(nodes map[nodeKey]reflect.Value, key nodeKey, node reflect.Value) error {
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
	// Crude but self-healing: past the cap, drop everything and let the hot
	// roots refill. An entry is 104 B, so the cap holds the cache near 6 MiB
	// instead of letting a root-rotating workload grow it without limit.
	if replica.branchNodes+len(branch) > towerBranchCacheLimit {
		replica.branches = make(map[nodeKey][]towerBranchNode)
		replica.branchNodes = 0
	}

	replica.branches[root] = branch
	replica.branchNodes += len(branch)
	replica.branchMu.Unlock()

	return branch, nil
}

// towerBranchCacheLimit bounds the branch cache by cached nodes, not roots:
// one huge branch and many small ones cost the same memory per node.
const towerBranchCacheLimit = 64 * 1024

// towerNodeUnchanged compares a live node against its shadow. Relation fields
// compare by id, not bitwise: they hold shadow pointers on one side and live
// pointers on the other, so a pointer comparison always differs — but skipping
// them entirely once left a reordered own slice stale in the shadow, because a
// membership change is carried by the copy, not by the rewiring. Identity equal
// means the shadow already points at shadow copies of the right ids.
func towerNodeUnchanged(live, shadow reflect.Value, plan cachedTowerFieldPlan) bool {
	if !towerSegmentsEqual(live, shadow, plan.segments) {
		return false
	}

	for _, field := range plan.fields {
		if field.relational {
			if !sameRelationIdentity(live.Field(field.index), shadow.Field(field.index), field.many, field.targetIDIndex) {
				return false
			}

			continue
		}

		if field.slice && !relationFieldEqual(live.Field(field.index), shadow.Field(field.index)) {
			return false
		}
	}

	return true
}

// graphMovedNodes reports which committed nodes no longer match the shadow in
// any way the relation index can see: a relation field compared by id, or a
// lookup-key field compared by value. keyChanged is set when a lookup-key field
// moved — the committed target index maps that field's old value to the node,
// so only the full rebuild can fix it; the delta path must refuse.
//
// Flush needs this because Unsafe cannot tell it what changed: the contract is
// exclusive access until Flush completes, not re-acquiring every pointer, so a
// caller may mutate through a pointer it kept since creation. The shadow *is*
// the last committed state, so comparing derives the change set instead of
// trusting a declaration. Scalar fields outside the lookup keys are ignored:
// the index does not hold them, so they cannot stale it.
func (replica *towerReplica) graphMovedNodes() (moved []nodeKey, keyChanged bool, err error) {
	var currentType reflect.Type
	var plan cachedTowerFieldPlan
	var keyFields []int

	for key, live := range replica.live {
		if key.typ != currentType {
			currentType = key.typ
			plan, err = towerFields(currentType)
			if err != nil {
				return nil, false, err
			}

			keyFields = keyFields[:0]
			names := replica.index.targetFields[currentType]
			for _, field := range plan.fields {
				if field.relational {
					continue
				}

				if _, lookup := names[currentType.Field(field.index).Name]; lookup {
					keyFields = append(keyFields, field.index)
				}
			}
		}

		shadow, present := replica.nodes[key]
		if !present {
			moved = append(moved, key)
			continue
		}

		liveValue, shadowValue := live.Elem(), shadow.Elem()
		changed := false
		for _, field := range plan.fields {
			if !field.relational {
				continue
			}

			if !sameRelationIdentity(liveValue.Field(field.index), shadowValue.Field(field.index), field.many, field.targetIDIndex) {
				changed = true
				break
			}
		}

		// Key fields are typically one int, so per-field comparison beats a
		// segment pass here — measured, not assumed: the segment variant cost
		// ~9% of the whole gate on this fixture.
		for _, index := range keyFields {
			if !relationFieldEqual(liveValue.Field(index), shadowValue.Field(index)) {
				changed = true
				keyChanged = true
				break
			}
		}

		if changed {
			moved = append(moved, key)
		}
	}

	return moved, keyChanged, nil
}

// sameRelationIdentity compares two relation fields by the ids they point at.
func sameRelationIdentity(live, shadow reflect.Value, many bool, idIndex int) bool {
	if !many {
		liveID, livePresent := fieldPointerID(live, idIndex)
		shadowID, shadowPresent := fieldPointerID(shadow, idIndex)

		return livePresent == shadowPresent && (!livePresent || liveID == shadowID)
	}

	if live.Len() != shadow.Len() {
		return false
	}

	for index := range live.Len() {
		liveID, livePresent := fieldPointerID(live.Index(index), idIndex)
		shadowID, shadowPresent := fieldPointerID(shadow.Index(index), idIndex)
		if livePresent != shadowPresent || (livePresent && liveID != shadowID) {
			return false
		}
	}

	return true
}

func (replica *towerReplica) diffFields(node towerBranchNode, plan cachedTowerFieldPlan) []int {
	if plan.equal != nil && plan.equal(node.liveEntity, node.shadowEntity) {
		return nil
	}

	key := node.key
	before := node.live.Elem()
	after := node.shadow.Elem()

	// One comparison per scalar run settles the common case; a miss only means
	// the per-field loop below has to name which fields moved.
	scalarsEqual := towerSegmentsEqual(before, after, plan.segments)

	var changed []int
	for _, field := range plan.fields {
		var equal bool
		switch {
		case field.relational:
			equal = replica.relationEqual(
				after.Field(field.index),
				field,
				replica.relations[towerRelationKey{node: key, field: field.index}],
			)
		case field.slice:
			equal = relationFieldEqual(before.Field(field.index), after.Field(field.index))
		case scalarsEqual:
			continue
		default:
			equal = relationFieldEqual(before.Field(field.index), after.Field(field.index))
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
		id, present := fieldPointerID(value, field.targetIDIndex)
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
	var segments []towerSegment
	for index := range typ.NumField() {
		structField := typ.Field(index)
		field := towerFieldPlan{index: index, targetIDIndex: -1}
		if spec, relational := specs[structField.Name]; relational {
			field.relational = true
			field.many = spec.many
			field.targetIDIndex = entityIDFieldIndex(spec.target)
		} else if structField.Type.Kind() == reflect.Slice {
			field.slice = true
		} else {
			// Extend the current segment or open a new one. Spanning the padding
			// between two scalar fields is deliberate: comparing it can only
			// produce a safe false positive.
			end := structField.Offset + structField.Type.Size()
			if index > 0 && len(segments) > 0 && fields[index-1].segmentEligible() {
				segments[len(segments)-1].size = end - segments[len(segments)-1].offset
			} else {
				segments = append(segments, towerSegment{offset: structField.Offset, size: end - structField.Offset})
			}
		}

		fields[index] = field
	}

	// equal is only usable on the struct as a whole, so a type holding a
	// relation is excluded: those fields compare by id against the baseline,
	// not by the pointer the shadow happens to carry.
	entry := cachedTowerFieldPlan{fields: fields, segments: segments}
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
