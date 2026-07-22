package nestory

import (
	"cmp"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
)

// ErrConflict: someone committed one of our resources after we snapshotted.
// The snapshot is refreshed in place so a retry runs against fresh data.
var ErrConflict = errors.New("nestory: transaction conflict")

// ErrExpiredSnapshot: the snapshot's contract is gone (committed, discarded,
// or never came from Get).
var ErrExpiredSnapshot = errors.New("nestory: expired snapshot")

var ErrNotFound = errors.New("nestory: entity not found")

// touchedResource is one resource a transaction touched — pure, type-erased
// data. The live resourceSlot and the typed write-back resolve at commit via
// baseRegistry[typ].
type touchedResource struct {
	dbName   string
	id       int
	ver      int // version observed at snapshot time
	work     any // *T, the client's detached copy
	original any // *T, state at the start of the branch
}

// committer is the type-erased view the transactionEngine drives at commit. *DB[T]
// implements it. resourceVersion/applyWrite/refreshSnapshot assume the per-row
// lock is already held — the transactionEngine owns locking so it can enforce a global order.
type committer interface {
	lockResource(id int)
	unlockResource(id int)
	lockStructure()
	unlockStructure()
	readLockResource(id int)
	readUnlockResource(id int)
	snapshotResource(id int) (work, original reflect.Value, version int, found bool)
	resourceVersion(id int) (int, bool)
	applyWrite(id int, work any)
	refreshSnapshot(id int, work any)
	hasSecondaryIndexes() bool
	validateIndexes(items []pendingWrite) error
	prepareIndexes(items []pendingWrite)
	finishIndexes(items []pendingWrite)
	encodeWrites(items []pendingWrite) ([]walRow, error)
	logWrites(items []pendingWrite) error
}

type pendingWrite struct {
	id      int
	work    any
	deleted bool
}

type createdResource struct {
	key  nodeKey
	work reflect.Value // *struct
}

type stagedDelete struct {
	key nodeKey
	ver int
}

type transactionState struct {
	active      bool
	hasResource bool
	resource    touchedResource
	resources   []touchedResource
	resourcePos map[transactionResourceKey]int
	creates     map[nodeKey]createdResource
	deletes     map[nodeKey]stagedDelete
}

type transactionResourceKey struct {
	dbName string
	id     int
}

type graphAccess uint8

const (
	graphAccessNone graphAccess = iota
	graphAccessRead
	graphAccessWrite
)

func (tx *transactionState) copyResources() []touchedResource {
	if !tx.hasResource {
		return nil
	}

	resources := make([]touchedResource, 1+len(tx.resources))
	resources[0] = tx.resource
	copy(resources[1:], tx.resources)

	return resources
}

func committerFor(dbName string) committer {
	return baseRegistry[dbName].(committer)
}

// transactionEngine sits above every DB, owns the live contracts and serialises commits.
// It stays generic — reaches a type's resources only through baseRegistry.
type transactionEngine struct {
	mu   sync.Mutex
	bind map[any]*transactionState // snapshot *T → owning transaction
}

func newEngine() *transactionEngine {
	return &transactionEngine{
		bind: make(map[any]*transactionState),
	}
}

var engine = newEngine()

func (en *transactionEngine) begin() *transactionState {
	return &transactionState{active: true}
}

func (en *transactionEngine) created(tx *transactionState, key nodeKey) (reflect.Value, bool) {
	en.mu.Lock()
	defer en.mu.Unlock()

	created, ok := tx.creates[key]
	return created.work, ok
}

func (en *transactionEngine) stageCreate(tx *transactionState, created createdResource) error {
	en.mu.Lock()
	defer en.mu.Unlock()

	if !tx.active {
		return ErrExpiredSnapshot
	}

	if _, duplicate := tx.creates[created.key]; duplicate {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, created.key)
	}

	if tx.creates == nil {
		tx.creates = make(map[nodeKey]createdResource)
	}

	tx.creates[created.key] = created
	return nil
}

func (en *transactionEngine) stageDelete(tx *transactionState, deleted stagedDelete) error {
	en.mu.Lock()
	defer en.mu.Unlock()

	if !tx.active {
		return ErrExpiredSnapshot
	}

	if tx.deletes == nil {
		tx.deletes = make(map[nodeKey]stagedDelete)
	}

	tx.deletes[deleted.key] = deleted
	return nil
}

func (en *transactionEngine) record(tx *transactionState, e touchedResource) {
	en.mu.Lock()
	defer en.mu.Unlock()

	key := transactionResourceKey{dbName: e.dbName, id: e.id}
	if tx.resourcePos != nil {
		if _, exists := tx.resourcePos[key]; exists {
			return
		}
	} else {
		if tx.hasResource && tx.resource.dbName == e.dbName && tx.resource.id == e.id {
			return
		}

		for _, resource := range tx.resources {
			if resource.dbName == e.dbName && resource.id == e.id {
				return
			}
		}
	}

	if !tx.hasResource {
		tx.hasResource = true
		tx.resource = e
		en.bind[e.work] = tx
		return
	}

	tx.resources = append(tx.resources, e)
	if len(tx.resources) == 8 {
		tx.resourcePos = make(map[transactionResourceKey]int, 1+len(tx.resources))
		tx.resourcePos[transactionResourceKey{dbName: tx.resource.dbName, id: tx.resource.id}] = 0
		for index, resource := range tx.resources {
			tx.resourcePos[transactionResourceKey{dbName: resource.dbName, id: resource.id}] = index + 1
		}
	} else if tx.resourcePos != nil {
		tx.resourcePos[key] = len(tx.resources)
	}

	en.bind[e.work] = tx
}

func (en *transactionEngine) work(tx *transactionState, dbName string, id int) (any, bool) {
	en.mu.Lock()
	defer en.mu.Unlock()

	if tx.resourcePos != nil {
		position, found := tx.resourcePos[transactionResourceKey{dbName: dbName, id: id}]
		if !found {
			return nil, false
		}

		if position == 0 {
			return tx.resource.work, true
		}

		return tx.resources[position-1].work, true
	}

	if tx.hasResource && tx.resource.dbName == dbName && tx.resource.id == id {
		return tx.resource.work, true
	}

	for _, resource := range tx.resources {
		if resource.dbName == dbName && resource.id == id {
			return resource.work, true
		}
	}

	return nil, false
}

func (en *transactionEngine) commitByPtr(work any) error {
	en.mu.Lock()
	tx, ok := en.bind[work]
	en.mu.Unlock()

	if !ok {
		return ErrExpiredSnapshot
	}

	return en.commit(tx)
}

func (en *transactionEngine) commit(tx *transactionState) error {
	en.mu.Lock()
	active := tx.active
	transactionResources := tx.copyResources()
	var createdResources map[nodeKey]createdResource
	if len(tx.creates) > 0 {
		createdResources = make(map[nodeKey]createdResource, len(tx.creates))
		for key, created := range tx.creates {
			createdResources[key] = created
		}
	}

	var stagedDeletes map[nodeKey]stagedDelete
	if len(tx.deletes) > 0 {
		stagedDeletes = make(map[nodeKey]stagedDelete, len(tx.deletes))
		for key, deleted := range tx.deletes {
			stagedDeletes[key] = deleted
		}
	}

	en.mu.Unlock()

	if !active {
		return ErrExpiredSnapshot
	}

	touchedResources := changedResources(transactionResources)
	if len(touchedResources) == 0 && len(createdResources) == 0 && len(stagedDeletes) == 0 {
		en.evict(tx)
		return nil
	}

	structural := len(createdResources) > 0 || len(stagedDeletes) > 0
	relationChanged, err := resourcesChangeRelationGraph(touchedResources)
	if err != nil {
		return err
	}

	access := transactionGraphAccess(touchedResources, createdResources, stagedDeletes, relationChanged)
	graphChanged := access == graphAccessWrite
	switch access {
	case graphAccessWrite:
		graphMu.Lock()
		defer graphMu.Unlock()
	case graphAccessRead:
		graphMu.RLock()
		defer graphMu.RUnlock()
	}

	var model *relationModel
	var indexDelta *relationIndexDelta
	var deleted map[nodeKey]struct{}
	if graphChanged {
		switch {
		case len(stagedDeletes) == 0 && len(createdResources) > 0:
			indexDelta, err = buildRelationCreateIndexDelta(touchedResources, createdResources)
		case len(stagedDeletes) == 0:
			indexDelta, err = buildRelationIndexDelta(touchedResources)
		}

		if err != nil {
			return err
		}

		if indexDelta == nil {
			model, deleted, err = validateTransactionGraph(touchedResources, createdResources, stagedDeletes)
			if err != nil {
				return err
			}
		}

		if indexDelta != nil {
			touchedResources, err = indexDelta.materializeOwnBackReferences(transactionResources, touchedResources, createdResources)
		} else {
			touchedResources, err = materializeOwnBackReferences(model, transactionResources, touchedResources, createdResources, deleted)
		}

		if err != nil {
			return err
		}
	}

	if !structural && !graphChanged && len(touchedResources) == 1 &&
		!committerFor(touchedResources[0].dbName).hasSecondaryIndexes() {
		return en.commitSingleWrite(tx, touchedResources[0])
	}

	structuralDBNames := transactionStructuralDBNames(touchedResources, createdResources, stagedDeletes)
	for _, dbName := range structuralDBNames {
		committerFor(dbName).lockStructure()
	}

	defer func() {
		for index := len(structuralDBNames) - 1; index >= 0; index-- {
			committerFor(structuralDBNames[index]).unlockStructure()
		}
	}()

	for key := range createdResources {
		if _, exists := committerFor(key.typ.Name()).resourceVersion(key.id); exists {
			return fmt.Errorf("%w: %s", ErrAlreadyExists, key)
		}
	}

	lockedResources := transactionResourceLocks(touchedResources, createdResources, stagedDeletes)
	for _, resource := range lockedResources {
		committerFor(resource.dbName).lockResource(resource.id)
	}

	defer func() {
		for index := len(lockedResources) - 1; index >= 0; index-- {
			resource := lockedResources[index]
			committerFor(resource.dbName).unlockResource(resource.id)
		}
	}()

	for _, e := range touchedResources {
		v, ok := committerFor(e.dbName).resourceVersion(e.id)
		if !ok || v != e.ver {
			en.refresh(tx)

			return ErrConflict
		}
	}

	for key, deletedResource := range stagedDeletes {
		if _, created := createdResources[key]; created {
			continue
		}

		version, ok := committerFor(key.typ.Name()).resourceVersion(key.id)
		if !ok || version != deletedResource.ver {
			return ErrConflict
		}
	}

	durableDeletes := deleted
	if durableDeletes == nil && len(stagedDeletes) > 0 {
		durableDeletes = make(map[nodeKey]struct{}, len(stagedDeletes))
		for key := range stagedDeletes {
			durableDeletes[key] = struct{}{}
		}
	}

	walBackedStructural := structural && (!graphChanged || len(stagedDeletes) == 0 &&
		(indexDelta != nil || prepareStructuralCreateWAL(model, touchedResources, createdResources)))
	changes := transactionChanges(touchedResources, createdResources, durableDeletes)
	if err := validateTransactionIndexes(changes); err != nil {
		return err
	}

	if err := logTransactionChanges(changes); err != nil {
		return err
	}

	prepareTransactionIndexes(changes)

	for _, e := range touchedResources {
		committerFor(e.dbName).applyWrite(e.id, e.work)
	}

	for _, created := range createdResources {
		runtime := baseRegistry[created.key.typ.Name()].(relationRuntime)
		runtime.relationApplyCreate(created.work)
		if indexDelta != nil {
			live, found := runtime.relationValue(created.key.id)
			if !found {
				panic("nestory: created relation node is not live")
			}

			indexDelta.bindCreatedNode(created.key, live)
		}
	}

	if !graphChanged {
		applyDeletedNodes(durableDeletes)
		finishTransactionIndexes(changes)
		en.evict(tx)

		return nil
	}

	if indexDelta != nil {
		finishTransactionIndexes(changes)
		indexDelta.publishAndRewire()
		en.evict(tx)

		return nil
	}

	if err := bindRelationModelToLiveNodes(model, touchedResources, createdResources); err != nil {
		return err
	}

	reconcileRelations(model, deleted)

	applyDeletedNodes(deleted)
	rewireRelations(relationRuntimes())
	finishTransactionIndexes(changes)

	if structural && !walBackedStructural {
		for _, runtime := range relationRuntimes() {
			if err := runtime.relationSave(); err != nil {
				return err
			}
		}

		if err := sharedTransactionWAL().truncate(); err != nil {
			return err
		}
	}

	storeCommittedOwnership(model, deleted)

	en.evict(tx)

	return nil
}

type transactionResourceLock struct {
	dbName string
	id     int
}

func transactionGraphAccess(
	resources []touchedResource,
	creates map[nodeKey]createdResource,
	deletes map[nodeKey]stagedDelete,
	relationChanged bool,
) graphAccess {
	if relationChanged {
		return graphAccessWrite
	}

	for key := range creates {
		if relationGraphParticipant(key.typ) {
			return graphAccessWrite
		}
	}

	for key := range deletes {
		if relationGraphParticipant(key.typ) {
			return graphAccessWrite
		}
	}

	for _, resource := range resources {
		if relationGraphParticipant(baseRegistry[resource.dbName].(relationRuntime).relationType()) {
			return graphAccessRead
		}
	}

	return graphAccessNone
}

func transactionStructuralDBNames(
	resources []touchedResource,
	creates map[nodeKey]createdResource,
	deletes map[nodeKey]stagedDelete,
) []string {
	names := make(map[string]struct{}, len(creates)+len(deletes))
	for _, resource := range resources {
		if committerFor(resource.dbName).hasSecondaryIndexes() {
			names[resource.dbName] = struct{}{}
		}
	}

	for key := range creates {
		names[key.typ.Name()] = struct{}{}
	}

	for key := range deletes {
		names[key.typ.Name()] = struct{}{}
	}

	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}

	sort.Strings(ordered)

	return ordered
}

func transactionResourceLocks(
	resources []touchedResource,
	creates map[nodeKey]createdResource,
	deletes map[nodeKey]stagedDelete,
) []transactionResourceLock {
	locks := make([]transactionResourceLock, 0, len(resources)+len(deletes))
	seen := make(map[transactionResourceKey]struct{}, len(resources)+len(deletes))
	for _, resource := range resources {
		key := transactionResourceKey{dbName: resource.dbName, id: resource.id}
		seen[key] = struct{}{}
		locks = append(locks, transactionResourceLock(key))
	}

	for key := range deletes {
		if _, created := creates[key]; created {
			continue
		}

		resource := transactionResourceKey{dbName: key.typ.Name(), id: key.id}
		if _, exists := seen[resource]; exists {
			continue
		}

		seen[resource] = struct{}{}
		locks = append(locks, transactionResourceLock(resource))
	}

	// Global (dbName,id) order prevents overlapping commits from forming a cycle.
	slices.SortFunc(locks, func(a, b transactionResourceLock) int {
		if byName := cmp.Compare(a.dbName, b.dbName); byName != 0 {
			return byName
		}

		return cmp.Compare(a.id, b.id)
	})

	return locks
}

func materializeOwnBackReferences(
	model *relationModel,
	transactionResources []touchedResource,
	changed []touchedResource,
	creates map[nodeKey]createdResource,
	deleted map[nodeKey]struct{},
) ([]touchedResource, error) {
	available := make(map[nodeKey]touchedResource, len(transactionResources))
	changedKeys := make(map[nodeKey]struct{}, len(changed))
	for _, resource := range transactionResources {
		runtime := baseRegistry[resource.dbName].(relationRuntime)
		available[nodeKey{typ: runtime.relationType(), id: resource.id}] = resource
	}

	for _, resource := range changed {
		runtime := baseRegistry[resource.dbName].(relationRuntime)
		changedKeys[nodeKey{typ: runtime.relationType(), id: resource.id}] = struct{}{}
	}

	for _, ref := range model.refs {
		if ref.spec.kind != ownRelation || !ref.spec.many || ownSliceBackReferencePersists(model, ref) {
			continue
		}

		if _, dies := deleted[ref.target]; dies {
			continue
		}

		if created, exists := creates[ref.target]; exists {
			model.nodes[ref.target] = relationGraphNode{key: ref.target, value: created.work}
			syncOwnSliceBackReference(model, ref)
			continue
		}

		if _, exists := changedKeys[ref.target]; exists {
			syncOwnSliceBackReference(model, ref)
			continue
		}

		resource, found := available[ref.target]
		if !found {
			var err error
			resource, err = snapshotTouchedResource(ref.target)
			if err != nil {
				return nil, err
			}
		}

		model.nodes[ref.target] = relationGraphNode{key: ref.target, value: reflect.ValueOf(resource.work)}
		if syncOwnSliceBackReference(model, ref) {
			changed = append(changed, resource)
			changedKeys[ref.target] = struct{}{}
		}
	}

	return changed, nil
}

func snapshotTouchedResource(key nodeKey) (touchedResource, error) {
	committer := committerFor(key.typ.Name())
	committer.lockResource(key.id)
	work, original, version, found := committer.snapshotResource(key.id)
	committer.unlockResource(key.id)
	if !found {
		return touchedResource{}, ErrNotFound
	}

	return touchedResource{
		dbName: key.typ.Name(), id: key.id, ver: version,
		work: work.Interface(), original: original.Interface(),
	}, nil
}

func bindRelationModelToLiveNodes(model *relationModel, resources []touchedResource, creates map[nodeKey]createdResource) error {
	for _, resource := range resources {
		runtime, ok := baseRegistry[resource.dbName].(relationRuntime)
		if !ok {
			continue
		}

		key := nodeKey{typ: runtime.relationType(), id: resource.id}
		value, found := runtime.relationValue(resource.id)
		if !found {
			return fmt.Errorf("%w: committed node %s is missing", ErrRelationInvariant, key)
		}

		model.nodes[key] = relationGraphNode{key: key, value: value}
	}

	for key := range creates {
		runtime := baseRegistry[key.typ.Name()].(relationRuntime)
		value, found := runtime.relationValue(key.id)
		if !found {
			return fmt.Errorf("%w: created node %s is missing", ErrRelationInvariant, key)
		}

		model.nodes[key] = relationGraphNode{key: key, value: value}
	}

	return nil
}

func (en *transactionEngine) commitSingleWrite(tx *transactionState, resource touchedResource) error {
	committer := committerFor(resource.dbName)
	committer.lockResource(resource.id)
	defer committer.unlockResource(resource.id)

	version, found := committer.resourceVersion(resource.id)
	if !found || version != resource.ver {
		en.refresh(tx)

		return ErrConflict
	}

	write := [1]pendingWrite{{id: resource.id, work: resource.work}}
	if err := committer.logWrites(write[:]); err != nil {
		return err
	}

	committer.applyWrite(resource.id, resource.work)
	en.evict(tx)

	return nil
}

func resourcesChangeRelationGraph(resources []touchedResource) (bool, error) {
	hasRelations, err := registryHasRelations()
	if err != nil {
		return false, err
	}

	var matchFields map[reflect.Type]map[int]struct{}
	for _, resource := range resources {
		before := reflect.ValueOf(resource.original).Elem()
		after := reflect.ValueOf(resource.work).Elem()
		if resource.original.(Entity).GetId() != resource.work.(Entity).GetId() {
			return false, fmt.Errorf("%w: primary key of %s(%d) changed", ErrRelationInvariant, before.Type(), resource.id)
		}

		if !hasRelations {
			continue
		}

		specs, err := relationSpecs(before.Type())
		if err != nil {
			return false, err
		}

		for _, spec := range specs {
			if !relationValueEqual(before.Field(spec.fieldIndex), after.Field(spec.fieldIndex), spec.many) {
				return true, nil
			}
		}

		if matchFields == nil {
			matchFields, err = registeredRelationMatchFields()
			if err != nil {
				return false, err
			}
		}

		for fieldIndex := range matchFields[before.Type()] {
			if !relationFieldEqual(before.Field(fieldIndex), after.Field(fieldIndex)) {
				return true, nil
			}
		}
	}

	return false, nil
}

func registeredRelationMatchFields() (map[reflect.Type]map[int]struct{}, error) {
	fields := make(map[reflect.Type]map[int]struct{})
	for _, runtime := range relationRuntimes() {
		typ := runtime.relationType()
		idField, found := typ.FieldByName(entityIDField)
		if !found {
			return nil, fmt.Errorf("%w: %s.%s does not exist", ErrRelationSchema, typ, entityIDField)
		}

		if fields[typ] == nil {
			fields[typ] = make(map[int]struct{})
		}

		fields[typ][idField.Index[0]] = struct{}{}

		specs, err := relationSpecs(typ)
		if err != nil {
			return nil, err
		}

		for _, spec := range specs {
			field, found := spec.target.FieldByName(spec.matchField)
			if !found {
				return nil, fmt.Errorf("%w: %s.%s does not exist", ErrRelationSchema, spec.target, spec.matchField)
			}

			if fields[spec.target] == nil {
				fields[spec.target] = make(map[int]struct{})
			}

			fields[spec.target][field.Index[0]] = struct{}{}
		}
	}

	return fields, nil
}

func transactionChanges(
	resources []touchedResource,
	creates map[nodeKey]createdResource,
	deletes map[nodeKey]struct{},
) map[string][]pendingWrite {
	byDBName := make(map[string][]pendingWrite)
	for _, resource := range resources {
		runtime := baseRegistry[resource.dbName].(relationRuntime)
		if _, removed := deletes[nodeKey{typ: runtime.relationType(), id: resource.id}]; removed {
			continue
		}

		byDBName[resource.dbName] = append(byDBName[resource.dbName], pendingWrite{id: resource.id, work: resource.work})
	}

	for key, created := range creates {
		if _, removed := deletes[key]; removed {
			continue
		}

		byDBName[key.typ.Name()] = append(byDBName[key.typ.Name()], pendingWrite{id: key.id, work: created.work.Interface()})
	}

	for key := range deletes {
		if _, created := creates[key]; created {
			continue
		}

		byDBName[key.typ.Name()] = append(byDBName[key.typ.Name()], pendingWrite{id: key.id, deleted: true})
	}

	return byDBName
}

func validateTransactionIndexes(changes map[string][]pendingWrite) error {
	for dbName, writes := range changes {
		if err := committerFor(dbName).validateIndexes(writes); err != nil {
			return err
		}
	}

	return nil
}

func prepareTransactionIndexes(changes map[string][]pendingWrite) {
	for dbName, writes := range changes {
		committerFor(dbName).prepareIndexes(writes)
	}
}

func finishTransactionIndexes(changes map[string][]pendingWrite) {
	for dbName, writes := range changes {
		committerFor(dbName).finishIndexes(writes)
	}
}

func logTransactionChanges(byDBName map[string][]pendingWrite) error {
	if len(byDBName) == 0 {
		return nil
	}

	dbNames := make([]string, 0, len(byDBName))
	for dbName := range byDBName {
		dbNames = append(dbNames, dbName)
	}

	sort.Strings(dbNames)
	if len(dbNames) == 1 {
		return committerFor(dbNames[0]).logWrites(byDBName[dbNames[0]])
	}

	frame := transactionWALFrame{}
	for _, dbName := range dbNames {
		rows, err := committerFor(dbName).encodeWrites(byDBName[dbName])
		if err != nil {
			return err
		}

		for _, row := range rows {
			frame.Rows = append(frame.Rows, transactionWALRow{Type: dbName, walRow: row})
		}
	}

	return sharedTransactionWAL().appendFrame(frame)
}

func prepareStructuralCreateWAL(model *relationModel, resources []touchedResource, creates map[nodeKey]createdResource) bool {
	covered := make(map[nodeKey]struct{}, len(resources)+len(creates))
	for _, resource := range resources {
		runtime, ok := baseRegistry[resource.dbName].(relationRuntime)
		if ok {
			covered[nodeKey{typ: runtime.relationType(), id: resource.id}] = struct{}{}
		}
	}

	for key := range creates {
		covered[key] = struct{}{}
	}

	for _, ref := range model.refs {
		if ref.spec.kind != ownRelation || !ref.spec.many || ownSliceBackReferencePersists(model, ref) {
			continue
		}

		if _, writable := covered[ref.target]; !writable {
			return false
		}

		syncOwnSliceBackReference(model, ref)
	}

	return true
}

func ownSliceBackReferencePersists(model *relationModel, ref resolvedRelation) bool {
	child := model.nodes[ref.target]
	back := child.value.Elem().FieldByName(ref.spec.matchField)
	if back.Kind() == reflect.Pointer {
		id, present := valueID(back)
		return present && id == ref.holder.id
	}

	owner := model.nodes[ref.holder]
	id := owner.value.Elem().FieldByName(entityIDField)
	return scalarEqual(back, id)
}

func changedResources(resources []touchedResource) []touchedResource {
	for firstUnchanged, resource := range resources {
		if !entityStateEqual(resource.original, resource.work) {
			continue
		}

		changed := make([]touchedResource, firstUnchanged, len(resources)-1)
		copy(changed, resources[:firstUnchanged])
		for _, remaining := range resources[firstUnchanged+1:] {
			if !entityStateEqual(remaining.original, remaining.work) {
				changed = append(changed, remaining)
			}
		}

		return changed
	}

	return resources
}

func entityStateEqual(before, after any) bool {
	a := reflect.ValueOf(before).Elem()
	b := reflect.ValueOf(after).Elem()
	specs, err := specsByField(a.Type())
	if err != nil {
		return false
	}

	if len(specs) == 0 {
		return reflect.DeepEqual(a.Interface(), b.Interface())
	}

	for i := range a.NumField() {
		field := a.Type().Field(i)
		left := a.Field(i)
		right := b.Field(i)
		if spec, relational := specs[field.Name]; relational {
			if !relationValueEqual(left, right, spec.many) {
				return false
			}

			continue
		}

		if !relationFieldEqual(left, right) {
			return false
		}
	}

	return true
}

func relationFieldEqual(left, right reflect.Value) bool {
	if left.Type() == right.Type() {
		switch left.Kind() {
		case reflect.Bool:
			return left.Bool() == right.Bool()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return left.Int() == right.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return left.Uint() == right.Uint()
		case reflect.Float32, reflect.Float64:
			return left.Float() == right.Float()
		case reflect.Complex64, reflect.Complex128:
			return left.Complex() == right.Complex()
		case reflect.String:
			return left.String() == right.String()
		case reflect.Chan, reflect.Pointer, reflect.UnsafePointer:
			return left.Pointer() == right.Pointer()
		}
	}

	return reflect.DeepEqual(left.Interface(), right.Interface())
}

func relationValueEqual(left, right reflect.Value, many bool) bool {
	if !many {
		leftID, leftOK := valueID(left)
		rightID, rightOK := valueID(right)
		return leftOK == rightOK && (!leftOK || leftID == rightID)
	}

	if left.Len() != right.Len() {
		return false
	}

	for i := range left.Len() {
		leftID, leftOK := valueID(left.Index(i))
		rightID, rightOK := valueID(right.Index(i))
		if leftOK != rightOK || leftOK && leftID != rightID {
			return false
		}
	}

	return true
}

func validateTransactionGraph(resources []touchedResource, creates map[nodeKey]createdResource, deletes map[nodeKey]stagedDelete) (*relationModel, map[nodeKey]struct{}, error) {
	overrides := make(map[nodeKey]relationGraphNode, len(resources)+len(creates))
	for _, resource := range resources {
		runtime, ok := baseRegistry[resource.dbName].(relationRuntime)
		if !ok {
			continue
		}

		key := nodeKey{typ: runtime.relationType(), id: resource.id}
		overrides[key] = relationGraphNode{key: key, value: reflect.ValueOf(resource.work)}
	}

	for key, created := range creates {
		overrides[key] = relationGraphNode{key: key, value: created.work}
	}

	state, err := prepareTransactionRelationState(overrides, creates)
	if err != nil {
		return nil, nil, err
	}

	var model *relationModel
	if state.rebuild {
		model, err = buildRelationModelFromTargets(state.nodes, state.targets, state.targetFields)
	} else {
		model, err = buildRelationModelDelta(state.committed, state.nodes, state.targets, overrides)
	}

	if err != nil {
		return nil, nil, err
	}

	explicit := make(map[nodeKey]struct{}, len(deletes))
	for key := range deletes {
		explicit[key] = struct{}{}
	}

	deleted, err := deletionClosure(model, explicit)
	if err != nil {
		return nil, nil, err
	}

	if err := validateRequiredRelations(model, deleted); err != nil {
		return nil, nil, err
	}

	return model, deleted, nil
}

type transactionRelationState struct {
	committed    *relationModel
	nodes        map[nodeKey]relationGraphNode
	targets      map[relationTargetKey]indexedRelationTarget
	targetFields map[reflect.Type]map[string]struct{}
	rebuild      bool
}

func prepareTransactionRelationState(
	overrides map[nodeKey]relationGraphNode,
	creates map[nodeKey]createdResource,
) (transactionRelationState, error) {
	committed := committedRelationModel()
	if committed == nil {
		nodes, err := collectRelationNodesWithOverrides(false, overrides)
		if err != nil {
			return transactionRelationState{}, err
		}

		fields, err := relationTargetFields(nodes)
		if err != nil {
			return transactionRelationState{}, err
		}

		return transactionRelationState{
			nodes: nodes, targets: buildRelationTargetIndexFromFields(nodes, fields),
			targetFields: fields, rebuild: true,
		}, nil
	}

	nodes := make(map[nodeKey]relationGraphNode, len(committed.nodes)+len(creates))
	for key, node := range committed.nodes {
		nodes[key] = node
	}

	for key, node := range overrides {
		nodes[key] = node
	}

	for key := range creates {
		if !relationModelHasType(committed, key.typ) {
			fields, err := relationTargetFields(nodes)
			if err != nil {
				return transactionRelationState{}, err
			}

			return transactionRelationState{
				committed: committed, nodes: nodes, targets: buildRelationTargetIndexFromFields(nodes, fields),
				targetFields: fields, rebuild: true,
			}, nil
		}
	}

	if relationTargetKeysChanged(overrides, committed.nodes, committed.targetFields) {
		return transactionRelationState{
			committed: committed, nodes: nodes, targets: buildRelationTargetIndexFromFields(nodes, committed.targetFields),
			targetFields: committed.targetFields, rebuild: true,
		}, nil
	}

	if len(creates) == 0 {
		return transactionRelationState{
			committed: committed, nodes: nodes, targets: committed.targets, targetFields: committed.targetFields,
		}, nil
	}

	targets := make(map[relationTargetKey]indexedRelationTarget, len(committed.targets)+len(creates))
	for key, target := range committed.targets {
		targets[key] = target
	}

	rebuild := false
	for key := range creates {
		if relationNodeTargetsOverlap(targets, nodes[key], committed.targetFields[key.typ]) {
			rebuild = true
		}

		indexRelationNodeTargets(targets, nodes[key], committed.targetFields[key.typ])
	}

	return transactionRelationState{
		committed: committed, nodes: nodes, targets: targets,
		targetFields: committed.targetFields, rebuild: rebuild,
	}, nil
}

func relationModelHasType(model *relationModel, typ reflect.Type) bool {
	for key := range model.nodes {
		if key.typ == typ {
			return true
		}
	}

	return false
}

func relationNodeTargetsOverlap(
	targets map[relationTargetKey]indexedRelationTarget,
	node relationGraphNode,
	fields map[string]struct{},
) bool {
	for field := range fields {
		value, present := relationKey(node.value, field)
		if !present {
			continue
		}

		key := relationTargetKey{typ: node.key.typ, field: field, value: value.Interface()}
		if _, exists := targets[key]; exists {
			return true
		}
	}

	return false
}

func relationTargetKeysChanged(
	overrides, committed map[nodeKey]relationGraphNode,
	fields map[reflect.Type]map[string]struct{},
) bool {
	for key, after := range overrides {
		before, exists := committed[key]
		if !exists {
			continue
		}

		for field := range fields[key.typ] {
			left := before.value.Elem().FieldByName(field)
			right := after.value.Elem().FieldByName(field)
			if !relationFieldEqual(left, right) {
				return true
			}
		}
	}

	return false
}

// refresh pulls live state into every snapshot and re-stamps versions so the
// caller can retry. Caller holds every touched lock (commit does); en.mu is
// only ever taken after resource locks, never the reverse, so no deadlock.
func (en *transactionEngine) refresh(tx *transactionState) {
	en.mu.Lock()
	defer en.mu.Unlock()

	resources := tx.copyResources()
	for i := range resources {
		e := &resources[i]
		c := committerFor(e.dbName)

		v, ok := c.resourceVersion(e.id)
		if !ok {
			continue
		}

		c.refreshSnapshot(e.id, e.work)
		c.refreshSnapshot(e.id, e.original)
		e.ver = v
	}

	if len(resources) > 0 {
		tx.resource = resources[0]
		copy(tx.resources, resources[1:])
	}

	rewireTouchedCopies(resources)
}

func (en *transactionEngine) evict(tx *transactionState) {
	en.mu.Lock()
	defer en.mu.Unlock()

	if tx.hasResource {
		delete(en.bind, tx.resource.work)
	}

	for _, e := range tx.resources {
		delete(en.bind, e.work)
	}

	tx.active = false
	tx.hasResource = false
	tx.resource = touchedResource{}
	tx.resources = nil
	tx.resourcePos = nil
	tx.creates = nil
	tx.deletes = nil
}
