package nestory

import (
	"errors"
	"fmt"
	"reflect"
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
	readLockResource(id int)
	readUnlockResource(id int)
	snapshotResource(id int) (work, original reflect.Value, version int, found bool)
	resourceVersion(id int) (int, bool)
	applyWrite(id int, work any)
	refreshSnapshot(id int, work any)
	logWrites(items []pendingWrite) error
}

type pendingWrite struct {
	id   int
	work any
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
	} else if tx.hasResource && tx.resource.dbName == e.dbName && tx.resource.id == e.id {
		return
	}

	if !tx.hasResource {
		tx.hasResource = true
		tx.resource = e
		en.bind[e.work] = tx
		return
	}

	if tx.resourcePos == nil {
		tx.resourcePos = map[transactionResourceKey]int{
			{dbName: tx.resource.dbName, id: tx.resource.id}: 0,
		}
	}

	tx.resources = append(tx.resources, e)
	tx.resourcePos[key] = len(tx.resources)
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

	graphChanged := structural || relationChanged
	if graphChanged {
		graphMu.Lock()
		defer graphMu.Unlock()
	} else {
		graphMu.RLock()
		defer graphMu.RUnlock()
	}

	var model *relationModel
	var deleted map[nodeKey]struct{}
	if graphChanged {
		model, deleted, err = validateTransactionGraph(touchedResources, createdResources, stagedDeletes)
		if err != nil {
			return err
		}

		touchedResources, err = materializeOwnBackReferences(model, transactionResources, touchedResources, createdResources, deleted)
		if err != nil {
			return err
		}
	}

	if !graphChanged && len(touchedResources) == 1 {
		return en.commitSingleWrite(tx, touchedResources[0])
	}

	for key := range createdResources {
		if _, exists := committerFor(key.typ.Name()).resourceVersion(key.id); exists {
			return fmt.Errorf("%w: %s", ErrAlreadyExists, key)
		}
	}

	// Global (dbName,id) lock order: overlapping commits can't form a wait cycle.
	sort.Slice(touchedResources, func(i, j int) bool {
		resA := touchedResources[i]
		resB := touchedResources[j]

		if resA.dbName != resB.dbName {
			return resA.dbName < resB.dbName
		}

		return resA.id < resB.id
	})

	for _, resource := range touchedResources {
		committerFor(resource.dbName).lockResource(resource.id)
	}

	defer func() {
		for i := len(touchedResources) - 1; i >= 0; i-- {
			committerFor(touchedResources[i].dbName).unlockResource(touchedResources[i].id)
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

	logStructuralCreate := structural && len(stagedDeletes) == 0 && prepareStructuralCreateWAL(model, touchedResources, createdResources)
	if !structural {
		if err := logTransactionWrites(touchedResources); err != nil {
			return err
		}
	} else if logStructuralCreate {
		if err := logTransactionCreates(touchedResources, createdResources); err != nil {
			return err
		}
	}

	for _, e := range touchedResources {
		committerFor(e.dbName).applyWrite(e.id, e.work)
	}

	for _, created := range createdResources {
		runtime := baseRegistry[created.key.typ.Name()].(relationRuntime)
		runtime.relationApplyCreate(created.work)
	}

	if !graphChanged {
		en.evict(tx)
		return nil
	}

	if err := bindRelationModelToLiveNodes(model, touchedResources, createdResources); err != nil {
		return err
	}

	reconcileRelations(model, deleted)
	applyDeletedNodes(deleted)
	rewireRelations(relationRuntimes())

	if structural && !logStructuralCreate {
		for _, runtime := range relationRuntimes() {
			if err := runtime.relationSave(); err != nil {
				return err
			}
		}
	}

	storeCommittedOwnership(model, deleted)

	en.evict(tx)

	return nil
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
			if !reflect.DeepEqual(before.Field(fieldIndex).Interface(), after.Field(fieldIndex).Interface()) {
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
		idField, found := typ.FieldByName("Id")
		if !found {
			return nil, fmt.Errorf("%w: %s.Id does not exist", ErrRelationSchema, typ)
		}

		fields[typ] = map[int]struct{}{idField.Index[0]: {}}

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

func logTransactionWrites(resources []touchedResource) error {
	if len(resources) == 1 {
		resource := resources[0]
		write := [1]pendingWrite{{id: resource.id, work: resource.work}}

		return committerFor(resource.dbName).logWrites(write[:])
	}

	byDBName := map[string][]pendingWrite{}
	order := make([]string, 0)
	for _, resource := range resources {
		if _, seen := byDBName[resource.dbName]; !seen {
			order = append(order, resource.dbName)
		}

		byDBName[resource.dbName] = append(byDBName[resource.dbName], pendingWrite{id: resource.id, work: resource.work})
	}

	for _, dbName := range order {
		if err := committerFor(dbName).logWrites(byDBName[dbName]); err != nil {
			return err
		}
	}

	return nil
}

func logTransactionCreates(resources []touchedResource, creates map[nodeKey]createdResource) error {
	byDBName := make(map[string][]pendingWrite)
	for _, resource := range resources {
		byDBName[resource.dbName] = append(byDBName[resource.dbName], pendingWrite{id: resource.id, work: resource.work})
	}

	for key, created := range creates {
		byDBName[key.typ.Name()] = append(byDBName[key.typ.Name()], pendingWrite{id: key.id, work: created.work.Interface()})
	}

	dbNames := make([]string, 0, len(byDBName))
	for dbName := range byDBName {
		dbNames = append(dbNames, dbName)
	}

	sort.Strings(dbNames)
	for _, dbName := range dbNames {
		if err := committerFor(dbName).logWrites(byDBName[dbName]); err != nil {
			return err
		}
	}

	return nil
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
	id := owner.value.Elem().FieldByName("Id")
	return scalarEqual(back, id)
}

func changedResources(resources []touchedResource) []touchedResource {
	var changed []touchedResource
	for _, resource := range resources {
		if !entityStateEqual(resource.original, resource.work) {
			changed = append(changed, resource)
		}
	}

	return changed
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

		if !reflect.DeepEqual(left.Interface(), right.Interface()) {
			return false
		}
	}

	return true
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

	nodes, err := collectRelationNodesWithOverrides(false, overrides)
	if err != nil {
		return nil, nil, err
	}

	model, err := buildRelationModel(nodes)
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
