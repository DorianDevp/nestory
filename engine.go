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

type txId uint64

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

func committerFor(dbName string) committer {
	return baseRegistry[dbName].(committer)
}

// transactionEngine sits above every DB, owns the live contracts and serialises commits.
// It stays generic — reaches a type's resources only through baseRegistry.
type transactionEngine struct {
	mu      sync.Mutex
	nextTx  txId
	txs     map[txId][]touchedResource
	creates map[txId]map[nodeKey]createdResource
	deletes map[txId]map[nodeKey]stagedDelete
	bind    map[any]txId // snapshot *T → owning tx, for O(1) Get→Update
}

func newEngine() *transactionEngine {
	return &transactionEngine{
		txs:     make(map[txId][]touchedResource),
		creates: make(map[txId]map[nodeKey]createdResource),
		deletes: make(map[txId]map[nodeKey]stagedDelete),
		bind:    make(map[any]txId),
	}
}

var engine = newEngine()

func (en *transactionEngine) begin() txId {
	en.mu.Lock()
	defer en.mu.Unlock()

	en.nextTx++
	id := en.nextTx
	en.txs[id] = nil
	en.creates[id] = make(map[nodeKey]createdResource)
	en.deletes[id] = make(map[nodeKey]stagedDelete)

	return id
}

func (en *transactionEngine) created(tx txId, key nodeKey) (reflect.Value, bool) {
	en.mu.Lock()
	defer en.mu.Unlock()

	created, ok := en.creates[tx][key]
	return created.work, ok
}

func (en *transactionEngine) stageCreate(tx txId, created createdResource) error {
	en.mu.Lock()
	defer en.mu.Unlock()

	if _, active := en.txs[tx]; !active {
		return ErrExpiredSnapshot
	}
	if _, duplicate := en.creates[tx][created.key]; duplicate {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, created.key)
	}

	en.creates[tx][created.key] = created
	return nil
}

func (en *transactionEngine) stageDelete(tx txId, deleted stagedDelete) error {
	en.mu.Lock()
	defer en.mu.Unlock()

	if _, active := en.txs[tx]; !active {
		return ErrExpiredSnapshot
	}

	en.deletes[tx][deleted.key] = deleted
	return nil
}

func (en *transactionEngine) record(tx txId, e touchedResource) {
	en.mu.Lock()
	defer en.mu.Unlock()

	for _, existing := range en.txs[tx] {
		if existing.dbName == e.dbName && existing.id == e.id {
			return
		}
	}

	en.txs[tx] = append(en.txs[tx], e)
	en.bind[e.work] = tx
}

func (en *transactionEngine) work(tx txId, dbName string, id int) (any, bool) {
	en.mu.Lock()
	defer en.mu.Unlock()

	for _, resource := range en.txs[tx] {
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

func (en *transactionEngine) commit(transactionId txId) error {
	en.mu.Lock()
	resources, active := en.txs[transactionId]
	touchedResources := append([]touchedResource(nil), resources...)
	createdResources := make(map[nodeKey]createdResource, len(en.creates[transactionId]))
	for key, created := range en.creates[transactionId] {
		createdResources[key] = created
	}
	stagedDeletes := make(map[nodeKey]stagedDelete, len(en.deletes[transactionId]))
	for key, deleted := range en.deletes[transactionId] {
		stagedDeletes[key] = deleted
	}
	en.mu.Unlock()

	if !active {
		return ErrExpiredSnapshot
	}

	graphMu.Lock()
	defer graphMu.Unlock()

	touchedResources = changedResources(touchedResources)
	if len(touchedResources) == 0 && len(createdResources) == 0 && len(stagedDeletes) == 0 {
		en.evict(transactionId)
		return nil
	}

	_, deleted, err := validateTransactionGraph(touchedResources, createdResources, stagedDeletes)
	if err != nil {
		return err
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
			en.refresh(transactionId)

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

	// Relation checks see every changed branch at once, before either the WAL
	// or live memory is changed.
	structural := len(createdResources) > 0 || len(stagedDeletes) > 0
	if !structural {
		if err := logTransactionWrites(touchedResources); err != nil {
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

	nodes, err := collectRelationNodes(false, nil)
	if err != nil {
		return err
	}
	model, err := buildRelationModel(nodes)
	if err != nil {
		return err
	}
	reconcileRelations(model, deleted)
	applyDeletedNodes(deleted)
	for _, runtime := range relationRuntimes() {
		runtime.relationRewire()
	}

	if structural {
		for _, runtime := range relationRuntimes() {
			if err := runtime.relationSave(); err != nil {
				return err
			}
		}
	}

	if err := refreshCommittedOwnership(); err != nil {
		return err
	}

	en.evict(transactionId)

	return nil
}

func logTransactionWrites(resources []touchedResource) error {
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

func changedResources(resources []touchedResource) []touchedResource {
	changed := make([]touchedResource, 0, len(resources))
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
func (en *transactionEngine) refresh(tx txId) {
	en.mu.Lock()
	defer en.mu.Unlock()

	for i := range en.txs[tx] {
		e := &en.txs[tx][i]
		c := committerFor(e.dbName)

		v, ok := c.resourceVersion(e.id)
		if !ok {
			continue
		}

		c.refreshSnapshot(e.id, e.work)
		c.refreshSnapshot(e.id, e.original)
		e.ver = v
	}

	rewireTouchedCopies(en.txs[tx])
}

func (en *transactionEngine) evict(tx txId) {
	en.mu.Lock()
	defer en.mu.Unlock()

	for _, e := range en.txs[tx] {
		delete(en.bind, e.work)
	}

	delete(en.txs, tx)
	delete(en.creates, tx)
	delete(en.deletes, tx)
}
