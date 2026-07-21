package nestory

import (
	"errors"
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
// data. The live Resource and the typed write-back resolve at commit via
// baseRegistry[typ].
type touchedResource struct {
	dbName   string
	id       int
	ver      int // version observed at snapshot time
	work     any // *T, the client's detached copy
	original any // *T, state at the start of the branch
}

// committer is the type-erased view the Engine drives at commit. *DB[T]
// implements it. resourceVersion/applyWrite/refreshSnapshot assume the per-row
// lock is already held — the Engine owns locking so it can enforce a global order.
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

func committerFor(dbName string) committer {
	return baseRegistry[dbName].(committer)
}

// Engine sits above every DB, owns the live contracts and serialises commits.
// It stays generic — reaches a type's resources only through baseRegistry.
type Engine struct {
	mu     sync.Mutex
	nextTx txId
	txs    map[txId][]touchedResource
	bind   map[any]txId // snapshot *T → owning tx, for O(1) Get→Update
}

func newEngine() *Engine {
	return &Engine{
		txs:  make(map[txId][]touchedResource),
		bind: make(map[any]txId),
	}
}

var engine = newEngine()

func (en *Engine) begin() txId {
	en.mu.Lock()
	defer en.mu.Unlock()

	en.nextTx++
	id := en.nextTx
	en.txs[id] = nil

	return id
}

func (en *Engine) record(tx txId, e touchedResource) {
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

func (en *Engine) work(tx txId, dbName string, id int) (any, bool) {
	en.mu.Lock()
	defer en.mu.Unlock()

	for _, resource := range en.txs[tx] {
		if resource.dbName == dbName && resource.id == id {
			return resource.work, true
		}
	}

	return nil, false
}

func (en *Engine) commitByPtr(work any) error {
	en.mu.Lock()
	tx, ok := en.bind[work]
	en.mu.Unlock()

	if !ok {
		return ErrExpiredSnapshot
	}

	return en.commit(tx)
}

func (en *Engine) discardByPtr(work any) {
	en.mu.Lock()
	tx, ok := en.bind[work]
	en.mu.Unlock()

	if ok {
		en.evict(tx)
	}
}

func (en *Engine) commit(transactionId txId) error {
	en.mu.Lock()
	touchedResources := append([]touchedResource(nil), en.txs[transactionId]...)
	en.mu.Unlock()

	if touchedResources == nil {
		return ErrExpiredSnapshot
	}

	graphMu.Lock()
	defer graphMu.Unlock()

	touchedResources = changedResources(touchedResources)
	if len(touchedResources) == 0 {
		en.evict(transactionId)
		return nil
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

	// Relation checks see every changed branch at once, before either the WAL
	// or live memory is changed.
	if err := validateRelationUpdates(touchedResources); err != nil {
		return err
	}

	// Log before mutating memory: a crash replays the whole tx or none of it.
	// One fsync per type.
	byDbName := map[string][]pendingWrite{}
	order := make([]string, 0)

	for _, e := range touchedResources {
		if _, seen := byDbName[e.dbName]; !seen {
			order = append(order, e.dbName)
		}

		byDbName[e.dbName] = append(byDbName[e.dbName], pendingWrite{id: e.id, work: e.work})
	}

	for _, dbName := range order {
		if err := committerFor(dbName).logWrites(byDbName[dbName]); err != nil {
			return err
		}
	}

	for _, e := range touchedResources {
		committerFor(e.dbName).applyWrite(e.id, e.work)
	}

	if nodes, err := collectRelationNodes(false, nil); err == nil {
		if model, modelErr := buildRelationModel(nodes); modelErr == nil {
			reconcileRelations(model, nil)
			for _, runtime := range relationRuntimes() {
				runtime.relationRewire()
			}

			storeCommittedOwnership(model)
		}
	}

	en.evict(transactionId)

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

func validateRelationUpdates(resources []touchedResource) error {
	overrides := make(map[nodeKey]relationGraphNode, len(resources))
	for _, resource := range resources {
		runtime, ok := baseRegistry[resource.dbName].(relationRuntime)
		if !ok {
			continue
		}

		key := nodeKey{typ: runtime.relationType(), id: resource.id}
		overrides[key] = relationGraphNode{key: key, value: reflect.ValueOf(resource.work)}
	}

	nodes, err := collectRelationNodesWithOverrides(false, overrides)
	if err != nil {
		return err
	}

	model, err := buildRelationModel(nodes)
	if err != nil {
		return err
	}

	return validateRequiredRelations(model, nil)
}

// refresh pulls live state into every snapshot and re-stamps versions so the
// caller can retry. Caller holds every touched lock (commit does); en.mu is
// only ever taken after resource locks, never the reverse, so no deadlock.
func (en *Engine) refresh(tx txId) {
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

func (en *Engine) evict(tx txId) {
	en.mu.Lock()
	defer en.mu.Unlock()

	for _, e := range en.txs[tx] {
		delete(en.bind, e.work)
	}

	delete(en.txs, tx)
}
