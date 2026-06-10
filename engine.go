package nestory

import (
	"errors"
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
	dbName  string
	id   int
	ver  int // version observed at snapshot time
	work any // *T, the client's detached copy
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

	en.txs[tx] = append(en.txs[tx], e)
	en.bind[e.work] = tx
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

	en.evict(transactionId)

	return nil
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
		e.ver = v
	}
}

func (en *Engine) evict(tx txId) {
	en.mu.Lock()
	defer en.mu.Unlock()

	for _, e := range en.txs[tx] {
		delete(en.bind, e.work)
	}

	delete(en.txs, tx)
}
