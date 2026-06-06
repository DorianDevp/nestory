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
	typ  string
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

func committerFor(typ string) committer {
	return baseRegistry[typ].(committer)
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

func (en *Engine) commit(tx txId) error {
	en.mu.Lock()
	es := append([]touchedResource(nil), en.txs[tx]...)
	en.mu.Unlock()

	if es == nil {
		return ErrExpiredSnapshot
	}

	// Global (typ,id) lock order: overlapping commits can't form a wait cycle.
	sort.Slice(es, func(i, j int) bool {
		if es[i].typ != es[j].typ {
			return es[i].typ < es[j].typ
		}
		return es[i].id < es[j].id
	})

	for _, e := range es {
		committerFor(e.typ).lockResource(e.id)
	}
	defer func() {
		for i := len(es) - 1; i >= 0; i-- {
			committerFor(es[i].typ).unlockResource(es[i].id)
		}
	}()

	for _, e := range es {
		v, ok := committerFor(e.typ).resourceVersion(e.id)
		if !ok || v != e.ver {
			en.refresh(tx)

			return ErrConflict
		}
	}

	// Log before mutating memory: a crash replays the whole tx or none of it.
	// One fsync per type.
	byTyp := map[string][]pendingWrite{}
	order := make([]string, 0)
	for _, e := range es {
		if _, seen := byTyp[e.typ]; !seen {
			order = append(order, e.typ)
		}
		byTyp[e.typ] = append(byTyp[e.typ], pendingWrite{id: e.id, work: e.work})
	}
	for _, typ := range order {
		if err := committerFor(typ).logWrites(byTyp[typ]); err != nil {
			return err
		}
	}

	for _, e := range es {
		committerFor(e.typ).applyWrite(e.id, e.work)
	}

	en.evict(tx)

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
		c := committerFor(e.typ)

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
