package nestory

import (
	"cmp"
	"reflect"
	"slices"
	"sync"
)

// Ownership branches carry one read-write lock each, held at the branch's
// top-level owner. It exists so a reader can exclude writers with a single
// acquisition instead of one per node: View used to RLock all 56 rows of a
// branch to read it, and 112 atomics scattered across 56 cache lines were two
// thirds of what reading a graph cost.
//
// The protocol is asymmetric on purpose. A reader takes the branch lock and
// nothing else. A writer takes the branch locks of every root it touches, then
// its per-row locks exactly as before: the row locks still serialize writers
// against each other, which is what the version checks rely on. Readers never
// take a row lock, so they cannot participate in a cycle with one; writers
// order branch locks among themselves the same way they order rows, by a total
// order over keys.
var branchLocks sync.Map

func branchLock(root nodeKey) *sync.RWMutex {
	if lock, found := branchLocks.Load(root); found {
		return lock.(*sync.RWMutex)
	}

	lock, _ := branchLocks.LoadOrStore(root, &sync.RWMutex{})

	return lock.(*sync.RWMutex)
}

func resetBranchLocks() { branchLocks = sync.Map{} }

// lockTouchedBranches write-locks the branch of every touched resource and
// returns the release. A node with no committed owner is its own root, so a
// write to a relation-free table locks exactly one branch: itself.
func lockTouchedBranches(resources []transactionResourceLock) func() {
	if len(resources) == 0 {
		return func() {}
	}

	roots := make([]nodeKey, 0, len(resources))
	for _, resource := range resources {
		root := committedBranchRoot(resource.dbName, resource.id)
		if root.typ == nil {
			continue
		}

		if !slices.Contains(roots, root) {
			roots = append(roots, root)
		}
	}

	if len(roots) == 0 {
		return func() {}
	}

	// The same total order writers use for rows, so two writers taking
	// overlapping branch sets cannot deadlock against each other.
	slices.SortFunc(roots, func(a, b nodeKey) int {
		if byName := cmp.Compare(a.typ.Name(), b.typ.Name()); byName != 0 {
			return byName
		}

		return cmp.Compare(a.id, b.id)
	})

	locks := make([]*sync.RWMutex, len(roots))
	for index, root := range roots {
		locks[index] = branchLock(root)
		locks[index].Lock()
	}

	return func() {
		for index := len(locks) - 1; index >= 0; index-- {
			locks[index].Unlock()
		}
	}
}

// committedBranchRoot resolves a resource to the top of its ownership branch.
// Registry lookups here are by name because that is what the lock lists carry;
// the result is what both sides of the protocol agree to lock.
func committedBranchRoot(dbName string, id int) nodeKey {
	registered, found := baseRegistry[dbName]
	if !found {
		return nodeKey{}
	}

	runtime, ok := registered.(relationRuntime)
	if !ok {
		return nodeKey{}
	}

	return committedOwnershipRoot(nodeKey{typ: runtime.relationType(), id: id})
}

// lockBranchForRead read-locks one branch. Reads take this and nothing else.
// It returns the mutex rather than a release closure: a method value escapes,
// and this sits on the hot read path.
func lockBranchForRead(typ reflect.Type, id int) *sync.RWMutex {
	lock := branchLock(committedOwnershipRoot(nodeKey{typ: typ, id: id}))
	lock.RLock()

	return lock
}
