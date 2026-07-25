package nestory

import (
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func seedTowerBranch(t *testing.T) (*DB[relationBenchOwner], *DB[relationBenchChild], *relationBenchOwner, []*relationBenchChild) {
	t.Helper()

	if err := Register[relationBenchChild](); err != nil {
		t.Fatal(err)
	}
	if err := Register[relationBenchOwner](); err != nil {
		t.Fatal(err)
	}

	childDB := Open[relationBenchChild]()
	ownerDB := Open[relationBenchOwner]()
	owner := &relationBenchOwner{Name: "before"}
	children := []*relationBenchChild{
		{OwnerID: 1, Value: 10},
		{OwnerID: 1, Value: 20},
	}
	owner.Children = children
	ownerDB.Unsafe().Create(owner)
	for _, child := range children {
		child.OwnerID = owner.Id
		childDB.Unsafe().Create(child)
	}
	if err := ownerDB.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}

	canonicalOwner, err := ownerDB.Unsafe().Get(owner.Id)
	if err != nil {
		t.Fatal(err)
	}
	canonicalChildren := make([]*relationBenchChild, len(children))
	for index, child := range children {
		canonicalChildren[index], err = childDB.Unsafe().Get(child.Id)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := ownerDB.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}

	return ownerDB, childDB, canonicalOwner, canonicalChildren
}

func TestTowerUpdateWithinPatchesOnlyChangedNodes(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, childDB, owner, children := seedTowerBranch(t)
		ownerResource, _ := ownerDB.resource(owner.Id)
		firstResource, _ := childDB.resource(children[0].Id)
		secondResource, _ := childDB.resource(children[1].Id)
		ownerVersion := ownerResource.version
		firstVersion := firstResource.version
		secondVersion := secondResource.version

		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			if shadow == ownerResource.item || shadow.Children[0] == firstResource.item {
				t.Fatal("Tower exposed a canonical writable pointer")
			}
			if ownerResource.item.Name != "before" || firstResource.item.Value != 10 {
				t.Fatal("Tower mutation became visible before commit")
			}

			shadow.Name = "after"
			shadow.Children[0].Value = 11
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if ownerResource.item != owner || firstResource.item != children[0] {
			t.Fatal("Tower moved a canonical stable pointer")
		}
		if owner.Name != "after" || children[0].Value != 11 {
			t.Fatalf("committed values = %q/%d", owner.Name, children[0].Value)
		}
		if owner.Children[0] != children[0] || owner.Children[1] != children[1] {
			t.Fatal("Tower leaked shadow relation pointers into the canonical graph")
		}
		if ownerResource.version != ownerVersion+1 || firstResource.version != firstVersion+1 {
			t.Fatal("changed nodes did not receive exactly one version increment")
		}
		if secondResource.version != secondVersion {
			t.Fatal("unchanged child was written")
		}
	})
}

func TestTowerUpdateWithinDiscardsCallbackError(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, _, owner, children := seedTowerBranch(t)
		stop := errors.New("stop")

		err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			shadow.Name = "discarded"
			shadow.Children[0].Value = 999
			return stop
		})
		if !errors.Is(err, stop) {
			t.Fatalf("UpdateWithin error = %v, want %v", err, stop)
		}
		if owner.Name != "before" || children[0].Value != 10 {
			t.Fatal("failed Tower callback changed canonical state")
		}

		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			if shadow.Name != "before" || shadow.Children[0].Value != 10 {
				t.Fatal("Tower did not rebuild after a failed callback")
			}
			shadow.Name = "kept"
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if owner.Name != "kept" {
			t.Fatalf("owner.Name = %q, want kept", owner.Name)
		}
	})
}

func TestTowerRelationEditFallsBackAndCanonicalizesPointers(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, _, owner, children := seedTowerBranch(t)

		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			shadow.Children[0], shadow.Children[1] = shadow.Children[1], shadow.Children[0]
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if owner.Children[0] != children[1] || owner.Children[1] != children[0] {
			t.Fatal("relation fallback did not preserve canonical child pointers")
		}
	})
}

func TestTowerRebuildsAfterRegularTransaction(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, _, owner, _ := seedTowerBranch(t)

		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			shadow.Name = "tower-one"
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		branch, err := ownerDB.Get(owner.Id)
		if err != nil {
			t.Fatal(err)
		}
		branch.Name = "regular"
		if err := ownerDB.Update(branch); err != nil {
			t.Fatal(err)
		}

		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			if shadow.Name != "regular" {
				t.Fatalf("stale Tower value = %q, want regular", shadow.Name)
			}
			shadow.Name = "tower-two"
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if owner.Name != "tower-two" {
			t.Fatalf("owner.Name = %q, want tower-two", owner.Name)
		}
	})
}

// towerUnrelatedRow takes part in no relation, so its rows can never enter an
// ownership branch and writing them must not retire the replica.
type towerUnrelatedRow struct {
	Id    int
	Value int
}

func (row towerUnrelatedRow) GetId() int { return row.Id }

func TestTowerSurvivesWritesToUnrelatedTable(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, childDB, owner, children := seedTowerBranch(t)
		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			shadow.Name = "warm"
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !projectTower.replica.Load().synced() {
			t.Fatal("Tower is not synced right after its own commit")
		}

		if err := Register[towerUnrelatedRow](); err != nil {
			t.Fatal(err)
		}
		unrelatedDB := Open[towerUnrelatedRow]()
		row := &towerUnrelatedRow{}
		unrelatedDB.Unsafe().Create(row)
		if err := unrelatedDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}
		if err := unrelatedDB.UpdateWithin(row.Id, func(live *towerUnrelatedRow) error {
			live.Value = 42
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if !projectTower.replica.Load().synced() {
			t.Fatal("write to an unrelated table retired the Tower replica")
		}

		// A table inside the graph must still retire it.
		if err := childDB.UpdateWithin(children[0].Id, func(live *relationBenchChild) error {
			live.Value = 99
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if projectTower.replica.Load().synced() {
			t.Fatal("write to a participating table left the Tower replica synced")
		}
	})
}

// Nested roots are the one pair whose branches genuinely overlap: root owns
// mid, so UpdateWithin(root) and UpdateWithin(mid) both diff mid and leaf. They
// must queue behind their shared top-level owner instead of racing.
func TestTowerNestedRootsDoNotRace(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[dialogBenchNode](); err != nil {
			t.Fatal(err)
		}

		db := Open[dialogBenchNode]()
		root := &dialogBenchNode{Text: "root"}
		db.Unsafe().Create(root)
		mid := &dialogBenchNode{Text: "mid", ParentID: root.Id}
		db.Unsafe().Create(mid)
		leaf := &dialogBenchNode{Text: "leaf", ParentID: mid.Id}
		db.Unsafe().Create(leaf)
		mid.Children = []*dialogBenchNode{leaf}
		root.Children = []*dialogBenchNode{mid}
		if err := db.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		var group sync.WaitGroup
		for iteration := range 50 {
			for _, id := range []int{root.Id, mid.Id} {
				group.Add(1)
				go func() {
					defer group.Done()

					err := db.UpdateWithin(id, func(node *dialogBenchNode) error {
						node.Text = strconv.Itoa(iteration)
						for _, child := range node.Children {
							child.Text = strconv.Itoa(iteration)
						}
						return nil
					})
					if err != nil {
						t.Error(err)
					}
				}()
			}
		}

		group.Wait()
	})
}

// towerIfaceOwner carries a child whose any field can hold something that is
// not comparable at all, which is the one case a cached comparability answer
// has to stay away from.
type ifaceOwner struct {
	Id       int           `key:"primary"`
	Children []*ifaceChild `rel:"own,OwnerID"`
}

func (owner ifaceOwner) GetId() int { return owner.Id }

type ifaceChild struct {
	Id      int `key:"primary"`
	OwnerID int
	Payload any
}

func (child ifaceChild) GetId() int { return child.Id }

func TestTowerDiffsInterfaceFieldWithoutPanicking(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		// The child must not take the whole-struct fast path: reflect calls an
		// interface type comparable, but Value.Equal panics on a slice inside it.
		if towerAlwaysComparable(reflect.TypeFor[ifaceChild]()) {
			t.Fatal("ifaceChild claimed to be always comparable")
		}

		if err := Register[ifaceChild](); err != nil {
			t.Fatal(err)
		}
		if err := Register[ifaceOwner](); err != nil {
			t.Fatal(err)
		}

		childDB := Open[ifaceChild]()
		ownerDB := Open[ifaceOwner]()
		owner := &ifaceOwner{}
		child := &ifaceChild{Payload: []int{1, 2, 3}}
		owner.Children = []*ifaceChild{child}
		ownerDB.Unsafe().Create(owner)
		child.OwnerID = owner.Id
		childDB.Unsafe().Create(child)
		if err := ownerDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		// A slice inside any is not comparable; Value.Equal would panic on it.
		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *ifaceOwner) error {
			shadow.Children[0].Payload = []int{9}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		canonical, err := childDB.Unsafe().Get(child.Id)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := canonical.Payload.([]int); !ok || len(got) != 1 || got[0] != 9 {
			t.Fatalf("Payload = %#v, want []int{9}", canonical.Payload)
		}
	})
}

// The compiled comparator is what keeps a diff off reflect, and nothing else
// fails visibly if it stops being installed — the diff just gets several times
// slower. So assert the plan actually carries one.
func TestTowerPlanUsesCompiledComparator(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		seedTowerBranch(t)

		child, err := towerFields(reflect.TypeFor[relationBenchChild]())
		if err != nil {
			t.Fatal(err)
		}
		if child.equal == nil {
			t.Fatal("plain child type has no compiled comparator; diff fell back to reflect")
		}

		// An owner holds a relation, whose fields compare by id against the
		// baseline rather than by the pointer the shadow carries.
		owner, err := towerFields(reflect.TypeFor[relationBenchOwner]())
		if err != nil {
			t.Fatal(err)
		}
		if owner.equal != nil {
			t.Fatal("relation holder got a whole-struct comparator")
		}

		// The comparator must see through the pointer, not compare pointers.
		left := &relationBenchChild{Id: 1, OwnerID: 2, Value: 3}
		right := &relationBenchChild{Id: 1, OwnerID: 2, Value: 3}
		if !child.equal(left, right) {
			t.Fatal("comparator reported distinct pointers with equal contents as different")
		}

		right.Value = 4
		if child.equal(left, right) {
			t.Fatal("comparator missed a changed field")
		}
	})
}

// Shadow nodes live inside one array per table, so their addresses are only
// stable as long as that array never grows. A callback holds those addresses,
// and the relation wiring points at them, so a move is a correctness bug rather
// than a slowdown — the same rule chunkStore lives by.
func TestTowerShadowAddressesAreStable(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, _, owner, _ := seedTowerBranch(t)

		var firstOwner *relationBenchOwner
		var firstChildren []*relationBenchChild
		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			firstOwner = shadow
			firstChildren = append([]*relationBenchChild(nil), shadow.Children...)
			shadow.Name = "one"
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// Same replica, no rebuild in between: every address must be identical.
		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			if shadow != firstOwner {
				t.Fatal("shadow root moved between callbacks")
			}
			for index, child := range shadow.Children {
				if child != firstChildren[index] {
					t.Fatalf("shadow child %d moved between callbacks", index)
				}
			}
			shadow.Name = "two"
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if owner.Name != "two" {
			t.Fatalf("owner.Name = %q, want two", owner.Name)
		}
	})
}

func TestTowerCommitReplaysFromWAL(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, _, owner, _ := seedTowerBranch(t)
		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			shadow.Name = "replayed"
			shadow.Children[0].Value = 77
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		resetRegistries()
		if err := Register[relationBenchChild](); err != nil {
			t.Fatal(err)
		}
		if err := Register[relationBenchOwner](); err != nil {
			t.Fatal(err)
		}

		reloadedChildren := Open[relationBenchChild]()
		reloadedOwners := Open[relationBenchOwner]()
		reloadedOwner, err := reloadedOwners.Unsafe().Get(owner.Id)
		if err != nil {
			t.Fatal(err)
		}
		reloadedChild, err := reloadedChildren.Unsafe().Get(reloadedOwner.Children[0].Id)
		if err != nil {
			t.Fatal(err)
		}
		if reloadedOwner.Name != "replayed" || reloadedChild.Value != 77 {
			t.Fatalf("replayed values = %q/%d", reloadedOwner.Name, reloadedChild.Value)
		}
		if reloadedOwner.Children[0] != reloadedChild {
			t.Fatal("WAL reload did not restore canonical relation pointers")
		}
	})
}

func TestTowerCallbackDoesNotBlockCanonicalView(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, _, owner, _ := seedTowerBranch(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		updateDone := make(chan error, 1)

		go func() {
			updateDone <- ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
				shadow.Name = "after"
				close(entered)
				<-release
				return nil
			})
		}()

		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("Tower callback did not start")
		}

		viewDone := make(chan error, 1)
		go func() {
			viewDone <- ownerDB.View(owner.Id, func(current *relationBenchOwner) error {
				if current.Name != "before" {
					return errors.New("View observed uncommitted Tower state")
				}
				return nil
			})
		}()

		select {
		case err := <-viewDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("View blocked for the duration of the Tower callback")
		}

		close(release)
		if err := <-updateDone; err != nil {
			t.Fatal(err)
		}
		if owner.Name != "after" {
			t.Fatalf("owner.Name = %q, want after", owner.Name)
		}
	})
}

// TestTowerRefreshSeesReorderedChildren pins the refresh path down on a change
// that lives only in a relation field. A detached update that reorders an own
// slice leaves every scalar field of the owner untouched, so a refresh that
// skips relation fields when deciding what moved would keep the shadow's old
// slice — and the next callback would read stale children.
func TestTowerRefreshSeesReorderedChildren(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, _, owner, _ := seedTowerBranch(t)

		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			shadow.Name = "warm"
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		branch, err := ownerDB.Get(owner.Id)
		if err != nil {
			t.Fatal(err)
		}

		branch.Children[0], branch.Children[1] = branch.Children[1], branch.Children[0]
		if err := ownerDB.Update(branch); err != nil {
			t.Fatal(err)
		}

		first := owner.Children[0].Value

		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			if got := shadow.Children[0].Value; got != first {
				t.Fatalf("shadow.Children[0].Value = %d, want %d (stale shadow slice)", got, first)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}

// TestUnsafeCreateFlushUsesShadowDelta proves the create flush takes the delta
// route, not just that the result looks right: publishAndRewire nils the
// committed model, while the full rebuild stores a fresh one, so the nil is a
// fingerprint of the path taken. The shadow must then serve the new child from
// its own world, wired and diffable.
func TestUnsafeCreateFlushUsesShadowDelta(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, childDB, owner, _ := seedTowerBranch(t)

		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			shadow.Name = "warm"
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		child := &relationBenchChild{OwnerID: owner.Id, Value: 30}
		childDB.Unsafe().Create(child)
		owner.Children = append(owner.Children, child)
		if err := childDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		if committedRelationModel() != nil {
			t.Fatal("create flush rebuilt the full model instead of publishing a delta")
		}

		childKey := nodeKey{typ: reflect.TypeFor[relationBenchChild](), id: child.Id}
		ownerKey := nodeKey{typ: reflect.TypeFor[relationBenchOwner](), id: owner.Id}
		if got := committedRelationIndexSnapshot().owners[childKey]; got != ownerKey {
			t.Fatalf("owners[%v] = %v, want %v", childKey, got, ownerKey)
		}

		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
			if len(shadow.Children) != 3 {
				t.Fatalf("shadow sees %d children, want 3", len(shadow.Children))
			}

			last := shadow.Children[2]
			if last == child {
				t.Fatal("shadow hands out the live pointer instead of a shadow copy")
			}

			if last.Value != 30 {
				t.Fatalf("shadow child Value = %d, want 30", last.Value)
			}

			shadow.Name = "after-create"
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if owner.Name != "after-create" {
			t.Fatalf("owner.Name = %q, want after-create", owner.Name)
		}
	})
}
