package nestory

import (
	"errors"
	"testing"
)

func TestViewReadsLiveOwnershipTreeWithoutDirtyingIt(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txChild](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txOwner](); err != nil {
			t.Fatal(err)
		}

		ownerDB := Open[txOwner]()
		childDB := Open[txChild]()
		owner := &txOwner{}
		child := &txChild{Owner: owner, N: 7}
		owner.Child = child
		if err := ownerDB.Create(owner); err != nil {
			t.Fatal(err)
		}

		if err := ownerDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		ownerResource, _ := ownerDB.resource(owner.Id)
		childResource, _ := childDB.resource(child.Id)
		var viewedOwner *txOwner
		var viewedChild *txChild
		if err := ownerDB.View(owner.Id, func(current *txOwner) error {
			if ownerResource.mu.TryLock() {
				ownerResource.mu.Unlock()
				t.Fatal("View did not read-lock its root")
			}

			if childResource.mu.TryLock() {
				childResource.mu.Unlock()
				t.Fatal("View did not read-lock an owned child")
			}

			viewedOwner = current
			viewedChild = current.Child
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if viewedOwner != ownerResource.item || viewedChild != childResource.item {
			t.Fatal("View did not expose canonical stable pointers")
		}

		if dirty := ownerDB.store.dirtyIndices(); len(dirty) != 0 {
			t.Fatalf("owner dirty chunks after View = %v", dirty)
		}

		if dirty := childDB.store.dirtyIndices(); len(dirty) != 0 {
			t.Fatalf("child dirty chunks after View = %v", dirty)
		}
	})
}

func TestViewReturnsLookupAndCallbackErrors(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[tCounter](); err != nil {
			t.Fatal(err)
		}

		db := Open[tCounter]()
		if err := db.View(404, func(*tCounter) error { return nil }); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing View error = %v, want %v", err, ErrNotFound)
		}

		if err := db.View(404, nil); err == nil {
			t.Fatal("View accepted a nil callback")
		}

		counter := &tCounter{}
		if err := db.Create(counter); err != nil {
			t.Fatal(err)
		}

		stop := errors.New("stop")
		if err := db.View(counter.Id, func(*tCounter) error { return stop }); !errors.Is(err, stop) {
			t.Fatalf("callback error = %v, want %v", err, stop)
		}
	})
}
