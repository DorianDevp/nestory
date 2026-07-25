package nestory

import (
	"errors"
	"testing"
	"time"
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

		// A read on its own must not mark anything dirty; the writer below
		// legitimately does, so this has to be settled first.
		if err := ownerDB.View(owner.Id, func(*txOwner) error { return nil }); err != nil {
			t.Fatal(err)
		}

		if dirty := ownerDB.store.dirtyIndices(); len(dirty) != 0 {
			t.Fatalf("owner dirty chunks after View = %v", dirty)
		}

		if dirty := childDB.store.dirtyIndices(); len(dirty) != 0 {
			t.Fatalf("child dirty chunks after View = %v", dirty)
		}

		// Assert the property the contract promises — writers to the branch are
		// excluded for the callback's duration — rather than the mechanism that
		// delivers it. View used to hold one read lock per row; it now holds one
		// per branch, and the guarantee is what has to survive that.
		var viewedOwner *txOwner
		var viewedChild *txChild
		writerDone := make(chan struct{})
		if err := ownerDB.View(owner.Id, func(current *txOwner) error {
			go func() {
				defer close(writerDone)
				if err := childDB.UpdateWithin(child.Id, func(live *txChild) error {
					live.N = 99

					return nil
				}); err != nil {
					t.Error(err)
				}
			}()

			select {
			case <-writerDone:
				t.Error("a writer to an owned child ran during View")
			case <-time.After(25 * time.Millisecond):
			}

			viewedOwner = current
			viewedChild = current.Child

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		select {
		case <-writerDone:
		case <-time.After(2 * time.Second):
			t.Fatal("the writer never completed after View returned")
		}

		if childResource.item.N != 99 {
			t.Fatalf("child N = %d, want the writer's 99", childResource.item.N)
		}

		if viewedOwner != ownerResource.item || viewedChild != childResource.item {
			t.Fatal("View did not expose canonical stable pointers")
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
