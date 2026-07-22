package nestory

import (
	"errors"
	"sync"
	"testing"
)

type txOwner struct {
	Id    int      `key:"primary"`
	Child *txChild `rel:"own,Id"`
}

func (v txOwner) GetId() int { return v.Id }

type txChild struct {
	Id    int      `key:"primary"`
	Owner *txOwner `rel:"ownedby,Id"`
	N     int
}

func (v txChild) GetId() int { return v.Id }

type txKeyTarget struct {
	Id  int `key:"primary"`
	Key string
}

func (target txKeyTarget) GetId() int { return target.Id }

type txKeyBorrower struct {
	Id     int          `key:"primary"`
	Target *txKeyTarget `rel:"borrow,Key"`
}

func (borrower txKeyBorrower) GetId() int { return borrower.Id }

type txList struct {
	Id      int        `key:"primary"`
	Entries []*txEntry `rel:"own,List"`
}

func (list txList) GetId() int { return list.Id }

type txEntry struct {
	Id   int     `key:"primary"`
	List *txList `rel:"ownedby,Id"`
	Text string
}

func (entry txEntry) GetId() int { return entry.Id }

type txOptionalTarget struct {
	Id  int `key:"primary"`
	Key string
}

func (target txOptionalTarget) GetId() int { return target.Id }

type txOptionalHolder struct {
	Id     int               `key:"primary"`
	Target *txOptionalTarget `rel:"option,Key"`
}

func (holder txOptionalHolder) GetId() int { return holder.Id }

func TestTransactionEditsOwnershipTreeWithoutUpdate(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		world := seedRuntime(t)
		originalPost := world.current.Title

		err := world.userDB.Transaction(func(tx *Tx[rtUser]) error {
			first, err := tx.Get(world.user.Id)
			if err != nil {
				return err
			}

			second, err := tx.Get(world.user.Id)
			if err != nil {
				return err
			}

			if first != second {
				t.Fatal("repeated Get returned different transaction-local roots")
			}

			if first == world.user || first.Profile == world.profile || first.Posts[0] == world.current {
				t.Fatal("transaction exposed a live ownership pointer")
			}

			first.Name = "Grace"
			first.Posts[0].Title = "transactional"
			if world.current.Title != originalPost {
				t.Fatal("transaction mutation became visible before commit")
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		user, _ := world.userDB.Unsafe().Get(world.user.Id)
		post, _ := world.postDB.Unsafe().Get(world.current.Id)
		if user.Name != "Grace" || post.Title != "transactional" {
			t.Fatalf("committed graph = user %q, post %q", user.Name, post.Title)
		}

		if user.Posts[0] != post || post.User != user {
			t.Fatal("commit did not canonicalize the ownership pointers")
		}
	})
}

func TestTransactionErrorRollsBackOwnershipTree(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		world := seedRuntime(t)
		stop := errors.New("stop")

		err := world.userDB.Transaction(func(tx *Tx[rtUser]) error {
			user, getErr := tx.Get(world.user.Id)
			if getErr != nil {
				return getErr
			}

			user.Name = "discarded"
			user.Posts[0].Title = "discarded"
			return stop
		})
		if !errors.Is(err, stop) {
			t.Fatalf("Transaction error = %v, want %v", err, stop)
		}

		user, _ := world.userDB.FindOneBy("Id", world.user.Id)
		post, _ := world.postDB.FindOneBy("Id", world.current.Id)
		if user.Name != "Ada" || post.Title != "current" {
			t.Fatalf("rollback leaked changes: user %q, post %q", user.Name, post.Title)
		}
	})
}

func TestFlatUpdateMergesCompleteOwnershipBranch(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		world := seedRuntime(t)
		branch, err := world.userDB.Get(world.user.Id)
		if err != nil {
			t.Fatal(err)
		}

		branch.Name = "Lin"
		branch.Posts[0].Title = "flat branch"
		if world.user.Name != "Ada" || world.current.Title != "current" {
			t.Fatal("flat branch mutated live objects before Update")
		}

		if err := world.userDB.Update(branch); err != nil {
			t.Fatal(err)
		}

		user, _ := world.userDB.FindOneBy("Id", world.user.Id)
		post, _ := world.postDB.FindOneBy("Id", world.current.Id)
		if user.Name != "Lin" || post.Title != "flat branch" {
			t.Fatal("Update did not merge the complete ownership branch")
		}
	})
}

func TestUpdateWithinRetriesNestedOwnershipConflict(t *testing.T) {
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
		child := &txChild{Owner: owner}
		owner.Child = child
		ownerDB.Unsafe().Create(owner)
		childDB.Unsafe().Create(child)
		if err := ownerDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		const goroutines = 8
		const perGoroutine = 100
		var wait sync.WaitGroup
		wait.Add(goroutines)
		for range goroutines {
			go func() {
				defer wait.Done()
				for range perGoroutine {
					if err := ownerDB.UpdateWithin(owner.Id, func(branch *txOwner) error {
						branch.Child.N++
						return nil
					}); err != nil {
						panic(err)
					}
				}
			}()
		}

		wait.Wait()

		stored, err := childDB.FindOneBy("Id", child.Id)
		if err != nil {
			t.Fatal(err)
		}

		if want := goroutines * perGoroutine; stored.N != want {
			t.Fatalf("nested counter = %d, want %d", stored.N, want)
		}
	})
}

func TestTransactionCreatesAndDeletesOwnershipTreesOnce(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txChild](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txOwner](); err != nil {
			t.Fatal(err)
		}

		ownerDB := Open[txOwner]()
		childDB := Open[txChild]()
		oldOwner := &txOwner{}
		oldChild := &txChild{Owner: oldOwner}
		oldOwner.Child = oldChild
		ownerDB.Unsafe().Create(oldOwner)
		childDB.Unsafe().Create(oldChild)
		if err := ownerDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		newOwner := &txOwner{}
		newChild := &txChild{Owner: newOwner, N: 9}
		newOwner.Child = newChild
		err := ownerDB.Transaction(func(tx *Tx[txOwner]) error {
			if err := tx.Create(newOwner); err != nil {
				return err
			}

			created, err := tx.Get(newOwner.Id)
			if err != nil {
				return err
			}

			if created != newOwner {
				t.Fatal("Get did not reuse the staged create pointer")
			}

			created.Child.N++

			return tx.Delete(oldOwner.Id)
		})
		if err != nil {
			t.Fatal(err)
		}

		if ownerDB.Len() != 1 || childDB.Len() != 1 {
			t.Fatalf("live counts = owners %d, children %d", ownerDB.Len(), childDB.Len())
		}

		storedOwner, err := ownerDB.Unsafe().Get(newOwner.Id)
		if err != nil {
			t.Fatal(err)
		}

		storedChild, err := childDB.Unsafe().Get(newChild.Id)
		if err != nil {
			t.Fatal(err)
		}

		if storedChild.N != 10 || storedOwner.Child != storedChild || storedChild.Owner != storedOwner {
			t.Fatal("transaction did not persist and canonicalize the new ownership tree")
		}

		resetRegistries()
		if err := Register[txChild](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txOwner](); err != nil {
			t.Fatal(err)
		}

		if Open[txOwner]().Len() != 1 || Open[txChild]().Len() != 1 {
			t.Fatal("structural transaction did not survive reload")
		}
	})
}

func TestTransactionErrorDiscardsCreatedTree(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txChild](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txOwner](); err != nil {
			t.Fatal(err)
		}

		ownerDB := Open[txOwner]()
		Open[txChild]()
		owner := &txOwner{}
		child := &txChild{Owner: owner}
		owner.Child = child
		stop := errors.New("stop")
		err := ownerDB.Transaction(func(tx *Tx[txOwner]) error {
			if err := tx.Create(owner); err != nil {
				return err
			}

			return stop
		})
		if !errors.Is(err, stop) {
			t.Fatalf("error = %v, want %v", err, stop)
		}

		if ownerDB.Len() != 0 || Open[txChild]().Len() != 0 {
			t.Fatal("rolled back create reached the live store")
		}
	})
}

func TestShortCreateAndDeleteOwnCompleteTree(t *testing.T) {
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
		child := &txChild{Owner: owner}
		owner.Child = child
		if err := ownerDB.Create(owner); err != nil {
			t.Fatal(err)
		}

		if ownerDB.Len() != 1 || childDB.Len() != 1 {
			t.Fatal("Create did not persist the complete ownership tree")
		}

		if err := ownerDB.Delete(owner.Id); err != nil {
			t.Fatal(err)
		}

		if ownerDB.Len() != 0 || childDB.Len() != 0 {
			t.Fatal("Delete did not cascade through the complete ownership tree")
		}
	})
}

func TestCreateUsesWALAndRehydratesScalarOwnership(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[rtChild](); err != nil {
			t.Fatal(err)
		}

		if err := Register[rtParent](); err != nil {
			t.Fatal(err)
		}

		parentDB := Open[rtParent]()
		Open[rtChild]()
		child := &rtChild{}
		parent := &rtParent{Children: []*rtChild{child}}
		if err := parentDB.Create(parent); err != nil {
			t.Fatal(err)
		}

		if child.ParentID != parent.Id {
			t.Fatalf("child ParentID = %d, want %d", child.ParentID, parent.Id)
		}

		for _, typeName := range []string{"rtParent", "rtChild"} {
			chunks, err := sortedChunkFiles(chunkDirFor(typeName))
			if err != nil {
				t.Fatal(err)
			}

			if len(chunks) != 0 {
				t.Fatalf("%s create wrote snapshot chunks: %v", typeName, chunks)
			}
		}

		resetRegistries()
		if err := Register[rtChild](); err != nil {
			t.Fatal(err)
		}

		if err := Register[rtParent](); err != nil {
			t.Fatal(err)
		}

		Open[rtChild]()
		reloadedParent, err := Open[rtParent]().Get(parent.Id)
		if err != nil {
			t.Fatal(err)
		}

		if len(reloadedParent.Children) != 1 || reloadedParent.Children[0].Id != child.Id {
			t.Fatalf("reloaded children = %#v", reloadedParent.Children)
		}

		if reloadedParent.Children[0].ParentID != parent.Id {
			t.Fatalf("reloaded child ParentID = %d, want %d", reloadedParent.Children[0].ParentID, parent.Id)
		}
	})
}

func TestCreatePublishesRelationIndexDelta(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txEntry](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txList](); err != nil {
			t.Fatal(err)
		}

		entryDB := Open[txEntry]()
		listDB := Open[txList]()
		list := &txList{}
		first := &txEntry{List: list, Text: "first"}
		list.Entries = []*txEntry{first}
		if err := listDB.Create(list); err != nil {
			t.Fatal(err)
		}

		before := committedRelationIndexSnapshot()
		second := &txEntry{Text: "second"}
		err := listDB.Transaction(func(tx *Tx[txList]) error {
			current, err := tx.Get(list.Id)
			if err != nil {
				return err
			}

			second.List = current
			current.Entries = append(current.Entries, second)
			return entryDB.Join(tx).Create(second)
		})
		if err != nil {
			t.Fatal(err)
		}

		if after := committedRelationIndexSnapshot(); after != before {
			t.Fatal("create rebuilt the committed relation index")
		}

		storedList, err := listDB.Unsafe().Get(list.Id)
		if err != nil {
			t.Fatal(err)
		}

		storedEntry, err := entryDB.Unsafe().Get(second.Id)
		if err != nil {
			t.Fatal(err)
		}

		if len(storedList.Entries) != 2 {
			t.Fatalf("stored entries = %d, want 2", len(storedList.Entries))
		}

		if storedList.Entries[1] != storedEntry || storedEntry.List != storedList {
			t.Fatalf(
				"create pointers: entries=%d appended=%p stored=%p back=%p owner=%p",
				len(storedList.Entries), storedList.Entries[1], storedEntry, storedEntry.List, storedList,
			)
		}

		resetRegistries()
		if err := Register[txEntry](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txList](); err != nil {
			t.Fatal(err)
		}

		Open[txEntry]()
		reloaded, err := Open[txList]().Get(list.Id)
		if err != nil {
			t.Fatal(err)
		}

		if len(reloaded.Entries) != 2 || reloaded.Entries[1].Text != "second" || reloaded.Entries[1].List != reloaded {
			t.Fatal("incremental create did not survive WAL replay")
		}
	})
}

func TestCreateIndexesOptionToExistingTarget(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txOptionalTarget](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txOptionalHolder](); err != nil {
			t.Fatal(err)
		}

		targetDB := Open[txOptionalTarget]()
		holderDB := Open[txOptionalHolder]()
		target := &txOptionalTarget{Key: "present"}
		targetDB.Unsafe().Create(target)
		holderDB.Unsafe().Create(&txOptionalHolder{Target: target})
		if err := holderDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		canonicalTarget, err := targetDB.Unsafe().Get(target.Id)
		if err != nil {
			t.Fatal(err)
		}

		before := committedRelationIndexSnapshot()
		holder := &txOptionalHolder{Target: canonicalTarget}
		if err := holderDB.Create(holder); err != nil {
			t.Fatal(err)
		}

		if after := committedRelationIndexSnapshot(); after != before {
			t.Fatal("option create rebuilt the committed relation index")
		}

		stored, err := holderDB.Unsafe().Get(holder.Id)
		if err != nil {
			t.Fatal(err)
		}

		if stored.Target != canonicalTarget {
			t.Fatalf("option target = %p, canonical = %p", stored.Target, canonicalTarget)
		}
	})
}

func TestCreateRewiresBorrowInverseIncrementally(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[rtIndexedTarget](); err != nil {
			t.Fatal(err)
		}

		if err := Register[rtIndexedHolder](); err != nil {
			t.Fatal(err)
		}

		targetDB := Open[rtIndexedTarget]()
		holderDB := Open[rtIndexedHolder]()
		target := &rtIndexedTarget{}
		targetDB.Unsafe().Create(target)
		holderDB.Unsafe().Create(&rtIndexedHolder{Targets: []*rtIndexedTarget{target}})
		if err := holderDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		canonicalTarget, err := targetDB.Unsafe().Get(target.Id)
		if err != nil {
			t.Fatal(err)
		}

		before := committedRelationIndexSnapshot()
		created := &rtIndexedHolder{Targets: []*rtIndexedTarget{canonicalTarget}}
		if err := holderDB.Create(created); err != nil {
			t.Fatal(err)
		}

		if after := committedRelationIndexSnapshot(); after != before {
			t.Fatal("borrow create rebuilt the committed relation index")
		}

		stored, err := holderDB.Unsafe().Get(created.Id)
		if err != nil {
			t.Fatal(err)
		}

		if len(canonicalTarget.Holders) != 2 || canonicalTarget.Holders[1] != stored {
			t.Fatalf("inverse holders = %#v, want created holder last", canonicalTarget.Holders)
		}
	})
}

func TestCreateDeltaRejectsMissingRequiredRelation(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txEntry](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txList](); err != nil {
			t.Fatal(err)
		}

		entryDB := Open[txEntry]()
		listDB := Open[txList]()
		list := &txList{}
		entry := &txEntry{List: list}
		list.Entries = []*txEntry{entry}
		if err := listDB.Create(list); err != nil {
			t.Fatal(err)
		}

		before := committedRelationIndexSnapshot()
		err := entryDB.Create(&txEntry{Text: "orphan"})
		if !errors.Is(err, ErrRelationInvariant) {
			t.Fatalf("error = %v, want ErrRelationInvariant", err)
		}

		if entryDB.Len() != 1 || committedRelationIndexSnapshot() != before {
			t.Fatal("invalid create changed live state")
		}
	})
}

func TestCreateDeltaRejectsOwnershipCycle(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[rtNode](); err != nil {
			t.Fatal(err)
		}

		db := Open[rtNode]()
		if err := db.Create(&rtNode{}); err != nil {
			t.Fatal(err)
		}

		before := committedRelationIndexSnapshot()
		cycle := &rtNode{}
		cycle.Children = []*rtNode{cycle}
		err := db.Create(cycle)
		if !errors.Is(err, ErrRelationInvariant) {
			t.Fatalf("error = %v, want ErrRelationInvariant", err)
		}

		if db.Len() != 1 || committedRelationIndexSnapshot() != before {
			t.Fatal("cyclic create changed live state")
		}
	})
}

func TestConcurrentCreatesPublishIntoOneRelationIndex(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txEntry](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txList](); err != nil {
			t.Fatal(err)
		}

		entryDB := Open[txEntry]()
		listDB := Open[txList]()
		lists := []*txList{{}, {}}
		for _, list := range lists {
			entry := &txEntry{List: list, Text: "seed"}
			list.Entries = []*txEntry{entry}
			if err := listDB.Create(list); err != nil {
				t.Fatal(err)
			}
		}

		before := committedRelationIndexSnapshot()
		start := make(chan struct{})
		var ready sync.WaitGroup
		ready.Add(len(lists))
		errors := make(chan error, len(lists))
		for _, list := range lists {
			go func() {
				ready.Done()
				<-start
				errors <- listDB.Transaction(func(tx *Tx[txList]) error {
					current, err := tx.Get(list.Id)
					if err != nil {
						return err
					}

					entry := &txEntry{List: current, Text: "concurrent"}
					current.Entries = append(current.Entries, entry)
					if err := entryDB.Join(tx).Create(entry); err != nil {
						return err
					}

					return nil
				})
			}()
		}

		ready.Wait()
		close(start)
		for range lists {
			if err := <-errors; err != nil {
				t.Fatal(err)
			}
		}

		if after := committedRelationIndexSnapshot(); after != before {
			t.Fatal("concurrent creates replaced the committed relation index")
		}

		for _, list := range lists {
			stored, err := listDB.Unsafe().Get(list.Id)
			if err != nil {
				t.Fatal(err)
			}

			if len(stored.Entries) != 2 || stored.Entries[1].List != stored {
				t.Fatal("concurrent create did not publish canonical ownership")
			}
		}
	})
}

func TestReparentPersistsMaterializedOwnershipBackReference(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[rtNode](); err != nil {
			t.Fatal(err)
		}

		db := Open[rtNode]()
		root := &rtNode{}
		left := &rtNode{}
		right := &rtNode{}
		branch := &rtNode{Children: []*rtNode{{}}}
		left.Children = []*rtNode{branch}
		root.Children = []*rtNode{left, right}
		if err := db.Create(root); err != nil {
			t.Fatal(err)
		}

		if err := db.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		err := db.UpdateWithin(root.Id, func(current *rtNode) error {
			currentLeft := findRTNode(current, left.Id)
			currentRight := findRTNode(current, right.Id)
			moved := currentLeft.Children[0]
			currentLeft.Children = nil
			currentRight.Children = append(currentRight.Children, moved)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		stored, err := db.Unsafe().Get(branch.Id)
		if err != nil {
			t.Fatal(err)
		}

		if stored.ParentID != right.Id {
			t.Fatalf("live branch ParentID = %d, want %d", stored.ParentID, right.Id)
		}

		resetRegistries()
		if err := Register[rtNode](); err != nil {
			t.Fatal(err)
		}

		reloaded, err := Open[rtNode]().Get(root.Id)
		if err != nil {
			t.Fatal(err)
		}

		reloadedLeft := findRTNode(reloaded, left.Id)
		reloadedRight := findRTNode(reloaded, right.Id)
		if len(reloadedLeft.Children) != 0 {
			t.Fatal("old parent regained the branch after WAL replay")
		}

		if children := reloadedRight.Children; len(children) != 1 || children[0].Id != branch.Id {
			t.Fatalf("new parent's children = %#v", children)
		}
	})
}

func findRTNode(root *rtNode, id int) *rtNode {
	if root.Id == id {
		return root
	}

	for _, child := range root.Children {
		if found := findRTNode(child, id); found != nil {
			return found
		}
	}

	return nil
}

func TestTransactionRejectsOwnershipCycleFromDelta(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[rtNode](); err != nil {
			t.Fatal(err)
		}

		db := Open[rtNode]()
		root := &rtNode{Children: []*rtNode{{}}}
		if err := db.Create(root); err != nil {
			t.Fatal(err)
		}

		err := db.UpdateWithin(root.Id, func(current *rtNode) error {
			current.Children[0].Children = []*rtNode{current}
			return nil
		})
		if !errors.Is(err, ErrRelationInvariant) {
			t.Fatalf("cycle error = %v, want %v", err, ErrRelationInvariant)
		}
	})
}

func TestTransactionRebuildsIndexWhenRelationKeyChanges(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txKeyTarget](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txKeyBorrower](); err != nil {
			t.Fatal(err)
		}

		targetDB := Open[txKeyTarget]()
		borrowerDB := Open[txKeyBorrower]()
		target := &txKeyTarget{Key: "stable"}
		targetDB.Unsafe().Create(target)
		borrowerDB.Unsafe().Create(&txKeyBorrower{Target: target})
		if err := targetDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		err := targetDB.UpdateWithin(target.Id, func(current *txKeyTarget) error {
			current.Key = "changed"
			return nil
		})
		if !errors.Is(err, ErrRelationInvariant) {
			t.Fatalf("key change error = %v, want %v", err, ErrRelationInvariant)
		}
	})
}

func TestTransactionRejectsDuplicateRelationKeyOnCreate(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txKeyTarget](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txKeyBorrower](); err != nil {
			t.Fatal(err)
		}

		targetDB := Open[txKeyTarget]()
		borrowerDB := Open[txKeyBorrower]()
		target := &txKeyTarget{Key: "duplicate"}
		targetDB.Unsafe().Create(target)
		borrowerDB.Unsafe().Create(&txKeyBorrower{Target: target})
		if err := targetDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		err := targetDB.Create(&txKeyTarget{Key: "duplicate"})
		if !errors.Is(err, ErrRelationInvariant) {
			t.Fatalf("duplicate key error = %v, want %v", err, ErrRelationInvariant)
		}
	})
}

func TestJoinCombinesDifferentTypesInOneCommit(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		world := seedRuntime(t)
		err := world.userDB.Transaction(func(users *Tx[rtUser]) error {
			watchers := world.watcherDB.Join(users)
			if err := watchers.Delete(world.watcher.Id); err != nil {
				return err
			}

			return users.Delete(world.user.Id)
		})
		if err != nil {
			t.Fatal(err)
		}

		if world.userDB.Len() != 0 || world.profileDB.Len() != 0 || world.postDB.Len() != 0 || world.watcherDB.Len() != 0 {
			t.Fatal("joined transaction did not delete the requested ownership and borrower trees")
		}

		if world.assetDB.Len() != 1 || world.logDB.Len() != 1 {
			t.Fatal("joined transaction deleted unowned entities")
		}

		log, err := world.logDB.Unsafe().Get(world.log.Id)
		if err != nil {
			t.Fatal(err)
		}

		if log.User != nil {
			t.Fatal("joined transaction did not clear an option into the deleted tree")
		}
	})
}

func TestTransactionDeleteIsBlockedByOutsiderBorrow(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		world := seedRuntime(t)
		err := world.userDB.Delete(world.user.Id)
		if !errors.Is(err, ErrDeleteRestricted) {
			t.Fatalf("delete error = %v, want %v", err, ErrDeleteRestricted)
		}

		if world.userDB.Len() != 1 || world.postDB.Len() != 2 || world.watcherDB.Len() != 1 {
			t.Fatal("rejected delete changed the live ownership or borrower graph")
		}

		current, getErr := world.postDB.Unsafe().Get(world.current.Id)
		if getErr != nil {
			t.Fatal(getErr)
		}

		if len(current.Watchers) != 1 || current.Watchers[0].Id != world.watcher.Id {
			t.Fatal("rejected delete detached the outsider borrow inverse")
		}
	})
}

func TestTargetedBorrowRepointUpdatesBothInverseSides(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		world := seedRuntime(t)
		favorite, err := world.postDB.Unsafe().Get(world.favorite.Id)
		if err != nil {
			t.Fatal(err)
		}

		err = world.watcherDB.UpdateWithin(world.watcher.Id, func(watcher *rtWatcher) error {
			watcher.Post = favorite
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		current, err := world.postDB.Unsafe().Get(world.current.Id)
		if err != nil {
			t.Fatal(err)
		}

		favorite, err = world.postDB.Unsafe().Get(world.favorite.Id)
		if err != nil {
			t.Fatal(err)
		}

		watcher, err := world.watcherDB.Unsafe().Get(world.watcher.Id)
		if err != nil {
			t.Fatal(err)
		}

		if len(current.Watchers) != 0 {
			t.Fatal("old borrow target retained the inverse holder")
		}

		if len(favorite.Watchers) != 1 || favorite.Watchers[0] != watcher || watcher.Post != favorite {
			t.Fatal("new borrow target did not receive the canonical inverse holder")
		}
	})
}

func TestIndexedBorrowRepointMovesDeleteVeto(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txKeyTarget](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txKeyBorrower](); err != nil {
			t.Fatal(err)
		}

		targetDB := Open[txKeyTarget]()
		borrowerDB := Open[txKeyBorrower]()
		first := &txKeyTarget{Key: "first"}
		second := &txKeyTarget{Key: "second"}
		borrower := &txKeyBorrower{Target: first}
		targetDB.Unsafe().Create(first)
		targetDB.Unsafe().Create(second)
		borrowerDB.Unsafe().Create(borrower)
		if err := targetDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		secondLive, err := targetDB.Unsafe().Get(second.Id)
		if err != nil {
			t.Fatal(err)
		}

		err = borrowerDB.UpdateWithin(borrower.Id, func(current *txKeyBorrower) error {
			current.Target = secondLive
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := targetDB.Delete(first.Id); err != nil {
			t.Fatalf("old borrow target remained restricted: %v", err)
		}

		if err := targetDB.Delete(second.Id); !errors.Is(err, ErrDeleteRestricted) {
			t.Fatalf("new borrow target delete error = %v, want %v", err, ErrDeleteRestricted)
		}
	})
}

func TestIndexedReparentMovesCascadeOwnership(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[rtNode](); err != nil {
			t.Fatal(err)
		}

		db := Open[rtNode]()
		root := &rtNode{}
		left := &rtNode{}
		right := &rtNode{}
		branch := &rtNode{}
		left.Children = []*rtNode{branch}
		root.Children = []*rtNode{left, right}
		if err := db.Create(root); err != nil {
			t.Fatal(err)
		}

		err := db.Transaction(func(tx *Tx[rtNode]) error {
			from, getErr := tx.Get(left.Id)
			if getErr != nil {
				return getErr
			}

			to, getErr := tx.Get(right.Id)
			if getErr != nil {
				return getErr
			}

			to.Children = append(to.Children, from.Children[0])
			from.Children = nil

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := db.Delete(left.Id); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Unsafe().Get(branch.Id); err != nil {
			t.Fatalf("new owner's child was deleted with the old owner: %v", err)
		}

		if err := db.Delete(right.Id); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Unsafe().Get(branch.Id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("reparented child lookup error = %v, want %v", err, ErrNotFound)
		}
	})
}

func TestConcurrentBorrowAndDeleteCannotBothCommit(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[txKeyTarget](); err != nil {
			t.Fatal(err)
		}

		if err := Register[txKeyBorrower](); err != nil {
			t.Fatal(err)
		}

		targetDB := Open[txKeyTarget]()
		borrowerDB := Open[txKeyBorrower]()
		first := &txKeyTarget{Key: "first"}
		doomed := &txKeyTarget{Key: "doomed"}
		borrower := &txKeyBorrower{Target: first}
		targetDB.Unsafe().Create(first)
		targetDB.Unsafe().Create(doomed)
		borrowerDB.Unsafe().Create(borrower)
		if err := targetDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		doomedLive, err := targetDB.Unsafe().Get(doomed.Id)
		if err != nil {
			t.Fatal(err)
		}

		ready := make(chan struct{}, 2)
		release := make(chan struct{})
		results := make(chan transactionRaceResult, 2)
		go func() {
			err := targetDB.Transaction(func(tx *Tx[txKeyTarget]) error {
				if deleteErr := tx.Delete(doomed.Id); deleteErr != nil {
					return deleteErr
				}

				ready <- struct{}{}
				<-release
				return nil
			})
			results <- transactionRaceResult{name: "delete", err: err}
		}()
		go func() {
			err := borrowerDB.Transaction(func(tx *Tx[txKeyBorrower]) error {
				current, getErr := tx.Get(borrower.Id)
				if getErr != nil {
					return getErr
				}

				current.Target = doomedLive
				ready <- struct{}{}
				<-release
				return nil
			})
			results <- transactionRaceResult{name: "borrow", err: err}
		}()

		<-ready
		<-ready
		close(release)
		firstResult := <-results
		secondResult := <-results
		if firstResult.err == nil && secondResult.err == nil {
			t.Fatal("concurrent borrow and target delete both committed")
		}

		if firstResult.err != nil && secondResult.err != nil {
			t.Fatalf("both transactions failed: %s=%v, %s=%v", firstResult.name, firstResult.err, secondResult.name, secondResult.err)
		}
	})
}

type transactionRaceResult struct {
	name string
	err  error
}

func TestTargetedReferenceReorderPreservesOrderAndInverseUniqueness(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[rtIndexedTarget](); err != nil {
			t.Fatal(err)
		}

		if err := Register[rtIndexedHolder](); err != nil {
			t.Fatal(err)
		}

		targetDB := Open[rtIndexedTarget]()
		holderDB := Open[rtIndexedHolder]()
		first := &rtIndexedTarget{}
		second := &rtIndexedTarget{}
		targetDB.Unsafe().Create(first)
		targetDB.Unsafe().Create(second)
		holder := &rtIndexedHolder{Targets: []*rtIndexedTarget{second, first, second}}
		holderDB.Unsafe().Create(holder)
		if err := holderDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		firstLive, err := targetDB.Unsafe().Get(first.Id)
		if err != nil {
			t.Fatal(err)
		}

		secondLive, err := targetDB.Unsafe().Get(second.Id)
		if err != nil {
			t.Fatal(err)
		}

		err = holderDB.UpdateWithin(holder.Id, func(current *rtIndexedHolder) error {
			current.Targets = []*rtIndexedTarget{firstLive, secondLive, firstLive}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		holderLive, err := holderDB.Unsafe().Get(holder.Id)
		if err != nil {
			t.Fatal(err)
		}

		if len(holderLive.Targets) != 3 || holderLive.Targets[0] != firstLive || holderLive.Targets[1] != secondLive || holderLive.Targets[2] != firstLive {
			t.Fatalf("reference order changed: %#v", holderLive.Targets)
		}

		for _, target := range []*rtIndexedTarget{firstLive, secondLive} {
			if len(target.Holders) != 1 || target.Holders[0] != holderLive {
				t.Fatalf("inverse for target %d = %#v", target.Id, target.Holders)
			}
		}
	})
}

func TestSelfBorrowCountsOnceAndDoesNotBlockOwnDelete(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		if err := Register[schemaSelfBorrow](); err != nil {
			t.Fatal(err)
		}

		db := Open[schemaSelfBorrow]()
		entity := &schemaSelfBorrow{}
		entity.Peer = entity
		if err := db.Create(entity); err != nil {
			t.Fatal(err)
		}

		if err := db.Delete(entity.Id); err != nil {
			t.Fatal(err)
		}

		if db.Len() != 0 {
			t.Fatal("self-borrowing entity survived its own delete")
		}
	})
}
