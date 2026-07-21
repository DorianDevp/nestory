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
