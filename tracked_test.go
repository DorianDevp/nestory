package nestory

import (
	"errors"
	"testing"
)

func auditTrackedWrites(t *testing.T) {
	t.Helper()

	previous := AuditTrackedWrites
	AuditTrackedWrites = true
	t.Cleanup(func() { AuditTrackedWrites = previous })
}

func TestTrackedUpdateWithinPublishesRootAndDeclaredChild(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		auditTrackedWrites(t)
		ownerDB, _, owner, children := seedTowerBranch(t)

		err := ownerDB.Tracked().UpdateWithin(owner.Id, func(w *Writes, shadow *relationBenchOwner) error {
			shadow.Name = "renamed"

			child := Edit(w, shadow.Children[1])
			child.Value = 21
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if owner.Name != "renamed" {
			t.Fatalf("owner.Name = %q, want renamed", owner.Name)
		}
		if children[1].Value != 21 {
			t.Fatalf("declared child Value = %d, want 21", children[1].Value)
		}
		if children[0].Value != 10 {
			t.Fatalf("untouched child Value = %d, want 10", children[0].Value)
		}
	})
}

func TestTrackedUpdateWithinNeedsNoEditForRootOnly(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		auditTrackedWrites(t)
		ownerDB, _, owner, _ := seedTowerBranch(t)

		err := ownerDB.Tracked().UpdateWithin(owner.Id, func(w *Writes, shadow *relationBenchOwner) error {
			shadow.Name = "root-only"
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if owner.Name != "root-only" {
			t.Fatalf("owner.Name = %q, want root-only", owner.Name)
		}
	})
}

func TestTrackedUpdateWithinAuditCatchesUndeclaredWrite(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		auditTrackedWrites(t)
		ownerDB, _, owner, children := seedTowerBranch(t)

		err := ownerDB.Tracked().UpdateWithin(owner.Id, func(w *Writes, shadow *relationBenchOwner) error {
			shadow.Children[0].Value = 999 // no Edit
			return nil
		})
		if !errors.Is(err, ErrUndeclaredWrite) {
			t.Fatalf("error = %v, want ErrUndeclaredWrite", err)
		}
		if children[0].Value != 10 {
			t.Fatalf("undeclared change was published: Value = %d", children[0].Value)
		}
		if owner.Name != "before" {
			t.Fatalf("owner.Name = %q, want before", owner.Name)
		}
	})
}

func TestTrackedUpdateWithinRejectsEditOutsideBranch(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		auditTrackedWrites(t)
		ownerDB, childDB, owner, _ := seedTowerBranch(t)

		stranger := &relationBenchChild{Value: 500}
		if err := childDB.Create(stranger); err != nil {
			t.Fatal(err)
		}

		err := ownerDB.Tracked().UpdateWithin(owner.Id, func(w *Writes, shadow *relationBenchOwner) error {
			Edit(w, stranger).Value = 501
			return nil
		})
		if err == nil {
			t.Fatal("declaring a node outside the branch was accepted")
		}
		if owner.Name != "before" {
			t.Fatalf("owner.Name = %q, want before", owner.Name)
		}
	})
}

// A tracked write that ends up changing relations has to reach the same commit
// path as an untracked one, since the shadow cannot publish pointers itself.
func TestTrackedUpdateWithinHandlesRelationEdit(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		auditTrackedWrites(t)
		ownerDB, _, owner, children := seedTowerBranch(t)

		err := ownerDB.Tracked().UpdateWithin(owner.Id, func(w *Writes, shadow *relationBenchOwner) error {
			shadow.Children[0], shadow.Children[1] = shadow.Children[1], shadow.Children[0]
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if owner.Children[0] != children[1] || owner.Children[1] != children[0] {
			t.Fatal("tracked relation edit did not preserve canonical child pointers")
		}
	})
}
