package nestory

import "testing"

func TestUnsafeGetPersistsLiveMutation(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	resetRegistries()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	if err := Register[bqItem](); err != nil {
		t.Fatal(err)
	}

	db := Open[bqItem]()
	item := &bqItem{Name: "cold"}
	db.AddToPersistQueue(item)
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	live, err := db.UnsafeGet(item.Id)
	if err != nil {
		t.Fatal(err)
	}

	if live != db.AllEntities()[0] {
		t.Fatal("UnsafeGet returned a copy instead of the stable store pointer")
	}

	allocs := testing.AllocsPerRun(1_000, func() {
		_, _ = db.UnsafeGet(item.Id)
	})
	if allocs != 0 {
		t.Fatalf("UnsafeGet allocations = %v, want 0", allocs)
	}

	live.Name = "hot"
	if found, _ := db.FindOneBy("Id", item.Id); found.Name != "hot" {
		t.Fatal("unsafe mutation was not immediately visible")
	}

	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	resetRegistries()
	if err := Register[bqItem](); err != nil {
		t.Fatal(err)
	}

	reloaded, err := Open[bqItem]().FindOneBy("Id", item.Id)
	if err != nil {
		t.Fatal(err)
	}

	if reloaded == nil || reloaded.Name != "hot" {
		t.Fatalf("reloaded item = %#v, want persisted unsafe mutation", reloaded)
	}
}

func TestUnsafeGetValidatesRelationsAtFlush(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		world := seedRuntime(t)
		live, err := world.userDB.UnsafeGet(world.user.Id)
		if err != nil {
			t.Fatal(err)
		}

		profile := live.Profile
		live.Profile = nil
		if err := world.userDB.Flush(); err == nil {
			t.Fatal("Flush accepted a nil required own relation")
		}

		if live.Profile != nil {
			t.Fatal("failed Flush unexpectedly rolled the unsafe mutation back")
		}

		live.Profile = profile
		if err := world.userDB.Flush(); err != nil {
			t.Fatalf("Flush after repairing live relation: %v", err)
		}

		ownedProfile, err := world.profileDB.UnsafeGet(profile.Id)
		if err != nil {
			t.Fatal(err)
		}

		owner := ownedProfile.Owner
		ownedProfile.Owner = nil
		if err := world.profileDB.Flush(); err == nil {
			t.Fatal("Flush inferred a nil required ownedby relation from own")
		}

		ownedProfile.Owner = owner
		if err := world.profileDB.Flush(); err != nil {
			t.Fatalf("Flush after repairing ownedby relation: %v", err)
		}
	})
}

func TestUnsafeGetMissingEntity(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	resetRegistries()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	if err := Register[bqItem](); err != nil {
		t.Fatal(err)
	}

	if _, err := Open[bqItem]().UnsafeGet(404); err != ErrNotFound {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

func TestUnsafeDeleteOwnerUsesCommittedOwnershipTree(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		world := seedRuntime(t)
		user, err := world.userDB.UnsafeGet(world.user.Id)
		if err != nil {
			t.Fatal(err)
		}

		profile, err := world.profileDB.UnsafeGet(world.profile.Id)
		if err != nil {
			t.Fatal(err)
		}

		// Destroy both in-memory descriptions of the edge. Flush must still know
		// that Profile belonged to User in the last committed graph.
		user.Profile = nil
		profile.Owner = nil
		if err := world.watcherDB.QueueDelete(world.watcher.Id); err != nil {
			t.Fatal(err)
		}

		if err := world.userDB.QueueDelete(world.user.Id); err != nil {
			t.Fatal(err)
		}

		if err := world.userDB.Flush(); err != nil {
			t.Fatalf("delete owner after unsafe relation mutation: %v", err)
		}

		if world.userDB.Len() != 0 || world.profileDB.Len() != 0 || world.postDB.Len() != 0 {
			t.Fatal("owner deletion did not remove its committed ownership subtree")
		}

		if world.assetDB.Len() != 1 || world.logDB.Len() != 1 {
			t.Fatal("owner deletion removed non-owned entities")
		}

		log, err := world.logDB.FindOneBy("Id", world.log.Id)
		if err != nil {
			t.Fatal(err)
		}

		if log.User != nil {
			t.Fatal("option into deleted ownership subtree was not cleared")
		}
	})
}

func TestUpdateWithinRejectsBrokenOwnershipBeforeWrite(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		world := seedRuntime(t)
		err := world.userDB.UpdateWithin(world.user.Id, func(user *rtUser) {
			user.Profile = nil
		})
		if err == nil {
			t.Fatal("UpdateWithin accepted a broken ownership tree")
		}

		live, findErr := world.userDB.FindOneBy("Id", world.user.Id)
		if findErr != nil {
			t.Fatal(findErr)
		}

		if live.Profile == nil {
			t.Fatal("failed safe update mutated the live ownership tree")
		}
	})
}
