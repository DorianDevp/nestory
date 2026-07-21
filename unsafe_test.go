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

		current := live.Current
		live.Current = nil
		if err := world.userDB.Flush(); err == nil {
			t.Fatal("Flush accepted a nil required borrow relation")
		}

		if live.Current != nil {
			t.Fatal("failed Flush unexpectedly rolled the unsafe mutation back")
		}

		live.Current = current
		if err := world.userDB.Flush(); err != nil {
			t.Fatalf("Flush after repairing live relation: %v", err)
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
