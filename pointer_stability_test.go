package nestory

import "testing"

// TestPointerStableAcrossInserts is the regression test for Tier-1.1: a
// pointer handed out by the DB must stay valid after enough inserts to cross
// chunk boundaries (which, with the old []T backing store, reallocated the
// slice and dangled every outstanding pointer).
func TestPointerStableAcrossInserts(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	registerForTest[bqItem](t)
	db := Open[bqItem]()

	first := &bqItem{Name: "first"}
	db.Unsafe().Create(first)
	if err := db.Unsafe().Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Grab a stable pointer into the store. Ids are int and integer literals
	// are int, so the lookup key matches the index with no cast needed.
	p, err := db.Unsafe().Get(1)
	if err != nil || p == nil {
		t.Fatalf("FindOneBy(Id,1): p=%v err=%v", p, err)
	}

	// Insert well past a chunk boundary, this is what used to reallocate the
	// backing slice and invalidate p.
	for i := 0; i < chunkLimit+50; i++ {
		db.Unsafe().Create(&bqItem{Name: "filler"})
	}

	if err := db.Unsafe().Flush(); err != nil {
		t.Fatalf("flush 2: %v", err)
	}

	// Same id must resolve to the very same address...
	p2, _ := db.Unsafe().Get(1)
	if p2 != p {
		t.Errorf("pointer to id=1 moved across growth: %p -> %p", p, p2)
	}

	// ...and a mutation applied through the store must be visible via the
	// pointer we captured before all those inserts.
	branch, err := db.Get(1)
	if err != nil {
		t.Fatalf("get branch: %v", err)
	}

	branch.Name = "updated"
	if err := db.Update(branch); err != nil {
		t.Fatalf("update: %v", err)
	}

	if p.Name != "updated" {
		t.Errorf("stale pointer: p.Name = %q, want %q", p.Name, "updated")
	}
}
