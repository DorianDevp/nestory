package nestory

import (
	"os"
	"path/filepath"
	"testing"
)

// Regression tests for the bugs fixed alongside the gobase→nestory rename.

// ============================================================
// Bug: AddToPersistQueue assigned Id from len(GoEntity), which is still 0
// while items sit in the persist queue. Batch-queueing N items pre-Flush
// gave them all Id=1.
// ============================================================

type bqItem struct {
	Id   int `key:"primary"`
	Name string
}

func (b bqItem) GetId() int { return b.Id }

func TestRegression_BatchQueueAssignsDistinctIds(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	Register[bqItem]()
	db := Open[bqItem]()

	items := []*bqItem{
		{Name: "first"},
		{Name: "second"},
		{Name: "third"},
		{Name: "fourth"},
	}

	// Queue all four BEFORE any Flush — this is what the old code mishandled.
	for _, it := range items {
		if err := db.AddToPersistQueue(it); err != nil {
			t.Fatalf("AddToPersistQueue: %v", err)
		}
	}

	seen := map[int]bool{}
	for _, it := range items {
		if it.Id == 0 {
			t.Errorf("item %q has Id=0 after queueing", it.Name)
			continue
		}
		if seen[it.Id] {
			t.Errorf("duplicate Id %d assigned to %q", it.Id, it.Name)
		}
		seen[it.Id] = true
	}

	if len(seen) != 4 {
		t.Errorf("expected 4 distinct Ids, got %d (set=%v)", len(seen), seen)
	}

	// Flush and verify all four landed on disk with their Ids intact.
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := db.Len(); got != 4 {
		t.Errorf("expected 4 entities persisted, got %d", got)
	}
}

func TestRegression_QueueRespectsExistingMaxId(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	Register[bqItem]()
	db := Open[bqItem]()

	// First batch: ids 1, 2, 3.
	for i := 0; i < 3; i++ {
		_ = db.AddToPersistQueue(&bqItem{Name: "batch1"})
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	// Second batch should start from 4, not from 1 again.
	second := &bqItem{Name: "batch2"}
	_ = db.AddToPersistQueue(second)
	if second.Id != 4 {
		t.Errorf("second batch first Id: got %d, want 4", second.Id)
	}
}

// ============================================================
// Bug: the auto-id counter started at 0 on every Open, so the first insert
// after a RELOAD reused id=1 and silently patched the existing entity #1
// instead of appending. The counter must be seeded from the max persisted id.
// ============================================================

func TestRegression_CounterSeededAfterReload(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	// Phase 1: persist ids 1, 2, 3.
	resetRegistries()
	Register[bqItem]()
	db := Open[bqItem]()
	for i := 0; i < 3; i++ {
		if err := db.AddToPersistQueue(&bqItem{Name: "seed"}); err != nil {
			t.Fatalf("seed queue: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("seed flush: %v", err)
	}

	// Phase 2: reload a fresh base from disk and insert one more.
	resetRegistries()
	Register[bqItem]()
	db2 := Open[bqItem]()

	fresh := &bqItem{Name: "after-reload"}
	if err := db2.AddToPersistQueue(fresh); err != nil {
		t.Fatalf("post-reload queue: %v", err)
	}
	if fresh.Id != 4 {
		t.Fatalf("after reload, next id = %d, want 4 (counter must seed from max persisted id)", fresh.Id)
	}
	if err := db2.Flush(); err != nil {
		t.Fatalf("post-reload flush: %v", err)
	}

	// The new entity must be appended, not overwrite id 1.
	if got := db2.Len(); got != 4 {
		t.Errorf("expected 4 entities after reload+insert, got %d", got)
	}
}

// ============================================================
// Bug: save() wrote directly to the target file. A crash mid-encode
// corrupted the data. Fix: write to <name>.tmp, fsync, then os.Rename.
// ============================================================

func TestRegression_SaveIsAtomic_NoLeftoverTmp(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	Register[bqItem]()
	db := Open[bqItem]()

	_ = db.AddToPersistQueue(&bqItem{Name: "ok"})
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// After a successful Flush, only the final .gob should exist — no .tmp.
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("leftover tmp file after successful save: %s", e.Name())
		}
	}

	// And the final file must exist.
	finalPath := filepath.Join(tmpDir, "bqItem.gob")
	if _, err := os.Stat(finalPath); err != nil {
		t.Errorf("final gob file missing: %v", err)
	}
}

func TestRegression_SaveOverwritePreservesPrevOnNewWrite(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	Register[bqItem]()
	db := Open[bqItem]()

	// Write v1.
	_ = db.AddToPersistQueue(&bqItem{Name: "v1"})
	if err := db.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}
	finalPath := filepath.Join(tmpDir, "bqItem.gob")
	infoV1, err := os.Stat(finalPath)
	if err != nil {
		t.Fatalf("stat v1: %v", err)
	}

	// Write v2 — should fully replace v1, no .tmp leftover.
	_ = db.AddToPersistQueue(&bqItem{Name: "v2"})
	if err := db.Flush(); err != nil {
		t.Fatalf("second Flush: %v", err)
	}

	entries, _ := os.ReadDir(tmpDir)
	tmpCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			tmpCount++
		}
	}
	if tmpCount != 0 {
		t.Errorf("expected 0 .tmp files after v2 save, got %d", tmpCount)
	}

	infoV2, err := os.Stat(finalPath)
	if err != nil {
		t.Errorf("v2 file disappeared: %v", err)
	} else if infoV2.Size() <= infoV1.Size() {
		// Loose sanity check: v2 has more entries, file should grow.
		t.Logf("v1 size=%d, v2 size=%d (informational)",
			infoV1.Size(), infoV2.Size())
	}
}
