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

	if got := len(*db.GoEntity); got != 4 {
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
