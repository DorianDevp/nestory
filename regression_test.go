package nestory

import (
	"os"
	"path/filepath"
	"testing"
)

// Bug: AddToPersistQueue assigned Id from len(), still 0 in queue, so a pre-Flush
// batch got all Id=1.
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

	// Queue all four before any Flush — what the old code mishandled.
	for _, it := range items {
		db.AddToPersistQueue(it)
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

	for i := 0; i < 3; i++ {
		db.AddToPersistQueue(&bqItem{Name: "batch1"})
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	// Second batch starts from 4, not 1.
	second := &bqItem{Name: "batch2"}
	db.AddToPersistQueue(second)
	if second.Id != 4 {
		t.Errorf("second batch first Id: got %d, want 4", second.Id)
	}
}

// Bug: the auto-id counter started at 0 on every Open, so the first insert after
// a reload reused id=1 and patched entity #1 instead of appending.
func TestRegression_CounterSeededAfterReload(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	// persist ids 1, 2, 3
	resetRegistries()
	Register[bqItem]()
	db := Open[bqItem]()
	for i := 0; i < 3; i++ {
		db.AddToPersistQueue(&bqItem{Name: "seed"})
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("seed flush: %v", err)
	}

	// reload from disk, insert one more
	resetRegistries()
	Register[bqItem]()
	db2 := Open[bqItem]()

	fresh := &bqItem{Name: "after-reload"}
	db2.AddToPersistQueue(fresh)
	if fresh.Id != 4 {
		t.Fatalf("after reload, next id = %d, want 4 (counter must seed from max persisted id)", fresh.Id)
	}

	if err := db2.Flush(); err != nil {
		t.Fatalf("post-reload flush: %v", err)
	}

	if got := db2.Len(); got != 4 {
		t.Errorf("expected 4 entities after reload+insert, got %d", got)
	}
}

// Bug: save() wrote directly to the target file, so a crash mid-encode corrupted
// it. Fix: write to .tmp, fsync, rename.
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

	db.AddToPersistQueue(&bqItem{Name: "ok"})
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Only .gob chunk files should remain, no .tmp.
	typeDir := filepath.Join(tmpDir, "bqItem")
	entries, err := os.ReadDir(typeDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("leftover tmp file after successful save: %s", e.Name())
		}
	}

	finalPath := filepath.Join(typeDir, "0.gob")
	if _, err := os.Stat(finalPath); err != nil {
		t.Errorf("final chunk file missing: %v", err)
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

	db.AddToPersistQueue(&bqItem{Name: "v1"})
	if err := db.Flush(); err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	typeDir := filepath.Join(tmpDir, "bqItem")
	finalPath := filepath.Join(typeDir, "0.gob")
	infoV1, err := os.Stat(finalPath)
	if err != nil {
		t.Fatalf("stat v1: %v", err)
	}

	// v2 — both rows live in chunk 0, rewritten in place.
	db.AddToPersistQueue(&bqItem{Name: "v2"})
	if err := db.Flush(); err != nil {
		t.Fatalf("second Flush: %v", err)
	}

	entries, _ := os.ReadDir(typeDir)
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
		t.Logf("v1 size=%d, v2 size=%d (informational)", infoV1.Size(), infoV2.Size())
	}
}
