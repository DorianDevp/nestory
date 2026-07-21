package nestory

import (
	"sync"
	"testing"
)

// tCounter is scalar-only, so its snapshot copy is fully detached.
type tCounter struct {
	Id int `key:"primary"`
	N  int
}

func (c tCounter) GetId() int { return c.Id }

// Snapshot says N=10, a commit (no Flush) says N=99; after a reload the WAL must
// have replayed it back to 99.
func TestWAL_CommitSurvivesReloadWithoutFlush(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[tCounter](); err != nil {
		t.Fatal(err)
	}

	db := Open[tCounter]()

	c := &tCounter{N: 10}
	db.Unsafe().Create(c)
	if err := db.Unsafe().Flush(); err != nil { // snapshot N=10, WAL empty
		t.Fatal(err)
	}

	id := c.Id

	// no Flush — durability must come from the WAL alone
	if err := db.UpdateWithin(id, func(c *tCounter) error {
		c.N = 99
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// restart: drop memory, reload from disk
	resetRegistries()
	if err := Register[tCounter](); err != nil {
		t.Fatal(err)
	}

	db2 := Open[tCounter]()
	got, err := db2.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	if got.N != 99 {
		t.Fatalf("after reload N=%d, want 99 (WAL not replayed over snapshot)", got.N)
	}
}

// Hammer one row from many goroutines. Every increment must land — a final
// N < total means version validation or commit locking is broken. Run with -race.
func TestUpdateWithinConcurrent(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[tCounter](); err != nil {
		t.Fatal(err)
	}

	db := Open[tCounter]()

	c := &tCounter{}
	db.Unsafe().Create(c)
	if err := db.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}

	id := c.Id

	const goroutines = 8
	const perG = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// UpdateWithin retries ErrConflict internally — a non-nil return is real.
				if err := db.UpdateWithin(id, func(c *tCounter) error {
					c.N++
					return nil
				}); err != nil {
					panic(err)
				}
			}
		}()
	}

	wg.Wait()

	got, err := db.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	if want := goroutines * perG; got.N != want {
		t.Fatalf("counter = %d, want %d (lost updates)", got.N, want)
	}
}

func TestDetachedRootPromotesIntoCommitEngine(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[tCounter](); err != nil {
		t.Fatal(err)
	}

	db := Open[tCounter]()
	counter := &tCounter{N: 1}
	db.Unsafe().Create(counter)
	if err := db.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}

	first, err := db.Get(counter.Id)
	if err != nil {
		t.Fatal(err)
	}

	second, err := db.Get(counter.Id)
	if err != nil {
		t.Fatal(err)
	}

	first.N = 2
	if err := db.Update(first); err != nil {
		t.Fatal(err)
	}

	second.N = 3
	if err := db.Update(second); err != ErrConflict {
		t.Fatalf("stale Update error = %v, want %v", err, ErrConflict)
	}

	if second.N != 2 {
		t.Fatalf("refreshed branch N = %d, want 2", second.N)
	}

	second.N = 3
	if err := db.Update(second); err != nil {
		t.Fatal(err)
	}

	live, err := db.Unsafe().Get(counter.Id)
	if err != nil {
		t.Fatal(err)
	}

	if live.N != 3 {
		t.Fatalf("live N = %d, want 3", live.N)
	}
}
