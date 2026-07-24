package nestory

import (
	"encoding/json"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
)

const towerMemoryWorkerEnv = "NESTORY_TOWER_MEMORY_WORKER"

// towerProfileChild carries a payload because the replica clones slice fields
// as well as struct headers — a shadow node is a whole second record, not a
// pointer to the first one.
type towerProfileChild struct {
	Id      int `key:"primary"`
	OwnerID int
	Kind    string
	Data    []byte
}

func (child towerProfileChild) GetId() int { return child.Id }

type towerProfileOwner struct {
	Id       int `key:"primary"`
	Name     string
	Children []*towerProfileChild `rel:"own,OwnerID"`
}

func (owner towerProfileOwner) GetId() int { return owner.Id }

// TestTowerMemoryProfile prices the Tower replica: the graph alone, then the
// same graph after one UpdateWithin has built the shadow. The difference is
// what a project pays, permanently, for the flat-allocation write path.
func TestTowerMemoryProfile(t *testing.T) {
	if worker := os.Getenv(towerMemoryWorkerEnv); worker != "" {
		runTowerMemoryWorker(t, worker)

		return
	}

	if os.Getenv(memoryProfileRunEnv) == "" {
		t.Skip("set NESTORY_MEMORY_PROFILE=1 to run the Tower memory profile")
	}

	graph := towerMemorySubprocess(t, "graph")
	replica := towerMemorySubprocess(t, "replica")

	entries := uint64(graph.Entries)
	added := positiveDifference(replica.HeapAlloc, graph.HeapAlloc)

	t.Log("variant\tbytes/node\tlive heap\theap in-use\tRSS")
	t.Logf(
		"graph\t%d\t%s\t%s\t%s",
		graph.HeapAlloc/entries,
		formatBytes(graph.HeapAlloc), formatBytes(graph.HeapInuse), formatBytes(graph.RSS),
	)
	t.Logf(
		"graph+replica\t%d\t%s\t%s\t%s",
		replica.HeapAlloc/entries,
		formatBytes(replica.HeapAlloc), formatBytes(replica.HeapInuse), formatBytes(replica.RSS),
	)
	t.Logf(
		"replica adds %s (%d B/node, +%.0f%% live heap, RSS %s to %s)",
		formatBytes(added),
		added/entries,
		100*float64(added)/float64(graph.HeapAlloc),
		formatBytes(graph.RSS), formatBytes(replica.RSS),
	)
}

func towerMemorySubprocess(t *testing.T, variant string) memoryProfileSample {
	t.Helper()

	previous, had := os.LookupEnv(memoryProfileWorkerEnv)
	_ = os.Unsetenv(memoryProfileWorkerEnv)
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(memoryProfileWorkerEnv, previous)
		}
	})

	_ = os.Setenv(towerMemoryWorkerEnv, variant)
	defer os.Unsetenv(towerMemoryWorkerEnv)

	return memoryProfileRun(t, "TestTowerMemoryProfile", variant)
}

func runTowerMemoryWorker(t *testing.T, variant string) {
	t.Helper()

	children := memoryProfileIntEnv("NESTORY_TOWER_MEMORY_CHILDREN", 100_000)
	payloadSize := memoryProfileIntEnv("NESTORY_MEMORY_PAYLOAD_BYTES", 128)

	DataDir = t.TempDir()
	resetRegistries()
	registerForTest[towerProfileChild](t)
	registerForTest[towerProfileOwner](t)

	childDB := Open[towerProfileChild]()
	ownerDB := Open[towerProfileOwner]()
	owner := &towerProfileOwner{
		Name:     "root",
		Children: make([]*towerProfileChild, 0, children),
	}
	ownerDB.Unsafe().Create(owner)
	for position := range children {
		payload := make([]byte, payloadSize)
		if len(payload) > 0 {
			payload[0] = byte(position)
		}

		child := &towerProfileChild{
			OwnerID: owner.Id,
			Kind:    "assistant",
			Data:    payload,
		}
		childDB.Unsafe().Create(child)
		owner.Children = append(owner.Children, child)
	}

	if err := ownerDB.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}

	if variant == "replica" {
		// One scalar root write is enough to build and retain the whole shadow.
		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *towerProfileOwner) error {
			shadow.Name = "written"
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if projectTower.replica.Load() == nil {
			t.Fatal("expected a live Tower replica")
		}
	}

	debug.FreeOSMemory()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	sample := memoryProfileSample{
		Variant:      variant,
		Entries:      children + 1,
		PayloadBytes: uint64(children * payloadSize),
		HeapAlloc:    stats.HeapAlloc,
		HeapInuse:    stats.HeapInuse,
		RSS:          processRSS(),
	}
	runtime.KeepAlive(ownerDB)
	runtime.KeepAlive(childDB)

	encoded, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("NESTORY_MEMORY_SAMPLE=%s", encoded)
}
