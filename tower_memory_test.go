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

	// Cumulative: each variant keeps one more part of the replica than the last,
	// so the delta between two rows is what that part costs.
	variants := []string{"graph", "shadow", "walk", "relations", "replica"}
	samples := make([]memoryProfileSample, 0, len(variants))
	for _, variant := range variants {
		samples = append(samples, towerMemorySubprocess(t, variant))
	}

	entries := uint64(samples[0].Entries)
	t.Log("component\tbytes/node\tlive heap\tRSS")
	previous := samples[0]
	t.Logf(
		"graph (no replica)\t-\t%s\t%s",
		formatBytes(previous.HeapAlloc), formatBytes(previous.RSS),
	)
	for position := 1; position < len(samples); position++ {
		current := samples[position]
		delta := positiveDifference(current.HeapAlloc, previous.HeapAlloc)
		t.Logf(
			"+ %s\t%d\t%s\t%s",
			current.Variant,
			delta/entries,
			formatBytes(current.HeapAlloc),
			formatBytes(current.RSS),
		)
		previous = current
	}

	full := samples[len(samples)-1]
	added := positiveDifference(full.HeapAlloc, samples[0].HeapAlloc)
	t.Logf(
		"replica total: %s (%d B/node, +%.0f%% live heap, RSS %s to %s)",
		formatBytes(added),
		added/entries,
		100*float64(added)/float64(samples[0].HeapAlloc),
		formatBytes(samples[0].RSS), formatBytes(full.RSS),
	)
}

// pruneTowerReplica drops the parts of the replica a variant is not paying for.
// Dropping nodes keeps blocks, so the shadow storage stays retained either way;
// what goes is the index over it.
func pruneTowerReplica(replica *towerReplica, variant string) {
	switch variant {
	case "shadow":
		replica.walk = nil
		replica.relations = nil
		replica.branches = nil
	case "walk":
		replica.relations = nil
		replica.branches = nil
	case "relations":
		replica.branches = nil
	case "replica":
	default:
		panic("unknown tower memory variant: " + variant)
	}
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

	if variant != "graph" {
		// One scalar root write is enough to build and retain the whole shadow.
		if err := ownerDB.UpdateWithin(owner.Id, func(shadow *towerProfileOwner) error {
			shadow.Name = "written"
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		replica := projectTower.replica.Load()
		if replica == nil {
			t.Fatal("expected a live Tower replica")
		}

		pruneTowerReplica(replica, variant)
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
