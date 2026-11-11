package nestory

import (
	"encoding/json"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
)

const graphMemoryWorkerEnv = "NESTORY_GRAPH_MEMORY_WORKER"

// memFlatRow is the control: one type, five fields, no relations. Everything the
// graph schema costs above this row is what relations and the Tower cost.
type memFlatRow struct {
	Id    int `key:"primary"`
	Name  string
	Kind  string
	Seq   int
	Score int
}

func (row memFlatRow) GetId() int { return row.Id }

// The graph schema: five types, two relations each, two levels of nesting under
// two different ownership paths, plus a three-row lookup table that every
// document borrows and every label optionally references.
//
//	memWorkspace ──own──> memProject ──own──> memDocument ──borrow──┐
//	     └────────own──> memLabel ─────────────option──────────────>┤
//	                                                          memTier (3 rows)
type memWorkspace struct {
	Id       int `key:"primary"`
	Name     string
	Projects []*memProject `rel:"own,WorkspaceID"`
	Labels   []*memLabel   `rel:"own,WorkspaceID"`
}

func (workspace memWorkspace) GetId() int { return workspace.Id }

type memProject struct {
	Id          int `key:"primary"`
	WorkspaceID int
	Name        string
	Workspace   *memWorkspace  `rel:"ownedby,Id"`
	Documents   []*memDocument `rel:"own,ProjectID"`
}

func (project memProject) GetId() int { return project.Id }

type memDocument struct {
	Id        int `key:"primary"`
	ProjectID int
	Title     string
	Project   *memProject `rel:"ownedby,Id"`
	Tier      *memTier    `rel:"borrow,Id"`
}

func (document memDocument) GetId() int { return document.Id }

type memLabel struct {
	Id          int `key:"primary"`
	WorkspaceID int
	Name        string
	Workspace   *memWorkspace `rel:"ownedby,Id"`
	Tier        *memTier      `rel:"option,Id"`
}

func (label memLabel) GetId() int { return label.Id }

// memTier never grows. Its two inverse views are computed collections, so the
// profile can price what a three-row table costs when 60,000 documents point at
// it.
type memTier struct {
	Id        int `key:"primary"`
	Name      string
	Documents []*memDocument `rel:"inverse,Tier"`
	Labels    []*memLabel    `rel:"inverse,Tier"`
}

func (tier memTier) GetId() int { return tier.Id }

// graphMemorySample is one measurement point. Peak is sampled while the
// operation's intermediate state is still reachable; churn is the TotalAlloc
// delta across the whole operation, so it counts garbage the peak never shows.
type graphMemorySample struct {
	Schema    string `json:"schema"`
	Phase     string `json:"phase"`
	Nodes     int    `json:"nodes"`
	Roots     int    `json:"roots"`
	HeapAlloc uint64 `json:"heapAlloc"`
	PeakHeap  uint64 `json:"peakHeap"`
	ChurnOp   uint64 `json:"churnOp"`
	RSS       uint64 `json:"rss"`
}

// nodesPerUnit is one workspace, two projects, six documents and one label. The
// shape is held constant at every scale so a size column compares like with like.
const nodesPerUnit = 10

func TestGraphMemoryProfile(t *testing.T) {
	if worker := os.Getenv(graphMemoryWorkerEnv); worker != "" {
		runGraphMemoryWorker(t, worker)

		return
	}

	if os.Getenv(memoryProfileRunEnv) == "" {
		t.Skip("set NESTORY_MEMORY_PROFILE=1 to run the graph memory profile")
	}

	sizes := []int{10, 100, 1_000, 10_000, 100_000}
	for _, schema := range []string{"flat", "graph"} {
		for _, nodes := range sizes {
			for _, sample := range graphMemorySubprocess(t, schema, nodes) {
				t.Logf(
					"%s\tnodes=%d\troots=%d\t%s\tlive=%s\tpeak=%s\tchurn=%d\trss=%s",
					sample.Schema, sample.Nodes, sample.Roots, sample.Phase,
					formatBytes(sample.HeapAlloc), formatBytes(sample.PeakHeap),
					sample.ChurnOp, formatBytes(sample.RSS),
				)
			}
		}
	}
}

func graphMemorySubprocess(t *testing.T, schema string, nodes int) []graphMemorySample {
	t.Helper()

	variant := schema + ":" + strconv.Itoa(nodes)
	_ = os.Setenv(graphMemoryWorkerEnv, variant)
	defer func() { _ = os.Unsetenv(graphMemoryWorkerEnv) }()

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	command := exec.Command(executable, "-test.run=^TestGraphMemoryProfile$", "-test.v")
	command.Env = append(os.Environ(), graphMemoryWorkerEnv+"="+variant)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("graph memory %s: %v\n%s", variant, err, output)
	}

	const marker = "NESTORY_GRAPH_SAMPLE="
	samples := make([]graphMemorySample, 0, 8)
	for _, line := range strings.Split(string(output), "\n") {
		position := strings.Index(line, marker)
		if position < 0 {
			continue
		}

		var sample graphMemorySample
		if err := json.Unmarshal([]byte(line[position+len(marker):]), &sample); err != nil {
			t.Fatalf("decode graph memory %s: %v", variant, err)
		}

		samples = append(samples, sample)
	}

	if len(samples) == 0 {
		t.Fatalf("graph memory %s returned no samples:\n%s", variant, output)
	}

	return samples
}

func runGraphMemoryWorker(t *testing.T, variant string) {
	t.Helper()

	parts := strings.Split(variant, ":")
	if len(parts) != 2 {
		t.Fatalf("malformed graph memory variant %q", variant)
	}

	nodes, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatal(err)
	}

	DataDir = t.TempDir()
	resetRegistries()

	if parts[0] == "flat" {
		runFlatMemoryWorker(t, nodes)

		return
	}

	runRelationMemoryWorker(t, nodes)
}

func runFlatMemoryWorker(t *testing.T, nodes int) {
	t.Helper()

	registerForTest[memFlatRow](t)
	db := Open[memFlatRow]()
	ids := make([]int, 0, nodes)
	for position := range nodes {
		row := &memFlatRow{
			Name:  "row-" + strconv.Itoa(position),
			Kind:  "assistant",
			Seq:   position,
			Score: position % 97,
		}
		db.Unsafe().Create(row)
		ids = append(ids, row.Id)
	}

	if err := db.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}

	emitGraphSample(t, graphMemorySample{
		Schema: "flat", Phase: "built", Nodes: nodes, Roots: nodes,
		HeapAlloc: settledHeap(), RSS: processRSS(),
	})

	// A point write on a relation-free type never touches the Tower. This is the
	// floor every relation-carrying number below should be read against.
	peak, churn := measureOperation(t, 32, func(iteration int) {
		if err := db.UpdateWithin(ids[iteration%len(ids)], func(row *memFlatRow) error {
			row.Name = strconv.Itoa(iteration)

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	emitGraphSample(t, graphMemorySample{
		Schema: "flat", Phase: "write", Nodes: nodes, Roots: nodes,
		HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
	})

	runtime.KeepAlive(db)
}

// relationGraph holds what the phases below need to address the graph by ID.
type relationGraph struct {
	workspaces *DB[memWorkspace]
	documents  *DB[memDocument]
	rootIDs    []int
	anyProject *memProject
	anyTier    *memTier
}

func runRelationMemoryWorker(t *testing.T, nodes int) {
	t.Helper()

	units := max(nodes/nodesPerUnit, 1)
	graph := buildRelationGraph(t, units)
	total := units*nodesPerUnit + 3

	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "built", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), RSS: processRSS(),
	})

	// One write on one root is enough to materialize a replica of the whole
	// project, so this delta is the replica's fixed price.
	if err := graph.workspaces.UpdateWithin(graph.rootIDs[0], func(workspace *memWorkspace) error {
		workspace.Name = "warm"

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "replica", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), RSS: processRSS(),
	})

	measureRelationWrites(t, graph, total, units)

	runtime.KeepAlive(graph.workspaces)
}

func measureRelationWrites(t *testing.T, graph relationGraph, total, units int) {
	t.Helper()

	root := graph.rootIDs[0]
	workspaces := graph.workspaces

	// Same root every time: the replica stays valid, so this is the steady state
	// the Tower was designed for.
	peak, churn := measureOperation(t, 32, func(iteration int) {
		if err := workspaces.UpdateWithin(root, func(workspace *memWorkspace) error {
			workspace.Name = strconv.Itoa(iteration)

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "updatewithin", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
	})

	peak, churn = measureOperation(t, 32, func(iteration int) {
		err := workspaces.Tracked().UpdateWithin(root, func(_ *Writes, workspace *memWorkspace) error {
			workspace.Name = strconv.Itoa(iteration)

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "tracked", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
	})

	peak, churn = measureOperation(t, 32, func(iteration int) {
		err := workspaces.Transaction(func(tx *Tx[memWorkspace]) error {
			return tx.UpdateWithin(root, func(workspace *memWorkspace) error {
				workspace.Name = strconv.Itoa(iteration)

				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "transaction", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
	})

	peak, churn = measureOperation(t, 32, func(iteration int) {
		branch, err := workspaces.Get(root)
		if err != nil {
			t.Fatal(err)
		}

		branch.Name = strconv.Itoa(iteration)
		if err := workspaces.Update(branch); err != nil {
			t.Fatal(err)
		}
	})
	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "getupdate", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
	})

	// A Tower write absorbs its own epoch bumps, so the replica survives it. Any
	// write that does not go through the Tower does not: applyWrite bumps the
	// table epoch and nothing puts it back. Alternating the two APIs on the same
	// root therefore rebuilds a shadow of the complete project on every pair.
	peak, churn = measureOperation(t, 16, func(iteration int) {
		if err := workspaces.UpdateWithin(root, func(workspace *memWorkspace) error {
			workspace.Name = strconv.Itoa(iteration)

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		branch, err := workspaces.Get(root)
		if err != nil {
			t.Fatal(err)
		}

		branch.Name = "mixed" + strconv.Itoa(iteration)
		if err := workspaces.Update(branch); err != nil {
			t.Fatal(err)
		}
	})
	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "mixed-api", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
	})

	// Mutating a field through a pointer kept since build time is what Unsafe's
	// contract allows, and nothing records it. The graph is untouched, so the
	// flush has no model or index work to do, but only a comparison against the
	// shadow can establish that.
	peak, churn = measureOperation(t, 16, func(iteration int) {
		graph.anyProject.Name = "renamed-" + strconv.Itoa(iteration)
		if err := graph.documents.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}
	})
	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "mutate-then-flush", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
	})

	// Same trigger, reached the way an append-heavy workload reaches it: insert a
	// row, then update a root. The insert retires the replica the update needs.
	peak, churn = measureOperation(t, 16, func(iteration int) {
		document := &memDocument{
			ProjectID: graph.anyProject.Id,
			Title:     "appended-" + strconv.Itoa(iteration),
			Project:   graph.anyProject,
			Tier:      graph.anyTier,
		}
		graph.documents.Unsafe().Create(document)
		graph.anyProject.Documents = append(graph.anyProject.Documents, document)
		if err := graph.documents.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		if err := workspaces.UpdateWithin(root, func(workspace *memWorkspace) error {
			workspace.Name = strconv.Itoa(iteration)

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "insert-then-write", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
	})

	// A delete that only a comparison against the shadow can classify: the doomed
	// document stays in its project's live slice, so the flush must discover it,
	// drop it for the survivor and refresh the tier's inverse view. Documents get
	// sequential ids at creation, and measureOperation replays index 0 for its
	// warm-up, so the phase keeps its own counter instead of trusting the index.
	deleted := 0
	deletions := min(16, units*6-4)
	peak, churn = measureOperation(t, deletions, func(int) {
		deleted++
		if err := graph.documents.Unsafe().Delete(deleted); err != nil {
			t.Fatal(err)
		}

		if err := graph.documents.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		if err := workspaces.UpdateWithin(root, func(workspace *memWorkspace) error {
			workspace.Name = "deleted-" + strconv.Itoa(deleted)

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	emitGraphSample(t, graphMemorySample{
		Schema: "graph", Phase: "delete-then-write", Nodes: total, Roots: units,
		HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
	})

	// Rotating roots keeps the replica valid but grows the branch cache, which is
	// materialized per root written and never evicted.
	if len(graph.rootIDs) > 1 {
		peak, churn = measureOperation(t, 32, func(iteration int) {
			id := graph.rootIDs[iteration%len(graph.rootIDs)]
			if err := workspaces.UpdateWithin(id, func(workspace *memWorkspace) error {
				workspace.Name = strconv.Itoa(iteration)

				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
		emitGraphSample(t, graphMemorySample{
			Schema: "graph", Phase: "rotating-roots", Nodes: total, Roots: units,
			HeapAlloc: settledHeap(), PeakHeap: peak, ChurnOp: churn, RSS: processRSS(),
		})
	}
}

func buildRelationGraph(t testing.TB, units int) relationGraph {
	t.Helper()

	registerForTest[memTier](t)
	registerForTest[memDocument](t)
	registerForTest[memProject](t)
	registerForTest[memLabel](t)
	registerForTest[memWorkspace](t)

	tierDB := Open[memTier]()
	documentDB := Open[memDocument]()
	projectDB := Open[memProject]()
	labelDB := Open[memLabel]()
	workspaceDB := Open[memWorkspace]()

	tiers := make([]*memTier, 3)
	for position := range tiers {
		tiers[position] = &memTier{Name: "tier-" + strconv.Itoa(position)}
		tierDB.Unsafe().Create(tiers[position])
	}

	rootIDs := make([]int, 0, units)
	var lastProject *memProject
	for unit := range units {
		workspace := &memWorkspace{Name: "ws-" + strconv.Itoa(unit)}
		workspaceDB.Unsafe().Create(workspace)
		rootIDs = append(rootIDs, workspace.Id)

		label := &memLabel{
			WorkspaceID: workspace.Id,
			Name:        "label-" + strconv.Itoa(unit),
			Workspace:   workspace,
			Tier:        tiers[unit%len(tiers)],
		}
		labelDB.Unsafe().Create(label)
		workspace.Labels = append(workspace.Labels, label)

		for index := range 2 {
			project := &memProject{
				WorkspaceID: workspace.Id,
				Name:        "proj-" + strconv.Itoa(unit) + "-" + strconv.Itoa(index),
				Workspace:   workspace,
			}
			projectDB.Unsafe().Create(project)
			workspace.Projects = append(workspace.Projects, project)
			lastProject = project

			for offset := range 3 {
				document := &memDocument{
					ProjectID: project.Id,
					Title:     "doc-" + strconv.Itoa(offset),
					Project:   project,
					Tier:      tiers[(unit+offset)%len(tiers)],
				}
				documentDB.Unsafe().Create(document)
				project.Documents = append(project.Documents, document)
			}
		}
	}

	if err := workspaceDB.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}

	runtime.KeepAlive(tierDB)
	runtime.KeepAlive(projectDB)
	runtime.KeepAlive(labelDB)

	return relationGraph{
		workspaces: workspaceDB,
		documents:  documentDB,
		rootIDs:    rootIDs,
		anyProject: lastProject,
		anyTier:    tiers[0],
	}
}

// measureOperation reports two different things about one write.
//
// peak is measured with the collector switched off across a single operation, so
// nothing it allocates can be reclaimed underneath the measurement: the
// HeapAlloc delta is exactly that operation's high-water mark. That is the
// number an out-of-memory kill would see.
//
// churn is the TotalAlloc delta per iteration with the collector running
// normally. It counts garbage the peak never shows, so it is what the collector
// has to keep up with under sustained load.
func measureOperation(t *testing.T, iterations int, operation func(int)) (peak, churn uint64) {
	t.Helper()

	// Warm once so first-call lazy state is not charged to the measurement.
	operation(0)

	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for iteration := range iterations {
		operation(iteration)
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	churn = (after.TotalAlloc - before.TotalAlloc) / uint64(iterations)

	runtime.GC()

	previous := debug.SetGCPercent(-1)
	runtime.ReadMemStats(&before)
	operation(iterations)
	runtime.ReadMemStats(&after)
	debug.SetGCPercent(previous)

	return positiveDifference(after.HeapAlloc, before.HeapAlloc), churn
}

// settledHeap reports retained memory after the collector has had a chance to
// release everything unreachable, so a sample measures what is held, not what
// was recently allocated.
func settledHeap() uint64 {
	debug.FreeOSMemory()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	return stats.HeapAlloc
}

func emitGraphSample(t *testing.T, sample graphMemorySample) {
	t.Helper()

	encoded, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("NESTORY_GRAPH_SAMPLE=%s", encoded)
}
