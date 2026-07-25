package compare

// The flat-record workloads measure raw engine overhead on a shape where
// nestory has no structural advantage. This file measures the shape where it
// claims one: an ownership graph, loaded and extended by traversal rather than
// by join. A workspace owns projects, a project owns documents; the workload is
// "give me one workspace with everything it owns", which every other engine has
// to reassemble from rows.

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/DorianDevp/nestory"
	"github.com/hashicorp/go-memdb"
	_ "modernc.org/sqlite"
)

type Workspace struct {
	Id       int
	Name     string
	Projects []*Project
}

type Project struct {
	Id          int
	WorkspaceID int
	Name        string
	Documents   []*Document
}

type Document struct {
	Id        int
	ProjectID int
	Title     string
}

// The scale comes from the environment, one value per process: nestory
// registers types process-globally, so a second configuration in the same
// process is not possible, and running each engine in its own process is what
// keeps one engine's heap and cache state out of another's measurement.
//
// graphShape is the second axis. "single" puts the whole project under one
// root — the largest branch that total allows — while "wide" spreads it over
// five-node roots. Real workloads sit between: one tenant owning an enormous
// workspace, or a table of many small ones.
type graphShape struct {
	mode       string
	workspaces int
	projects   int
	documents  int
}

func (shape graphShape) branchNodes() int {
	return 1 + shape.projects + shape.projects*shape.documents
}

func (shape graphShape) totalNodes() int { return shape.workspaces * shape.branchNodes() }

func (shape graphShape) String() string {
	return fmt.Sprintf("%s/n=%d/branch=%d", shape.mode, shape.totalNodes(), shape.branchNodes())
}

// benchShape reads the one configuration this process measures.
func benchShape(tb testing.TB) graphShape {
	tb.Helper()

	total := 1000
	if raw := os.Getenv("NESTORY_BENCH_SCALE"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			tb.Fatalf("NESTORY_BENCH_SCALE: %v", err)
		}

		total = parsed
	}

	if os.Getenv("NESTORY_BENCH_SHAPE") == "wide" {
		if total < 5 {
			tb.Skipf("wide needs at least five nodes, got %d", total)
		}

		return graphShape{mode: "wide", workspaces: total / 5, projects: 2, documents: 1}
	}

	return singleRootShape(total)
}

// singleRootShape splits one branch evenly between breadth and depth, so
// neither dimension alone explains a result.
func singleRootShape(total int) graphShape {
	if total <= 1 {
		return graphShape{mode: "single", workspaces: 1}
	}

	projects := 1
	for (projects+1)+(projects+1)*(projects+1) <= total-1 {
		projects++
	}

	return graphShape{
		mode: "single", workspaces: 1,
		projects: projects, documents: (total - 1 - projects) / projects,
	}
}

var graphSink int

// SQLite: the branch is three indexed queries, and the rows have to be
// assembled into objects on every read.

func openGraphSQLite(tb testing.TB, dir string, shape graphShape) *sql.DB {
	tb.Helper()

	db, err := sql.Open("sqlite", filepath.Join(dir, "graph.db"))
	if err != nil {
		tb.Fatalf("open: %v", err)
	}

	schema := []string{
		`PRAGMA synchronous=FULL`,
		`CREATE TABLE workspace(id INTEGER PRIMARY KEY, name TEXT)`,
		`CREATE TABLE project(id INTEGER PRIMARY KEY, workspace_id INTEGER, name TEXT)`,
		`CREATE TABLE document(id INTEGER PRIMARY KEY, project_id INTEGER, title TEXT)`,
		`CREATE INDEX project_workspace ON project(workspace_id)`,
		`CREATE INDEX document_project ON document(project_id)`,
	}
	for _, statement := range schema {
		if _, err := db.Exec(statement); err != nil {
			tb.Fatalf("schema: %v", err)
		}
	}

	tx, _ := db.Begin()
	projectID, documentID := 0, 0
	for workspace := 1; workspace <= shape.workspaces; workspace++ {
		if _, err := tx.Exec(`INSERT INTO workspace VALUES(?,?)`, workspace, fmt.Sprintf("ws-%d", workspace)); err != nil {
			tb.Fatalf("insert: %v", err)
		}

		for range shape.projects {
			projectID++
			if _, err := tx.Exec(`INSERT INTO project VALUES(?,?,?)`, projectID, workspace, "proj"); err != nil {
				tb.Fatalf("insert: %v", err)
			}

			for range shape.documents {
				documentID++
				if _, err := tx.Exec(`INSERT INTO document VALUES(?,?,?)`, documentID, projectID, "doc"); err != nil {
					tb.Fatalf("insert: %v", err)
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		tb.Fatalf("commit: %v", err)
	}

	return db
}

func BenchmarkGraphLoad_SQLite(b *testing.B) {
	shape := benchShape(b)
	db := openGraphSQLite(b, b.TempDir(), shape)
	defer db.Close()

	target := max(shape.workspaces/2, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		workspace := &Workspace{}
		if err := db.QueryRow(`SELECT id,name FROM workspace WHERE id=?`, target).
			Scan(&workspace.Id, &workspace.Name); err != nil {
			b.Fatalf("workspace: %v", err)
		}

		rows, err := db.Query(`SELECT id,workspace_id,name FROM project WHERE workspace_id=?`, target)
		if err != nil {
			b.Fatalf("projects: %v", err)
		}

		for rows.Next() {
			project := &Project{}
			if err := rows.Scan(&project.Id, &project.WorkspaceID, &project.Name); err != nil {
				b.Fatalf("scan: %v", err)
			}

			workspace.Projects = append(workspace.Projects, project)
		}

		rows.Close()

		for _, project := range workspace.Projects {
			documents, err := db.Query(`SELECT id,project_id,title FROM document WHERE project_id=?`, project.Id)
			if err != nil {
				b.Fatalf("documents: %v", err)
			}

			for documents.Next() {
				document := &Document{}
				if err := documents.Scan(&document.Id, &document.ProjectID, &document.Title); err != nil {
					b.Fatalf("scan: %v", err)
				}

				project.Documents = append(project.Documents, document)
			}

			documents.Close()
			graphSink += len(project.Documents)
		}
	}
}

// go-memdb: objects are already objects, but the branch still has to be found
// through one index lookup per level.

func graphSchema() *memdb.DBSchema {
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"workspace": {Name: "workspace", Indexes: map[string]*memdb.IndexSchema{
				"id": {Name: "id", Unique: true, Indexer: &memdb.IntFieldIndex{Field: "Id"}},
			}},
			"project": {Name: "project", Indexes: map[string]*memdb.IndexSchema{
				"id":        {Name: "id", Unique: true, Indexer: &memdb.IntFieldIndex{Field: "Id"}},
				"workspace": {Name: "workspace", Indexer: &memdb.IntFieldIndex{Field: "WorkspaceID"}},
			}},
			"document": {Name: "document", Indexes: map[string]*memdb.IndexSchema{
				"id":      {Name: "id", Unique: true, Indexer: &memdb.IntFieldIndex{Field: "Id"}},
				"project": {Name: "project", Indexer: &memdb.IntFieldIndex{Field: "ProjectID"}},
			}},
		},
	}
}

func openGraphMemdb(tb testing.TB, shape graphShape) *memdb.MemDB {
	tb.Helper()

	db, err := memdb.NewMemDB(graphSchema())
	if err != nil {
		tb.Fatalf("memdb: %v", err)
	}

	txn := db.Txn(true)
	projectID, documentID := 0, 0
	for workspace := 1; workspace <= shape.workspaces; workspace++ {
		if err := txn.Insert("workspace", &Workspace{Id: workspace, Name: "ws"}); err != nil {
			tb.Fatalf("insert: %v", err)
		}

		for range shape.projects {
			projectID++
			if err := txn.Insert("project", &Project{Id: projectID, WorkspaceID: workspace, Name: "proj"}); err != nil {
				tb.Fatalf("insert: %v", err)
			}

			for range shape.documents {
				documentID++
				if err := txn.Insert("document", &Document{Id: documentID, ProjectID: projectID, Title: "doc"}); err != nil {
					tb.Fatalf("insert: %v", err)
				}
			}
		}
	}

	txn.Commit()

	return db
}

func BenchmarkGraphLoad_Memdb(b *testing.B) {
	shape := benchShape(b)
	db := openGraphMemdb(b, shape)
	target := max(shape.workspaces/2, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		txn := db.Txn(false)
		raw, err := txn.First("workspace", "id", target)
		if err != nil || raw == nil {
			b.Fatalf("workspace: %v", err)
		}

		projects, err := txn.Get("project", "workspace", target)
		if err != nil {
			b.Fatalf("projects: %v", err)
		}

		for object := projects.Next(); object != nil; object = projects.Next() {
			project := object.(*Project)
			documents, err := txn.Get("document", "project", project.Id)
			if err != nil {
				b.Fatalf("documents: %v", err)
			}

			for document := documents.Next(); document != nil; document = documents.Next() {
				graphSink += document.(*Document).Id
			}
		}

		txn.Abort()
	}
}

// nestory: the branch is already assembled, so reading it is pointer
// traversal. View is the safe path — one branch read lock; Unsafe is the
// exclusive-access floor, and the gap between them is what isolation costs.

type NWorkspace struct {
	Id       int `key:"primary"`
	Name     string
	Projects []*NProject `rel:"own,WorkspaceID"`
}

func (w NWorkspace) GetId() int { return w.Id }

type NProject struct {
	Id          int `key:"primary"`
	WorkspaceID int
	Name        string
	Workspace   *NWorkspace  `rel:"ownedby,Id"`
	Documents   []*NDocument `rel:"own,ProjectID"`
}

func (p NProject) GetId() int { return p.Id }

type NDocument struct {
	Id        int `key:"primary"`
	ProjectID int
	Title     string
	Project   *NProject `rel:"ownedby,Id"`
}

func (d NDocument) GetId() int { return d.Id }

// Go calls a benchmark function repeatedly while it calibrates b.N, and
// nestory registers types process-globally, so the fixture is built exactly
// once per process — which is also the isolation the runner relies on.
var (
	nestoryGraphOnce sync.Once
	nestoryGraphDB   *nestory.DB[NWorkspace]
)

func openGraphNestory(tb testing.TB, shape graphShape) *nestory.DB[NWorkspace] {
	tb.Helper()

	nestoryGraphOnce.Do(func() { nestoryGraphDB = buildGraphNestory(tb, shape) })
	if nestoryGraphDB == nil {
		tb.Fatal("nestory graph fixture was not built")
	}

	return nestoryGraphDB
}

func buildGraphNestory(tb testing.TB, shape graphShape) *nestory.DB[NWorkspace] {
	tb.Helper()

	directory, err := os.MkdirTemp("", "nestory-graph")
	if err != nil {
		tb.Fatalf("tempdir: %v", err)
	}

	tb.Cleanup(func() { _ = os.RemoveAll(directory) })
	nestory.DataDir = directory
	for _, register := range []func() error{
		nestory.Register[NDocument], nestory.Register[NProject], nestory.Register[NWorkspace],
	} {
		if err := register(); err != nil {
			tb.Fatalf("register: %v", err)
		}
	}

	documents := nestory.Open[NDocument]()
	projects := nestory.Open[NProject]()
	workspaces := nestory.Open[NWorkspace]()

	for range shape.workspaces {
		workspace := &NWorkspace{Name: "ws"}
		workspaces.Unsafe().Create(workspace)
		for range shape.projects {
			project := &NProject{WorkspaceID: workspace.Id, Name: "proj", Workspace: workspace}
			projects.Unsafe().Create(project)
			workspace.Projects = append(workspace.Projects, project)
			for range shape.documents {
				document := &NDocument{ProjectID: project.Id, Title: "doc", Project: project}
				documents.Unsafe().Create(document)
				project.Documents = append(project.Documents, document)
			}
		}
	}

	if err := workspaces.Unsafe().Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}

	return workspaces
}

func BenchmarkGraphLoad_Nestory(b *testing.B) {
	shape := benchShape(b)
	workspaces := openGraphNestory(b, shape)
	target := max(shape.workspaces/2, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := workspaces.View(target, func(workspace *NWorkspace) error {
			for _, project := range workspace.Projects {
				for _, document := range project.Documents {
					graphSink += document.Id
				}
			}

			return nil
		}); err != nil {
			b.Fatalf("view: %v", err)
		}
	}
}

func BenchmarkGraphLoad_NestoryUnsafe(b *testing.B) {
	shape := benchShape(b)
	workspaces := openGraphNestory(b, shape)
	target := max(shape.workspaces/2, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		workspace, err := workspaces.Unsafe().Get(target)
		if err != nil {
			b.Fatalf("get: %v", err)
		}

		for _, project := range workspace.Projects {
			for _, document := range project.Documents {
				graphSink += document.Id
			}
		}
	}
}
