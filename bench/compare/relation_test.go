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
	"path/filepath"
	"testing"

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

// graphShape is the fixture every engine below builds: workspaces × projects
// each × documents each. Held constant so the comparison is like for like.
const (
	graphWorkspaces      = 200
	projectsPerWorkspace = 5
	documentsPerProject  = 10
)

var graphSink int

// SQLite: the branch is three indexed queries, and the rows have to be
// assembled into objects on every read.

func openGraphSQLite(tb testing.TB, dir string) *sql.DB {
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
	for workspace := 1; workspace <= graphWorkspaces; workspace++ {
		if _, err := tx.Exec(`INSERT INTO workspace VALUES(?,?)`, workspace, fmt.Sprintf("ws-%d", workspace)); err != nil {
			tb.Fatalf("insert: %v", err)
		}

		for range projectsPerWorkspace {
			projectID++
			if _, err := tx.Exec(`INSERT INTO project VALUES(?,?,?)`, projectID, workspace, "proj"); err != nil {
				tb.Fatalf("insert: %v", err)
			}

			for range documentsPerProject {
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

func BenchmarkGraphSQLite_LoadBranch(b *testing.B) {
	db := openGraphSQLite(b, b.TempDir())
	defer db.Close()

	target := graphWorkspaces / 2
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

func openGraphMemdb(tb testing.TB) *memdb.MemDB {
	tb.Helper()

	db, err := memdb.NewMemDB(graphSchema())
	if err != nil {
		tb.Fatalf("memdb: %v", err)
	}

	txn := db.Txn(true)
	projectID, documentID := 0, 0
	for workspace := 1; workspace <= graphWorkspaces; workspace++ {
		if err := txn.Insert("workspace", &Workspace{Id: workspace, Name: "ws"}); err != nil {
			tb.Fatalf("insert: %v", err)
		}

		for range projectsPerWorkspace {
			projectID++
			if err := txn.Insert("project", &Project{Id: projectID, WorkspaceID: workspace, Name: "proj"}); err != nil {
				tb.Fatalf("insert: %v", err)
			}

			for range documentsPerProject {
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

func BenchmarkGraphMemdb_LoadBranch(b *testing.B) {
	db := openGraphMemdb(b)
	target := graphWorkspaces / 2

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
