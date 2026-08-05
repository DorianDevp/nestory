package nestory

import "testing"

// BenchmarkGraphNestoryLoadBranch matches bench/compare/relation_test.go: the
// same 200 x 5 x 10 ownership graph, the same "read one workspace with
// everything it owns". Here the branch is already assembled, the read is a
// lookup plus pointer traversal, with nothing to reconstruct.
func BenchmarkGraphNestoryLoadBranch(b *testing.B) {
	const (
		workspaces = 200
		perSpace   = 5
		perProject = 10
	)

	quiet(b)
	DataDir = b.TempDir()
	resetRegistries()
	registerStartupGraph(b)
	tierDB := Open[memTier]()
	documentDB := Open[memDocument]()
	projectDB := Open[memProject]()
	workspaceDB := Open[memWorkspace]()

	tier := &memTier{Name: "tier"}
	tierDB.Unsafe().Create(tier)
	for range workspaces {
		workspace := &memWorkspace{Name: "ws"}
		workspaceDB.Unsafe().Create(workspace)
		for range perSpace {
			project := &memProject{WorkspaceID: workspace.Id, Name: "proj", Workspace: workspace}
			projectDB.Unsafe().Create(project)
			workspace.Projects = append(workspace.Projects, project)
			for range perProject {
				document := &memDocument{ProjectID: project.Id, Title: "doc", Project: project, Tier: tier}
				documentDB.Unsafe().Create(document)
				project.Documents = append(project.Documents, document)
			}
		}
	}

	if err := workspaceDB.Unsafe().Flush(); err != nil {
		b.Fatal(err)
	}

	target := workspaces / 2

	b.Run("View", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := workspaceDB.View(target, func(workspace *memWorkspace) error {
				for _, project := range workspace.Projects {
					for _, document := range project.Documents {
						baselineSink += document.Id
					}
				}

				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	// The same traversal without the safe API's per-row read locks over the
	// branch, to separate the cost of the data model from the cost of isolation.
	b.Run("Unsafe", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			workspace, err := workspaceDB.Unsafe().Get(target)
			if err != nil {
				b.Fatal(err)
			}

			for _, project := range workspace.Projects {
				for _, document := range project.Documents {
					baselineSink += document.Id
				}
			}
		}
	})
}
