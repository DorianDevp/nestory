package nestory

import (
	"fmt"
	"strconv"
	"testing"
)

// BenchmarkColdStart prices the number the comparison reports never had: how
// long a process waits before its first query, given data already on disk.
// Every iteration drops the in-memory registries and rehydrates from the same
// files — Register reads the store back, Open rebuilds indices, resById and
// the id counter. The Get at the end proves the reload actually served a row.
func BenchmarkColdStart(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("flat/n=%d", n), func(b *testing.B) {
			quiet(b)
			DataDir = b.TempDir()
			resetRegistries()
			registerForBench(b)
			db := Open[benchItem]()
			for position := 1; position <= n; position++ {
				db.Unsafe().Create(&benchItem{
					Name:  "user-" + strconv.Itoa(position),
					Email: "user-" + strconv.Itoa(position) + "@example.com",
					Age:   position % 90,
				})
			}
			if err := db.Unsafe().Flush(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				resetRegistries()
				registerForBench(b)
				reopened := Open[benchItem]()
				if _, err := reopened.Unsafe().Get(n / 2); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	b.Run("graph/n=100003", func(b *testing.B) {
		quiet(b)
		DataDir = b.TempDir()
		resetRegistries()
		seedStartupGraph(b, 10_000)

		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			resetRegistries()
			registerStartupGraph(b)
			workspaces := Open[memWorkspace]()
			Open[memProject]()
			Open[memDocument]()
			Open[memLabel]()
			Open[memTier]()
			workspace, err := workspaces.Unsafe().Get(1)
			if err != nil {
				b.Fatal(err)
			}

			if len(workspace.Projects) == 0 {
				b.Fatal("reload lost the relation wiring")
			}
		}
	})
}

func registerForBench(b *testing.B) {
	b.Helper()

	if err := Register[benchItem](); err != nil {
		b.Fatal(err)
	}
}

func registerStartupGraph(b *testing.B) {
	b.Helper()

	for _, register := range []func() error{
		Register[memTier], Register[memDocument], Register[memProject],
		Register[memLabel], Register[memWorkspace],
	} {
		if err := register(); err != nil {
			b.Fatal(err)
		}
	}
}

func seedStartupGraph(b *testing.B, units int) {
	b.Helper()

	registerStartupGraph(b)

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

	for unit := range units {
		workspace := &memWorkspace{Name: "ws-" + strconv.Itoa(unit)}
		workspaceDB.Unsafe().Create(workspace)

		label := &memLabel{
			WorkspaceID: workspace.Id, Name: "label-" + strconv.Itoa(unit),
			Workspace: workspace, Tier: tiers[unit%len(tiers)],
		}
		labelDB.Unsafe().Create(label)
		workspace.Labels = append(workspace.Labels, label)

		for range 2 {
			project := &memProject{
				WorkspaceID: workspace.Id,
				Name:        "proj-" + strconv.Itoa(unit),
				Workspace:   workspace,
			}
			projectDB.Unsafe().Create(project)
			workspace.Projects = append(workspace.Projects, project)

			for offset := range 3 {
				document := &memDocument{
					ProjectID: project.Id, Title: "doc-" + strconv.Itoa(offset),
					Project: project, Tier: tiers[(unit+offset)%len(tiers)],
				}
				documentDB.Unsafe().Create(document)
				project.Documents = append(project.Documents, document)
			}
		}
	}

	if err := workspaceDB.Unsafe().Flush(); err != nil {
		b.Fatal(err)
	}
}
