package nestory

import (
	"strconv"
	"testing"
)

// BenchmarkUnchangedFlush prices the flush gate itself: one scalar mutated
// through a kept pointer, then Flush. The graph never changes shape, so the
// whole cost is the walk that proves it, graphMovedNodes over every node.
func BenchmarkUnchangedFlush(b *testing.B) {
	for _, units := range []int{1_000, 10_000} {
		b.Run("nodes="+strconv.Itoa(units*10), func(b *testing.B) {
			quiet(b)
			DataDir = b.TempDir()
			resetRegistries()
			graph := buildRelationGraph(b, units)
			if err := graph.workspaces.UpdateWithin(graph.rootIDs[0], func(workspace *memWorkspace) error {
				workspace.Name = "warm"
				return nil
			}); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				graph.anyProject.Name = strconv.Itoa(iteration)
				if err := graph.documents.Unsafe().Flush(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
