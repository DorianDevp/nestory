package compare

import (
	"os"
	"testing"
)

// benchTempDirs holds fixture directories that outlive a single benchmark.
// A per-benchmark tb.Cleanup would remove them between the read and the write
// of the same engine, since the fixture is deliberately built once per process.
var benchTempDirs []string

func TestMain(m *testing.M) {
	code := m.Run()
	for _, directory := range benchTempDirs {
		_ = os.RemoveAll(directory)
	}

	os.Exit(code)
}
