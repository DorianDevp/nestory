package nestory

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"
)

// Benchmarks for the CURRENT shape of nestory ([]T, value-backed slice).
// These establish a baseline before any []T -> []*T change, and exercise
// the two things the README cares about: writes (queue + Flush, which is a
// full-file rewrite + fsync) and reads (FindOneBy scan, Index lookup, Filter).
//
// NOTE: nestory logs verbosely on every Flush/merge. quiet() redirects both
// log output and os.Stdout to /dev/null so we measure the DB, not the TTY.

type benchItem struct {
	Id    int `key:"primary"`
	Name  string
	Email string
	Age   int
}

func (b benchItem) GetId() int { return b.Id }

// quiet silences nestory's log.* and fmt.Print* chatter for the duration of
// the (sub-)benchmark, restoring the originals on cleanup.
func quiet(tb testing.TB) {
	tb.Helper()
	oldStdout := os.Stdout
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		tb.Fatalf("open devnull: %v", err)
	}
	os.Stdout = devnull
	log.SetOutput(io.Discard)
	tb.Cleanup(func() {
		os.Stdout = oldStdout
		devnull.Close()
		log.SetOutput(os.Stderr)
	})
}

// newBenchDB returns a fresh, flushed DB pre-populated with n items.
// Population goes through the real write path, so this is itself O(n^2)
// today (AddToPersistQueue rescans for max id on every call).
func newBenchDB(tb testing.TB, n int) *DB[benchItem] {
	tb.Helper()
	DataDir = tb.TempDir()
	resetRegistries()
	Register[benchItem]()
	db := Open[benchItem]()
	for i := 0; i < n; i++ {
		_ = db.AddToPersistQueue(&benchItem{
			Name:  fmt.Sprintf("user-%d", i),
			Email: fmt.Sprintf("user-%d@example.com", i),
			Age:   i % 90,
		})
	}
	if err := db.Flush(); err != nil {
		tb.Fatalf("seed flush: %v", err)
	}
	return db
}

var benchSizes = []int{100, 1000, 10000}

// ---------- WRITES ----------

// BenchmarkBatchInsertFlush measures the cost of queueing n items and doing
// one Flush (the durable write: encode whole dataset + fsync + rename).
// ns/op is per *batch*; divide by n for per-item.
func BenchmarkBatchInsertFlush(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				DataDir = b.TempDir()
				resetRegistries()
				Register[benchItem]()
				db := Open[benchItem]()
				items := make([]*benchItem, n)
				for j := range items {
					items[j] = &benchItem{
						Name:  fmt.Sprintf("user-%d", j),
						Email: fmt.Sprintf("user-%d@example.com", j),
						Age:   j % 90,
					}
				}
				b.StartTimer()

				for _, it := range items {
					_ = db.AddToPersistQueue(it)
				}
				_ = db.Flush()
			}
		})
	}
}

// BenchmarkFlushAtSize isolates the save() cost: one Flush over an already
// in-memory dataset of size n (no inserts timed). Shows write amplification —
// every Flush rewrites the entire file regardless of how much changed.
func BenchmarkFlushAtSize(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.Flush()
			}
		})
	}
}

// BenchmarkAddToPersistQueue isolates the in-memory insert path (no Flush).
// Exposes the O(n) max-id rescan: a single queued insert costs more as the
// queue/dataset grows.
func BenchmarkAddToPersistQueue(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("seed=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.AddToPersistQueue(&benchItem{Name: "x"})
			}
		})
	}
}

// ---------- READS ----------

// BenchmarkFindOneByID measures FindOneBy on the primary key. Note this path
// spawns a goroutine per row, so it is NOT a clean linear scan.
func BenchmarkFindOneByID(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			target := n / 2 // a middle id
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = db.FindOneBy("Id", target)
			}
		})
	}
}

// BenchmarkIndexLookupID is the "intended fast path": direct map hit on the
// Id index. Contrast its ns/op with FindOneByID above.
func BenchmarkIndexLookupID(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			target := n / 2
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.Index["Id"][target]
			}
		})
	}
}

// BenchmarkFilter measures a full predicate scan over the value slice — the
// best case for []T cache locality (sequential, no indirection).
func BenchmarkFilter(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.Filter(func(it benchItem) bool { return it.Age == 42 })
			}
		})
	}
}
