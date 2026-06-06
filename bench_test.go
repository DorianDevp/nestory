package nestory

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"
)

type benchItem struct {
	Id    int `key:"primary"`
	Name  string
	Email string
	Age   int
}

func (b benchItem) GetId() int { return b.Id }

// quiet silences nestory's log/stdout chatter for the benchmark.
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

// newBenchDB returns a fresh, flushed DB with n items.
func newBenchDB(tb testing.TB, n int) *DB[benchItem] {
	tb.Helper()
	DataDir = tb.TempDir()
	resetRegistries()
	Register[benchItem]()
	db := Open[benchItem]()
	for i := 0; i < n; i++ {
		db.AddToPersistQueue(&benchItem{
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

// queue n items + one Flush. ns/op is per batch; divide by n for per-item.
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
					db.AddToPersistQueue(it)
				}
				_ = db.Flush()
			}
		})
	}
}

// isolates save() cost: one Flush over an in-memory dataset of size n.
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

// in-memory insert path, no Flush.
func BenchmarkAddToPersistQueue(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("seed=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				db.AddToPersistQueue(&benchItem{Name: "x"})
			}
		})
	}
}

// one durable transactional row update — WAL frame, no chunk rewrite. ~constant in n.
func BenchmarkPointWrite(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			id := n / 2
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.UpdateWithin(id, func(it *benchItem) { it.Age = i % 90 })
			}
		})
	}
}

// FindOneBy on the primary key (hits the Id index).
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

// direct map hit on the Id index.
func BenchmarkIndexLookupID(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			target := n / 2
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.index["Id"][target]
			}
		})
	}
}

// full predicate scan over the store.
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
