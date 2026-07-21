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
		db.Unsafe().Create(&benchItem{
			Name:  fmt.Sprintf("user-%d", i),
			Email: fmt.Sprintf("user-%d@example.com", i),
			Age:   i % 90,
		})
	}

	if err := db.Unsafe().Flush(); err != nil {
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
					db.Unsafe().Create(it)
				}

				_ = db.Unsafe().Flush()
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
				_ = db.Unsafe().Flush()
			}
		})
	}
}

// Unsafe in-memory insert path, without validation or persistence.
func BenchmarkUnsafeCreate(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("seed=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				db.Unsafe().Create(&benchItem{Name: "x"})
			}
		})
	}
}

// One safe, durable transactional update through the ownership branch.
func BenchmarkPointWrite(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			id := n / 2
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.UpdateWithin(id, func(it *benchItem) error {
					it.Age = i % 90
					return nil
				})
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
				branch, err := db.FindOneBy("Id", target)
				if err != nil {
					b.Fatal(err)
				}

				// A safe Get is a branch lease. Update closes it; with no changes it
				// performs no WAL write.
				if err := db.Update(branch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSafeGetByID(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			target := n / 2
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				branch, err := db.Get(target)
				if err != nil {
					b.Fatal(err)
				}

				if err := db.Update(branch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnsafeGetByID(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			target := n / 2
			unsafe := db.Unsafe()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				item, err := unsafe.Get(target)
				if err != nil {
					b.Fatal(err)
				}

				if item.Id != target {
					b.Fatal("wrong entity")
				}
			}
		})
	}
}

func BenchmarkUnsafePointMutation(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			target := n / 2
			unsafe := db.Unsafe()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				item, err := unsafe.Get(target)
				if err != nil {
					b.Fatal(err)
				}

				item.Age = i % 90
			}
		})
	}
}

func BenchmarkUnsafePointMutationFlush(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			quiet(b)
			db := newBenchDB(b, n)
			target := n / 2
			unsafe := db.Unsafe()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				item, err := unsafe.Get(target)
				if err != nil {
					b.Fatal(err)
				}

				item.Age = i % 90
				if err := unsafe.Flush(); err != nil {
					b.Fatal(err)
				}
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
