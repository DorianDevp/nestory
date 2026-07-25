package nestory

import (
	"strconv"
	"testing"
)

// The raw-Go baselines answer a question the cross-engine tables cannot: how
// much of nestory's lead is its data model, and how much is simply the absence
// of overhead any plain slice would also lack. A slice and a map give up
// everything — durability, locking, versioning, relations, validation — so the
// gap between them and nestory is the price of being a database, isolated from
// the price of being a generic KV store.

type baselineRow struct {
	Id    int
	Name  string
	Email string
	Age   int
}

func seedBaseline(n int) ([]baselineRow, map[int]*baselineRow) {
	rows := make([]baselineRow, n)
	index := make(map[int]*baselineRow, n)
	for position := range rows {
		rows[position] = baselineRow{
			Id:    position + 1,
			Name:  "user-" + strconv.Itoa(position+1),
			Email: "user-" + strconv.Itoa(position+1) + "@example.com",
			Age:   (position + 1) % 90,
		}
		index[position+1] = &rows[position]
	}

	return rows, index
}

var baselineSink int

func BenchmarkBaselineMapGet(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			_, index := seedBaseline(n)
			target := n / 2
			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				baselineSink += index[target].Age
			}
		})
	}
}

func BenchmarkBaselineSliceWrite(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			_, index := seedBaseline(n)
			target := n / 2
			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				index[target].Age = iteration % 90
			}
		})
	}
}

// BenchmarkBaselineScan and BenchmarkFilterAtScale share sizes so the two can
// be read as one curve. 10,000 rows is 560 kB and lives in L2; the larger sizes
// leave every cache level, which is where a contiguous layout has to prove it
// still helps.
var scanScales = []int{10_000, 100_000, 1_000_000, 5_000_000}

func BenchmarkBaselineScan(b *testing.B) {
	for _, n := range scanScales {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			rows, _ := seedBaseline(n)
			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				matches := 0
				for position := range rows {
					if rows[position].Age == 42 {
						matches++
					}
				}

				baselineSink += matches
			}
		})
	}
}

// BenchmarkFilterAtScale runs nestory's Filter past the point where the data
// stops fitting in cache. The published 214 M rows/s came from a 10,000-row
// set — 560 kB, comfortably resident — so it measured cache-warm throughput,
// not the storage layout's asymptote.
func BenchmarkFilterAtScale(b *testing.B) {
	for _, n := range scanScales {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			quiet(b)
			DataDir = b.TempDir()
			resetRegistries()
			if err := Register[benchItem](); err != nil {
				b.Fatal(err)
			}

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
				baselineSink += len(db.Filter(func(item benchItem) bool { return item.Age == 42 }))
			}
		})
	}
}
