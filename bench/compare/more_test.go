package compare

// More competitors, same shape/workloads as compare_test.go: tidwall/buntdb
// (in-memory + AOF durable) and dgraph-io/badger/v4 (LSM, durable).

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/tidwall/buntdb"
)

// buntdb (in-memory + AOF durable)

func openBunt(tb testing.TB, dir string) *buntdb.DB {
	tb.Helper()
	db, err := buntdb.Open(filepath.Join(dir, "rec.db"))
	if err != nil {
		tb.Fatalf("open buntdb: %v", err)
	}
	var cfg buntdb.Config
	if err := db.ReadConfig(&cfg); err != nil {
		tb.Fatalf("buntdb config: %v", err)
	}
	cfg.SyncPolicy = buntdb.Always // fsync per commit, matching SQLite FULL / bbolt
	if err := db.SetConfig(cfg); err != nil {
		tb.Fatalf("buntdb set config: %v", err)
	}
	return db
}

func seedBunt(tb testing.TB, db *buntdb.DB, n int) {
	tb.Helper()
	err := db.Update(func(tx *buntdb.Tx) error {
		for j := 1; j <= n; j++ {
			if _, _, err := tx.Set(strconv.Itoa(j), string(enc(mkRec(j))), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("seed buntdb: %v", err)
	}
}

func BenchmarkBunt_BulkInsert(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := openBunt(b, b.TempDir())
				b.StartTimer()
				seedBunt(b, db, n)
				db.Close()
			}
		})
	}
}

func BenchmarkBunt_PointRead(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openBunt(b, b.TempDir())
			seedBunt(b, db, n)
			defer db.Close()
			target := strconv.Itoa(n / 2)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.View(func(tx *buntdb.Tx) error {
					if v, err := tx.Get(target); err == nil {
						sink += int64(dec([]byte(v)).Age)
					}
					return nil
				})
			}
		})
	}
}

func BenchmarkBunt_Scan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openBunt(b, b.TempDir())
			seedBunt(b, db, n)
			defer db.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.View(func(tx *buntdb.Tx) error {
					return tx.Ascend("", func(k, v string) bool {
						if r := dec([]byte(v)); r.Age == 42 {
							sink += int64(r.Id)
						}
						return true
					})
				})
			}
		})
	}
}

func BenchmarkBunt_PointWrite(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openBunt(b, b.TempDir())
			seedBunt(b, db, n)
			defer db.Close()
			target := strconv.Itoa(n / 2)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.Update(func(tx *buntdb.Tx) error {
					r := mkRec(n / 2)
					r.Age = i % 90
					_, _, err := tx.Set(target, string(enc(r)), nil)
					return err
				})
			}
		})
	}
}

// badger (LSM, durable)

func openBadger(tb testing.TB, dir string) *badger.DB {
	tb.Helper()
	opts := badger.DefaultOptions(dir).WithSyncWrites(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		tb.Fatalf("open badger: %v", err)
	}
	return db
}

// seedBadger uses a WriteBatch — badger caps a single transaction's size, so
// this is its idiomatic durable bulk path.
func seedBadger(tb testing.TB, db *badger.DB, n int) {
	tb.Helper()
	wb := db.NewWriteBatch()
	for j := 1; j <= n; j++ {
		if err := wb.Set(itob(j), enc(mkRec(j))); err != nil {
			tb.Fatalf("badger batch set: %v", err)
		}
	}
	if err := wb.Flush(); err != nil {
		tb.Fatalf("badger batch flush: %v", err)
	}
}

func BenchmarkBadger_BulkInsert(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := openBadger(b, b.TempDir())
				b.StartTimer()
				seedBadger(b, db, n)
				b.StopTimer()
				db.Close()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkBadger_PointRead(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openBadger(b, b.TempDir())
			seedBadger(b, db, n)
			defer db.Close()
			target := itob(n / 2)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(target)
					if err != nil {
						return nil
					}
					return item.Value(func(v []byte) error {
						sink += int64(dec(v).Age)
						return nil
					})
				})
			}
		})
	}
}

func BenchmarkBadger_Scan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openBadger(b, b.TempDir())
			seedBadger(b, db, n)
			defer db.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.View(func(txn *badger.Txn) error {
					it := txn.NewIterator(badger.DefaultIteratorOptions)
					defer it.Close()
					for it.Rewind(); it.Valid(); it.Next() {
						_ = it.Item().Value(func(v []byte) error {
							if r := dec(v); r.Age == 42 {
								sink += int64(r.Id)
							}
							return nil
						})
					}
					return nil
				})
			}
		})
	}
}

func BenchmarkBadger_PointWrite(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openBadger(b, b.TempDir())
			seedBadger(b, db, n)
			defer db.Close()
			target := itob(n / 2)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.Update(func(txn *badger.Txn) error {
					r := mkRec(n / 2)
					r.Age = i % 90
					return txn.Set(target, enc(r))
				})
			}
		})
	}
}
