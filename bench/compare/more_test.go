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
