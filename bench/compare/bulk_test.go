package compare

// Bulk insert: N rows in one batch or transaction, every engine, one process
// per player.
//
// Every engine starts each iteration from an empty dataset, so the timed work
// is always "load N rows into nothing". The engines that can be torn down and
// rebuilt are; nestory cannot — registration is process-global — so it deletes
// what it just wrote instead. Either way the reset happens outside the timer.

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/DorianDevp/nestory"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/buntdb"
	bolt "go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// bulkBatch returns the batch size and a fresh id offset per iteration.
func bulkBatch(tb testing.TB) int { return benchRows(tb) }

var (
	nestoryBulkOnce sync.Once
	nestoryBulkDB   *nestory.DB[PointRow]
)

func openBulkNestory(tb testing.TB) *nestory.DB[PointRow] {
	tb.Helper()

	nestoryBulkOnce.Do(func() {
		directory, err := os.MkdirTemp("", "nestory-bulk")
		if err != nil {
			tb.Fatalf("tempdir: %v", err)
		}

		benchTempDirs = append(benchTempDirs, directory)
		nestory.DataDir = directory
		if err := nestory.Register[PointRow](); err != nil {
			tb.Fatalf("register: %v", err)
		}

		nestoryBulkDB = nestory.Open[PointRow]()
	})

	return nestoryBulkDB
}

func BenchmarkBulk_Nestory(b *testing.B) {
	batch := bulkBatch(b)
	db := openBulkNestory(b)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		written := make([]int, 0, batch)
		for position := range batch {
			row := &PointRow{
				Name:  "user-" + strconv.Itoa(position),
				Email: "user@example.com",
				Age:   position % 90,
			}
			db.Unsafe().Create(row)
			written = append(written, row.Id)
		}

		if err := db.Unsafe().Flush(); err != nil {
			b.Fatalf("flush: %v", err)
		}

		// Back to empty, off the clock, so the next iteration measures the same
		// thing this one did.
		b.StopTimer()
		for _, id := range written {
			if err := db.Unsafe().Delete(id); err != nil {
				b.Fatalf("delete: %v", err)
			}
		}

		if err := db.Unsafe().Flush(); err != nil {
			b.Fatalf("flush: %v", err)
		}

		b.StartTimer()
	}
}

func BenchmarkBulk_Memdb(b *testing.B) {
	batch := bulkBatch(b)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		db, _ := memdbNew()
		b.StartTimer()

		txn := db.Txn(true)
		for position := 1; position <= batch; position++ {
			record := mkRec(position)
			if err := txn.Insert("rec", &record); err != nil {
				b.Fatalf("insert: %v", err)
			}
		}

		txn.Commit()
	}
}

func benchmarkBulkSQL(b *testing.B, db *sqlDB) {
	batch := bulkBatch(b)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		if _, err := db.handle.Exec(`DELETE FROM rec`); err != nil {
			b.Fatalf("reset: %v", err)
		}

		b.StartTimer()

		tx, err := db.handle.Begin()
		if err != nil {
			b.Fatalf("begin: %v", err)
		}

		statement, err := tx.Prepare(`INSERT INTO rec(id,name,email,age) VALUES(?,?,?,?)`)
		if err != nil {
			b.Fatalf("prepare: %v", err)
		}

		for position := 1; position <= batch; position++ {
			record := mkRec(position)
			if _, err := statement.Exec(record.Id, record.Name, record.Email, record.Age); err != nil {
				b.Fatalf("insert: %v", err)
			}
		}

		statement.Close()
		if err := tx.Commit(); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

func BenchmarkBulk_SQLite(b *testing.B) {
	db := &sqlDB{handle: openSQLite(b, b.TempDir())}
	defer db.handle.Close()
	benchmarkBulkSQL(b, db)
}

func BenchmarkBulk_SQLiteMem(b *testing.B) {
	db := &sqlDB{handle: openSQLiteMem(b)}
	defer db.handle.Close()
	benchmarkBulkSQL(b, db)
}

func BenchmarkBulk_Bolt(b *testing.B) {
	batch := bulkBatch(b)
	db := openBolt(b, b.TempDir())
	defer db.Close()

	// seedBolt is what normally creates the bucket, and bulk does not seed.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("rec"))

		return err
	}); err != nil {
		b.Fatalf("bucket: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("rec"))
			for position := 1; position <= batch; position++ {
				if err := bucket.Put(itob(position), enc(mkRec(position))); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func benchmarkBulkBunt(b *testing.B, db *buntdb.DB) {
	batch := bulkBatch(b)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := db.Update(func(tx *buntdb.Tx) error {
			for position := 1; position <= batch; position++ {
				if _, _, err := tx.Set(strconv.Itoa(position), string(enc(mkRec(position))), nil); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func BenchmarkBulk_Bunt(b *testing.B) {
	db := openBunt(b, b.TempDir())
	defer db.Close()
	benchmarkBulkBunt(b, db)
}

func BenchmarkBulk_BuntMem(b *testing.B) {
	db := openBuntMem(b)
	defer db.Close()
	benchmarkBulkBunt(b, db)
}

func benchmarkBulkBadger(b *testing.B, db *badger.DB) {
	batch := bulkBatch(b)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		writes := db.NewWriteBatch()
		for position := 1; position <= batch; position++ {
			if err := writes.Set(itob(position), enc(mkRec(position))); err != nil {
				b.Fatalf("set: %v", err)
			}
		}

		if err := writes.Flush(); err != nil {
			b.Fatalf("flush: %v", err)
		}
	}
}

func BenchmarkBulk_Badger(b *testing.B) {
	db := openBadger(b, b.TempDir())
	defer db.Close()
	benchmarkBulkBadger(b, db)
}

func BenchmarkBulk_BadgerMem(b *testing.B) {
	db := openBadgerMem(b)
	defer db.Close()
	benchmarkBulkBadger(b, db)
}

func BenchmarkBulk_Redis(b *testing.B) {
	batch := bulkBatch(b)
	client := openRedis(b)
	defer client.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		if err := client.FlushDB(context.Background()).Err(); err != nil {
			b.Fatalf("reset: %v", err)
		}

		b.StartTimer()

		_, err := client.Pipelined(context.Background(), func(pipe redis.Pipeliner) error {
			for position := 1; position <= batch; position++ {
				pipe.Set(context.Background(), strconv.Itoa(position), enc(mkRec(position)), 0)
			}

			return nil
		})
		if err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func BenchmarkBulk_Mongo(b *testing.B) {
	batch := bulkBatch(b)
	collection := openMongo(b)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		if _, err := collection.DeleteMany(context.Background(), bson.M{}); err != nil {
			b.Fatalf("reset: %v", err)
		}

		b.StartTimer()

		documents := make([]any, 0, batch)
		for position := 1; position <= batch; position++ {
			record := mkRec(position)
			documents = append(documents, bson.M{
				"_id": record.Id, "name": record.Name,
				"email": record.Email, "age": record.Age,
			})
		}

		if _, err := collection.InsertMany(context.Background(), documents); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}
