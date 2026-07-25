package compare

// Point read and point write by id, every engine, one process per player.
// The row count comes from NESTORY_BENCH_SCALE so a run measures one dataset
// size; the runner walks the scales.

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/DorianDevp/nestory"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/go-memdb"
	"github.com/tidwall/buntdb"
	bolt "go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// benchRows is the one dataset size this process measures.
func benchRows(tb testing.TB) int {
	tb.Helper()

	raw := os.Getenv("NESTORY_BENCH_SCALE")
	if raw == "" {
		return 1000
	}

	rows, err := strconv.Atoi(raw)
	if err != nil {
		tb.Fatalf("NESTORY_BENCH_SCALE: %v", err)
	}

	return rows
}

// nestory, registered once per process for the same reason the graph fixture is.

type PointRow struct {
	Id    int `key:"primary"`
	Name  string
	Email string
	Age   int
}

func (row PointRow) GetId() int { return row.Id }

var (
	nestoryPointOnce sync.Once
	nestoryPointDB   *nestory.DB[PointRow]
)

func openPointNestory(tb testing.TB, rows int) *nestory.DB[PointRow] {
	tb.Helper()

	nestoryPointOnce.Do(func() {
		directory, err := os.MkdirTemp("", "nestory-point")
		if err != nil {
			tb.Fatalf("tempdir: %v", err)
		}

		benchTempDirs = append(benchTempDirs, directory)
		nestory.DataDir = directory
		if err := nestory.Register[PointRow](); err != nil {
			tb.Fatalf("register: %v", err)
		}

		db := nestory.Open[PointRow]()
		for position := 1; position <= rows; position++ {
			db.Unsafe().Create(&PointRow{
				Name:  "user-" + strconv.Itoa(position),
				Email: "user-" + strconv.Itoa(position) + "@example.com",
				Age:   position % 90,
			})
		}

		if err := db.Unsafe().Flush(); err != nil {
			tb.Fatalf("flush: %v", err)
		}

		nestoryPointDB = db
	})

	return nestoryPointDB
}

func BenchmarkPointRead_Nestory(b *testing.B) {
	rows := benchRows(b)
	db := openPointNestory(b, rows)
	target := max(rows/2, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := db.View(target, func(row *PointRow) error {
			sink += int64(row.Age)

			return nil
		}); err != nil {
			b.Fatalf("view: %v", err)
		}
	}
}

func BenchmarkPointRead_NestoryUnsafe(b *testing.B) {
	rows := benchRows(b)
	db := openPointNestory(b, rows)
	target := max(rows/2, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		row, err := db.Unsafe().Get(target)
		if err != nil {
			b.Fatalf("get: %v", err)
		}

		sink += int64(row.Age)
	}
}

// A detached read: the caller gets a private copy it may mutate.
func BenchmarkPointRead_NestoryGet(b *testing.B) {
	rows := benchRows(b)
	db := openPointNestory(b, rows)
	target := max(rows/2, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		row, err := db.Get(target)
		if err != nil {
			b.Fatalf("get: %v", err)
		}

		sink += int64(row.Age)
	}
}

func BenchmarkPointWrite_Nestory(b *testing.B) {
	rows := benchRows(b)
	db := openPointNestory(b, rows)
	target := max(rows/2, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for iteration := range b.N {
		if err := db.UpdateWithin(target, func(row *PointRow) error {
			row.Age = iteration % 90

			return nil
		}); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

// go-memdb

func BenchmarkPointRead_Memdb(b *testing.B) {
	rows := benchRows(b)
	db, _ := memdb.NewMemDB(memSchema())
	seedMemdb(b, db, rows)
	target := max(rows/2, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		txn := db.Txn(false)
		raw, _ := txn.First("rec", "id", target)
		if raw != nil {
			sink += int64(raw.(*Rec).Age)
		}

		txn.Abort()
	}
}

func BenchmarkPointWrite_Memdb(b *testing.B) {
	rows := benchRows(b)
	db, _ := memdb.NewMemDB(memSchema())
	seedMemdb(b, db, rows)
	target := max(rows/2, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for iteration := range b.N {
		txn := db.Txn(true)
		record := mkRec(target)
		record.Age = iteration % 90
		if err := txn.Insert("rec", &record); err != nil {
			b.Fatalf("write: %v", err)
		}

		txn.Commit()
	}
}

// SQLite, on disk and in memory

func benchmarkPointReadSQL(b *testing.B, db *sqlDB) {
	rows := benchRows(b)
	seedSQLite(b, db.handle, rows)
	statement, err := db.handle.Prepare(`SELECT age FROM rec WHERE id=?`)
	if err != nil {
		b.Fatalf("prepare: %v", err)
	}

	defer statement.Close()

	target := max(rows/2, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		var age int
		if err := statement.QueryRow(target).Scan(&age); err != nil {
			b.Fatalf("read: %v", err)
		}

		sink += int64(age)
	}
}

func BenchmarkPointRead_SQLite(b *testing.B) {
	db := &sqlDB{handle: openSQLite(b, b.TempDir())}
	defer db.handle.Close()
	benchmarkPointReadSQL(b, db)
}

func BenchmarkPointRead_SQLiteMem(b *testing.B) {
	db := &sqlDB{handle: openSQLiteMem(b)}
	defer db.handle.Close()
	benchmarkPointReadSQL(b, db)
}

func BenchmarkPointWrite_SQLite(b *testing.B) {
	rows := benchRows(b)
	db := openSQLite(b, b.TempDir())
	defer db.Close()
	seedSQLite(b, db, rows)

	target := max(rows/2, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for iteration := range b.N {
		if _, err := db.Exec(`UPDATE rec SET age=? WHERE id=?`, iteration%90, target); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

func BenchmarkPointWrite_SQLiteMem(b *testing.B) {
	rows := benchRows(b)
	db := openSQLiteMem(b)
	defer db.Close()
	seedSQLite(b, db, rows)

	target := max(rows/2, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for iteration := range b.N {
		if _, err := db.Exec(`UPDATE rec SET age=? WHERE id=?`, iteration%90, target); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

// bbolt

func BenchmarkPointRead_Bolt(b *testing.B) {
	rows := benchRows(b)
	db := openBolt(b, b.TempDir())
	defer db.Close()
	seedBolt(b, db, rows)

	target := itob(max(rows/2, 1))
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := db.View(func(tx *bolt.Tx) error {
			sink += int64(dec(tx.Bucket([]byte("rec")).Get(target)).Age)

			return nil
		}); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

func BenchmarkPointWrite_Bolt(b *testing.B) {
	rows := benchRows(b)
	db := openBolt(b, b.TempDir())
	defer db.Close()
	seedBolt(b, db, rows)

	id := max(rows/2, 1)
	target := itob(id)
	b.ResetTimer()
	b.ReportAllocs()
	for iteration := range b.N {
		record := mkRec(id)
		record.Age = iteration % 90
		if err := db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("rec")).Put(target, enc(record))
		}); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

// BuntDB, on disk and in memory

func benchmarkPointReadBunt(b *testing.B, db *buntdb.DB) {
	rows := benchRows(b)
	seedBunt(b, db, rows)

	target := strconv.Itoa(max(rows/2, 1))
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := db.View(func(tx *buntdb.Tx) error {
			raw, err := tx.Get(target)
			if err != nil {
				return err
			}

			sink += int64(dec([]byte(raw)).Age)

			return nil
		}); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

func BenchmarkPointRead_Bunt(b *testing.B) {
	db := openBunt(b, b.TempDir())
	defer db.Close()
	benchmarkPointReadBunt(b, db)
}

func BenchmarkPointRead_BuntMem(b *testing.B) {
	db := openBuntMem(b)
	defer db.Close()
	benchmarkPointReadBunt(b, db)
}

func benchmarkPointWriteBunt(b *testing.B, db *buntdb.DB) {
	rows := benchRows(b)
	seedBunt(b, db, rows)

	id := max(rows/2, 1)
	target := strconv.Itoa(id)
	b.ResetTimer()
	b.ReportAllocs()
	for iteration := range b.N {
		record := mkRec(id)
		record.Age = iteration % 90
		if err := db.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(target, string(enc(record)), nil)

			return err
		}); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

func BenchmarkPointWrite_Bunt(b *testing.B) {
	db := openBunt(b, b.TempDir())
	defer db.Close()
	benchmarkPointWriteBunt(b, db)
}

func BenchmarkPointWrite_BuntMem(b *testing.B) {
	db := openBuntMem(b)
	defer db.Close()
	benchmarkPointWriteBunt(b, db)
}

// Badger, on disk and in memory

func benchmarkPointReadBadger(b *testing.B, db *badger.DB, seed func(testing.TB, *badger.DB, int)) {
	rows := benchRows(b)
	seed(b, db, rows)

	target := itob(max(rows/2, 1))
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(target)
			if err != nil {
				return err
			}

			return item.Value(func(value []byte) error {
				sink += int64(dec(value).Age)

				return nil
			})
		}); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

func BenchmarkPointRead_Badger(b *testing.B) {
	db := openBadger(b, b.TempDir())
	defer db.Close()
	benchmarkPointReadBadger(b, db, seedBadger)
}

func BenchmarkPointRead_BadgerMem(b *testing.B) {
	db := openBadgerMem(b)
	defer db.Close()
	benchmarkPointReadBadger(b, db, seedBadgerMem)
}

func benchmarkPointWriteBadger(b *testing.B, db *badger.DB, seed func(testing.TB, *badger.DB, int)) {
	rows := benchRows(b)
	seed(b, db, rows)

	id := max(rows/2, 1)
	target := itob(id)
	b.ResetTimer()
	b.ReportAllocs()
	for iteration := range b.N {
		record := mkRec(id)
		record.Age = iteration % 90
		if err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(target, enc(record))
		}); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

func BenchmarkPointWrite_Badger(b *testing.B) {
	db := openBadger(b, b.TempDir())
	defer db.Close()
	benchmarkPointWriteBadger(b, db, seedBadger)
}

func BenchmarkPointWrite_BadgerMem(b *testing.B) {
	db := openBadgerMem(b)
	defer db.Close()
	benchmarkPointWriteBadger(b, db, seedBadgerMem)
}

// sqlDB keeps the two SQLite variants sharing one read benchmark body.
type sqlDB struct{ handle *sql.DB }

// Redis and MongoDB, gated on a reachable server. Both answer over loopback
// TCP, so their numbers mostly say what an out-of-process database costs.

func BenchmarkPointRead_Redis(b *testing.B) {
	rows := benchRows(b)
	client := openRedis(b)
	defer client.Close()
	seedRedis(b, client, rows)

	target := strconv.Itoa(max(rows/2, 1))
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		raw, err := client.Get(context.Background(), target).Bytes()
		if err != nil {
			b.Fatalf("read: %v", err)
		}

		sink += int64(dec(raw).Age)
	}
}

func BenchmarkPointWrite_Redis(b *testing.B) {
	rows := benchRows(b)
	client := openRedis(b)
	defer client.Close()
	seedRedis(b, client, rows)

	id := max(rows/2, 1)
	target := strconv.Itoa(id)
	b.ResetTimer()
	b.ReportAllocs()
	for iteration := range b.N {
		record := mkRec(id)
		record.Age = iteration % 90
		if err := client.Set(context.Background(), target, enc(record), 0).Err(); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

func BenchmarkPointRead_Mongo(b *testing.B) {
	rows := benchRows(b)
	collection := openMongo(b)
	seedMongo(b, collection, rows)

	target := max(rows/2, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		var record Rec
		if err := collection.FindOne(context.Background(), bson.M{"_id": target}).Decode(&record); err != nil {
			b.Fatalf("read: %v", err)
		}

		sink += int64(record.Age)
	}
}

func BenchmarkPointWrite_Mongo(b *testing.B) {
	rows := benchRows(b)
	collection := openMongo(b)
	seedMongo(b, collection, rows)

	target := max(rows/2, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for iteration := range b.N {
		_, err := collection.UpdateByID(context.Background(), target,
			bson.M{"$set": bson.M{"age": iteration % 90}})
		if err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}
