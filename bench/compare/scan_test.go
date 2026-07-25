package compare

// Full scan with a predicate (Age == 42), every engine, one process per player.
// This is where storage layout shows: a contiguous array of structs against
// engines that must decode every value to look at one field.

import (
	"context"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/tidwall/buntdb"
	bolt "go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func BenchmarkScan_Nestory(b *testing.B) {
	rows := benchRows(b)
	db := openPointNestory(b, rows)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		sink += int64(len(db.Filter(func(row PointRow) bool { return row.Age == 42 })))
	}
}

func BenchmarkScan_Memdb(b *testing.B) {
	rows := benchRows(b)
	db, _ := memdbNew()
	seedMemdb(b, db, rows)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		txn := db.Txn(false)
		iterator, err := txn.Get("rec", "id")
		if err != nil {
			b.Fatalf("scan: %v", err)
		}

		for object := iterator.Next(); object != nil; object = iterator.Next() {
			if record := object.(*Rec); record.Age == 42 {
				sink += int64(record.Age)
			}
		}

		txn.Abort()
	}
}

func benchmarkScanSQL(b *testing.B, db *sqlDB) {
	rows := benchRows(b)
	seedSQLite(b, db.handle, rows)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		result, err := db.handle.Query(`SELECT id,name,email,age FROM rec WHERE age=42`)
		if err != nil {
			b.Fatalf("scan: %v", err)
		}

		for result.Next() {
			var record Rec
			if err := result.Scan(&record.Id, &record.Name, &record.Email, &record.Age); err != nil {
				b.Fatalf("row: %v", err)
			}

			sink += int64(record.Age)
		}

		result.Close()
	}
}

func BenchmarkScan_SQLite(b *testing.B) {
	db := &sqlDB{handle: openSQLite(b, b.TempDir())}
	defer db.handle.Close()
	benchmarkScanSQL(b, db)
}

func BenchmarkScan_SQLiteMem(b *testing.B) {
	db := &sqlDB{handle: openSQLiteMem(b)}
	defer db.handle.Close()
	benchmarkScanSQL(b, db)
}

func BenchmarkScan_Bolt(b *testing.B) {
	rows := benchRows(b)
	db := openBolt(b, b.TempDir())
	defer db.Close()
	seedBolt(b, db, rows)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := db.View(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("rec")).ForEach(func(_, value []byte) error {
				if record := dec(value); record.Age == 42 {
					sink += int64(record.Age)
				}

				return nil
			})
		}); err != nil {
			b.Fatalf("scan: %v", err)
		}
	}
}

func benchmarkScanBunt(b *testing.B, db *buntdb.DB) {
	rows := benchRows(b)
	seedBunt(b, db, rows)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := db.View(func(tx *buntdb.Tx) error {
			return tx.Ascend("", func(_, value string) bool {
				if record := dec([]byte(value)); record.Age == 42 {
					sink += int64(record.Age)
				}

				return true
			})
		}); err != nil {
			b.Fatalf("scan: %v", err)
		}
	}
}

func BenchmarkScan_Bunt(b *testing.B) {
	db := openBunt(b, b.TempDir())
	defer db.Close()
	benchmarkScanBunt(b, db)
}

func BenchmarkScan_BuntMem(b *testing.B) {
	db := openBuntMem(b)
	defer db.Close()
	benchmarkScanBunt(b, db)
}

func benchmarkScanBadger(b *testing.B, db *badger.DB, seed func(testing.TB, *badger.DB, int)) {
	rows := benchRows(b)
	seed(b, db, rows)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := db.View(func(txn *badger.Txn) error {
			iterator := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iterator.Close()

			for iterator.Rewind(); iterator.Valid(); iterator.Next() {
				err := iterator.Item().Value(func(value []byte) error {
					if record := dec(value); record.Age == 42 {
						sink += int64(record.Age)
					}

					return nil
				})
				if err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			b.Fatalf("scan: %v", err)
		}
	}
}

func BenchmarkScan_Badger(b *testing.B) {
	db := openBadger(b, b.TempDir())
	defer db.Close()
	benchmarkScanBadger(b, db, seedBadger)
}

func BenchmarkScan_BadgerMem(b *testing.B) {
	db := openBadgerMem(b)
	defer db.Close()
	benchmarkScanBadger(b, db, seedBadgerMem)
}

// Redis has no server-side predicate for opaque values, so a scan is SCAN plus
// MGET and the filtering happens in the client — which is the honest
// translation, and also why one would not model this in Redis.
func BenchmarkScan_Redis(b *testing.B) {
	rows := benchRows(b)
	client := openRedis(b)
	defer client.Close()
	seedRedis(b, client, rows)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		var cursor uint64
		for {
			keys, next, err := client.Scan(context.Background(), cursor, "*", 1000).Result()
			if err != nil {
				b.Fatalf("scan: %v", err)
			}

			if len(keys) > 0 {
				values, err := client.MGet(context.Background(), keys...).Result()
				if err != nil {
					b.Fatalf("mget: %v", err)
				}

				for _, value := range values {
					if raw, ok := value.(string); ok {
						if record := dec([]byte(raw)); record.Age == 42 {
							sink += int64(record.Age)
						}
					}
				}
			}

			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
}

func BenchmarkScan_Mongo(b *testing.B) {
	rows := benchRows(b)
	collection := openMongo(b)
	seedMongo(b, collection, rows)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		cursor, err := collection.Find(context.Background(), bson.M{"age": 42})
		if err != nil {
			b.Fatalf("scan: %v", err)
		}

		for cursor.Next(context.Background()) {
			var record Rec
			if err := cursor.Decode(&record); err != nil {
				b.Fatalf("decode: %v", err)
			}

			sink += int64(record.Age)
		}

		cursor.Close(context.Background())
	}
}
