package compare

// Pure in-memory variants of the disk-backed competitors, so nestory can be
// compared against engines playing the same game: no file, no fsync, state
// lives and dies with the process. Same record shape and workloads as
// compare_test.go. Redis — the canonical in-memory server database — is gated
// on NESTORY_REDIS_URI the same way MongoDB is gated in mongo_test.go.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/buntdb"
)

// SQLite, :memory:. One connection only: database/sql pools connections and
// every pooled connection would otherwise get its own private database.

func openSQLiteMem(tb testing.TB) *sql.DB {
	tb.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		tb.Fatalf("open sqlite mem: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE rec(id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)`); err != nil {
		tb.Fatalf("create: %v", err)
	}
	return db
}

func BenchmarkSQLiteMem_BulkInsert(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := openSQLiteMem(b)
				b.StartTimer()
				seedSQLite(b, db, n)
				b.StopTimer()
				db.Close()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkSQLiteMem_Scan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openSQLiteMem(b)
			defer db.Close()
			seedSQLite(b, db, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(`SELECT id,name,email,age FROM rec WHERE age=42`)
				if err != nil {
					b.Fatalf("scan: %v", err)
				}
				for rows.Next() {
					var r Rec
					if err := rows.Scan(&r.Id, &r.Name, &r.Email, &r.Age); err != nil {
						b.Fatalf("row: %v", err)
					}
					sink += int64(r.Age)
				}
				rows.Close()
			}
		})
	}
}

// BuntDB, :memory: — no AOF, no sync policy to configure.

func openBuntMem(tb testing.TB) *buntdb.DB {
	tb.Helper()
	db, err := buntdb.Open(":memory:")
	if err != nil {
		tb.Fatalf("open buntdb mem: %v", err)
	}
	return db
}

func BenchmarkBuntMem_BulkInsert(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := openBuntMem(b)
				b.StartTimer()
				seedBunt(b, db, n)
				b.StopTimer()
				db.Close()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkBuntMem_Scan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openBuntMem(b)
			defer db.Close()
			seedBunt(b, db, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				err := db.View(func(tx *buntdb.Tx) error {
					return tx.Ascend("", func(_, value string) bool {
						if r := dec([]byte(value)); r.Age == 42 {
							sink += int64(r.Age)
						}
						return true
					})
				})
				if err != nil {
					b.Fatalf("scan: %v", err)
				}
			}
		})
	}
}

// Badger, WithInMemory — LSM machinery kept, value log in RAM.

func openBadgerMem(tb testing.TB) *badger.DB {
	tb.Helper()
	options := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(options)
	if err != nil {
		tb.Fatalf("open badger mem: %v", err)
	}
	return db
}

func seedBadgerMem(tb testing.TB, db *badger.DB, n int) {
	tb.Helper()
	batch := db.NewWriteBatch()
	for j := 1; j <= n; j++ {
		if err := batch.Set(itob(j), enc(mkRec(j))); err != nil {
			tb.Fatalf("set: %v", err)
		}
	}
	if err := batch.Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}
}

func BenchmarkBadgerMem_BulkInsert(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := openBadgerMem(b)
				b.StartTimer()
				seedBadgerMem(b, db, n)
				b.StopTimer()
				db.Close()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkBadgerMem_Scan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openBadgerMem(b)
			defer db.Close()
			seedBadgerMem(b, db, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				err := db.View(func(txn *badger.Txn) error {
					iterator := txn.NewIterator(badger.DefaultIteratorOptions)
					defer iterator.Close()
					for iterator.Rewind(); iterator.Valid(); iterator.Next() {
						err := iterator.Item().Value(func(value []byte) error {
							if r := dec(value); r.Age == 42 {
								sink += int64(r.Age)
							}
							return nil
						})
						if err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					b.Fatalf("scan: %v", err)
				}
			}
		})
	}
}

// Redis — in-memory server over loopback TCP, appendonly off (its default), so
// no fsync anywhere. The comparison mostly measures embedded versus networked,
// exactly like MongoDB in mongo_test.go; read it with the same caveat.

func openRedis(tb testing.TB) *redis.Client {
	tb.Helper()
	uri := os.Getenv("NESTORY_REDIS_URI")
	if uri == "" {
		tb.Skip("set NESTORY_REDIS_URI (e.g. 127.0.0.1:6380) to run the Redis benchmarks")
	}
	client := redis.NewClient(&redis.Options{Addr: uri})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		tb.Skipf("redis unreachable at %s: %v", uri, err)
	}
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		tb.Fatalf("flushdb: %v", err)
	}
	return client
}

func seedRedis(tb testing.TB, client *redis.Client, n int) {
	tb.Helper()
	_, err := client.Pipelined(context.Background(), func(pipe redis.Pipeliner) error {
		for j := 1; j <= n; j++ {
			pipe.Set(context.Background(), strconv.Itoa(j), enc(mkRec(j)), 0)
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("seed redis: %v", err)
	}
}

func BenchmarkRedis_BulkInsert(b *testing.B) {
	client := openRedis(b)
	defer client.Close()
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if err := client.FlushDB(context.Background()).Err(); err != nil {
					b.Fatalf("flushdb: %v", err)
				}
				b.StartTimer()
				seedRedis(b, client, n)
			}
		})
	}
}

func BenchmarkRedis_Scan(b *testing.B) {
	client := openRedis(b)
	defer client.Close()
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			seedRedis(b, client, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
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
								if r := dec([]byte(raw)); r.Age == 42 {
									sink += int64(r.Age)
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
		})
	}
}
