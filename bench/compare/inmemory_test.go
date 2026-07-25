package compare

// Pure in-memory variants of the disk-backed competitors, so nestory can be
// compared against engines playing the same game: no file, no fsync, state
// lives and dies with the process. Same record shape and workloads as
// compare_test.go. Redis — the canonical in-memory server database — is gated
// on NESTORY_REDIS_URI the same way MongoDB is gated in mongo_test.go.

import (
	"context"
	"database/sql"
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

// BuntDB, :memory: — no AOF, no sync policy to configure.

func openBuntMem(tb testing.TB) *buntdb.DB {
	tb.Helper()
	db, err := buntdb.Open(":memory:")
	if err != nil {
		tb.Fatalf("open buntdb mem: %v", err)
	}
	return db
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
