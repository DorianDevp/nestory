package compare

// Shared fixtures for every benchmark category: the flat record, the
// encode/decode pair the KV engines need, and one open/seed pair per engine.
// The categories themselves live in point_test.go, bulk_test.go, scan_test.go
// and graph_test.go — this file holds only what they all reach for.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/go-memdb"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/buntdb"
	bolt "go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Rec mirrors nestory's benchItem: int id + two strings + an int.
type Rec struct {
	Id    int
	Name  string
	Email string
	Age   int
}

func mkRec(i int) Rec {
	return Rec{
		Id:    i,
		Name:  fmt.Sprintf("user-%d", i),
		Email: fmt.Sprintf("user-%d@example.com", i),
		Age:   i % 90,
	}
}

var sizes = []int{100, 1000, 10000}

var sink int64

func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func enc(r Rec) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func dec(b []byte) Rec {
	var r Rec
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&r); err != nil {
		panic(err)
	}
	return r
}

// SQLite (modernc, durable)

func openSQLite(tb testing.TB, dir string) *sql.DB {
	tb.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "rec.db"))
	if err != nil {
		tb.Fatalf("open sqlite: %v", err)
	}
	if _, err := db.Exec(`PRAGMA synchronous=FULL;`); err != nil {
		tb.Fatalf("pragma: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE rec(id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)`); err != nil {
		tb.Fatalf("create: %v", err)
	}
	return db
}

func seedSQLite(tb testing.TB, db *sql.DB, n int) {
	tb.Helper()
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare(`INSERT INTO rec(id,name,email,age) VALUES(?,?,?,?)`)
	for j := 1; j <= n; j++ {
		r := mkRec(j)
		if _, err := stmt.Exec(r.Id, r.Name, r.Email, r.Age); err != nil {
			tb.Fatalf("insert: %v", err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		tb.Fatalf("commit: %v", err)
	}
}

// bbolt (durable)

var bucket = []byte("rec")

func openBolt(tb testing.TB, dir string) *bolt.DB {
	tb.Helper()
	db, err := bolt.Open(filepath.Join(dir, "rec.bolt"), 0o600, nil)
	if err != nil {
		tb.Fatalf("open bolt: %v", err)
	}
	return db
}

func seedBolt(tb testing.TB, db *bolt.DB, n int) {
	tb.Helper()
	err := db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for j := 1; j <= n; j++ {
			if err := bk.Put(itob(j), enc(mkRec(j))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("seed bolt: %v", err)
	}
}

// go-memdb (in-memory, not durable)

func memSchema() *memdb.DBSchema {
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"rec": {
				Name: "rec",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {Name: "id", Unique: true, Indexer: &memdb.IntFieldIndex{Field: "Id"}},
				},
			},
		},
	}
}

func seedMemdb(tb testing.TB, db *memdb.MemDB, n int) {
	tb.Helper()
	txn := db.Txn(true)
	for j := 1; j <= n; j++ {
		r := mkRec(j)
		if err := txn.Insert("rec", &r); err != nil {
			tb.Fatalf("insert: %v", err)
		}
	}
	txn.Commit()
}

// PointWrite: durable single-row update

// memdbNew opens an empty go-memdb with the flat-record schema.
func memdbNew() (*memdb.MemDB, error) { return memdb.NewMemDB(memSchema()) }

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

// mongoRec is Rec with _id carrying the integer key, so a point read hits the
// primary index rather than a secondary one.
type mongoRec struct {
	ID    int    `bson:"_id"`
	Name  string `bson:"name"`
	Email string `bson:"email"`
	Age   int    `bson:"age"`
}

func mkMongoRec(i int) mongoRec {
	r := mkRec(i)

	return mongoRec{ID: r.Id, Name: r.Name, Email: r.Email, Age: r.Age}
}

// openMongo dials the server with journaled majority-free acknowledgement,
// which is the closest Mongo gets to the fsync-per-commit the durable embedded
// engines are configured for.
func openMongo(tb testing.TB) *mongo.Collection {
	tb.Helper()

	uri := os.Getenv("NESTORY_MONGO_URI")
	if uri == "" {
		tb.Skip("set NESTORY_MONGO_URI to benchmark MongoDB")
	}

	opts := options.Client().
		ApplyURI(uri).
		SetWriteConcern(writeconcern.W1()).
		SetRetryWrites(false)
	opts.WriteConcern.Journal = ptr(true)

	client, err := mongo.Connect(opts)
	if err != nil {
		tb.Fatalf("connect: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx, nil); err != nil {
		tb.Fatalf("ping: %v", err)
	}

	name := fmt.Sprintf("nestory_bench_%d", time.Now().UnixNano())
	database := client.Database(name)
	tb.Cleanup(func() {
		_ = database.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})

	return database.Collection("rec")
}

func ptr[T any](value T) *T { return &value }

func seedMongo(tb testing.TB, collection *mongo.Collection, n int) {
	tb.Helper()

	docs := make([]any, n)
	for i := range n {
		docs[i] = mkMongoRec(i + 1)
	}

	if _, err := collection.InsertMany(context.Background(), docs); err != nil {
		tb.Fatalf("seed: %v", err)
	}
}
