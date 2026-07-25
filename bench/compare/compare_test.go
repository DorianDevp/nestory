// Package compare benchmarks nestory's competitors on the same machine and
// record shape (Rec ≈ benchItem). nestory's own numbers come from the parent
// package's bench_test.go. Competitors: modernc.org/sqlite (durable),
// go.etcd.io/bbolt (durable), hashicorp/go-memdb (in-memory).
// Workloads: BulkInsert, PointRead, Scan, PointWrite.
package compare

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-memdb"
	bolt "go.etcd.io/bbolt"
	_ "modernc.org/sqlite"
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

func BenchmarkSQLite_BulkInsert(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := openSQLite(b, b.TempDir())
				b.StartTimer()
				seedSQLite(b, db, n)
				db.Close()
			}
		})
	}
}

func BenchmarkSQLite_Scan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openSQLite(b, b.TempDir())
			seedSQLite(b, db, n)
			defer db.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rows, _ := db.Query(`SELECT id,name,email,age FROM rec WHERE age=42`)
				for rows.Next() {
					var r Rec
					rows.Scan(&r.Id, &r.Name, &r.Email, &r.Age)
					sink += int64(r.Id)
				}
				rows.Close()
			}
		})
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

func BenchmarkBolt_BulkInsert(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := openBolt(b, b.TempDir())
				b.StartTimer()
				seedBolt(b, db, n)
				db.Close()
			}
		})
	}
}

func BenchmarkBolt_Scan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openBolt(b, b.TempDir())
			seedBolt(b, db, n)
			defer db.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = db.View(func(tx *bolt.Tx) error {
					c := tx.Bucket(bucket).Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						if r := dec(v); r.Age == 42 {
							sink += int64(r.Id)
						}
					}
					return nil
				})
			}
		})
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

func BenchmarkMemdb_BulkInsert(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db, _ := memdb.NewMemDB(memSchema())
				b.StartTimer()
				seedMemdb(b, db, n)
			}
		})
	}
}

// PointWrite: durable single-row update

func BenchmarkMemdb_Scan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db, _ := memdb.NewMemDB(memSchema())
			seedMemdb(b, db, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				txn := db.Txn(false)
				it, _ := txn.Get("rec", "id")
				for obj := it.Next(); obj != nil; obj = it.Next() {
					if r := obj.(*Rec); r.Age == 42 {
						sink += int64(r.Id)
					}
				}
				txn.Abort()
			}
		})
	}
}
