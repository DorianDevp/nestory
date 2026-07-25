// MongoDB is the odd one out here: every other engine in this package runs
// in-process, while Mongo is a server reached over a socket. Its numbers carry
// a client/server round trip that the embedded stores never pay, so they belong
// in a separate row of the comparison rather than alongside them.
//
// The benchmarks skip unless NESTORY_MONGO_URI points at a reachable server:
//
//	docker run -d --name nestory-bench-mongo -p 127.0.0.1:27018:27017 mongo:8
//	NESTORY_MONGO_URI=mongodb://127.0.0.1:27018 go test -bench Mongo ./compare/
package compare

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

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

func BenchmarkMongo_BulkInsert(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			collection := openMongo(b)
			docs := make([]any, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				base := i * n
				for j := range n {
					docs[j] = mkMongoRec(base + j + 1)
				}
				if _, err := collection.InsertMany(context.Background(), docs); err != nil {
					b.Fatalf("insert: %v", err)
				}
			}
		})
	}
}

func BenchmarkMongo_Scan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			collection := openMongo(b)
			seedMongo(b, collection, n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cursor, err := collection.Find(context.Background(), bson.M{"age": 42})
				if err != nil {
					b.Fatalf("find: %v", err)
				}

				for cursor.Next(context.Background()) {
					var rec mongoRec
					if err := cursor.Decode(&rec); err != nil {
						b.Fatalf("decode: %v", err)
					}

					sink += int64(rec.Age)
				}

				if err := cursor.Err(); err != nil {
					b.Fatalf("cursor: %v", err)
				}

				_ = cursor.Close(context.Background())
			}
		})
	}
}
