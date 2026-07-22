package nestory

import (
	"errors"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"
)

type indexedMessage struct {
	Id        int
	MessageID string `key:"unique"`
	SessionID string `index:"session_seq,1,unique"`
	Seq       int    `index:"session_seq,2,unique"`
	Payload   []byte
}

func (message indexedMessage) GetId() int { return message.Id }

type invalidIndexedMessage struct {
	Id      int
	Payload []byte `key:"unique"`
}

func (message invalidIndexedMessage) GetId() int { return message.Id }

func TestUniqueAndCompositeIndexesTrackTransactions(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db := Open[indexedMessage]()
	messages := []*indexedMessage{
		{MessageID: "m3", SessionID: "s1", Seq: 3},
		{MessageID: "m1", SessionID: "s1", Seq: 1},
		{MessageID: "m2", SessionID: "s1", Seq: 2},
		{MessageID: "other", SessionID: "s2", Seq: 1},
	}
	for _, message := range messages {
		if err := db.Create(message); err != nil {
			t.Fatal(err)
		}
	}

	byMessageID, err := db.FindOneBy("MessageID", "m2")
	if err != nil {
		t.Fatal(err)
	}

	if byMessageID.Seq != 2 {
		t.Fatalf("message seq = %d, want 2", byMessageID.Seq)
	}

	var sequence []int
	if err := db.ViewRange("session_seq", []any{"s1"}, func(entries []*indexedMessage) error {
		for _, entry := range entries {
			sequence = append(sequence, entry.Seq)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sequence, []int{1, 2, 3}) {
		t.Fatalf("sequence = %v", sequence)
	}

	if err := db.Create(&indexedMessage{MessageID: "m2", SessionID: "s3", Seq: 1}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate message error = %v, want ErrUniqueViolation", err)
	}

	if err := db.Create(&indexedMessage{MessageID: "m4", SessionID: "s1", Seq: 2}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate sequence error = %v, want ErrUniqueViolation", err)
	}

	if err := db.UpdateWithin(messages[1].Id, func(message *indexedMessage) error {
		message.MessageID = "renamed"
		message.Seq = 4

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := db.FindOneBy("MessageID", "m1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old unique key error = %v, want ErrNotFound", err)
	}

	if _, err := db.FindOneBy("MessageID", "renamed"); err != nil {
		t.Fatal(err)
	}

	sequence = sequence[:0]
	if err := db.ViewRange("session_seq", []any{"s1"}, func(entries []*indexedMessage) error {
		for _, entry := range entries {
			sequence = append(sequence, entry.Seq)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sequence, []int{2, 3, 4}) {
		t.Fatalf("sequence after update = %v", sequence)
	}

	if err := db.Transaction(func(tx *Tx[indexedMessage]) error {
		second, err := tx.Get(messages[2].Id)
		if err != nil {
			return err
		}

		third, err := tx.Get(messages[0].Id)
		if err != nil {
			return err
		}

		second.Seq, third.Seq = third.Seq, second.Seq

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var orderedIDs []string
	if err := db.ViewRange("session_seq", []any{"s1"}, func(entries []*indexedMessage) error {
		for _, entry := range entries {
			orderedIDs = append(orderedIDs, entry.MessageID)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(orderedIDs, []string{"m3", "m2", "renamed"}) {
		t.Fatalf("messages after sequence swap = %v", orderedIDs)
	}
}

func TestConcurrentUniqueCreateCommitsOnce(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db := Open[indexedMessage]()

	const workers = 16
	start := make(chan struct{})
	errs := make(chan error, workers)
	for worker := range workers {
		go func() {
			<-start
			errs <- db.Create(&indexedMessage{
				Id: worker + 1, MessageID: "same", SessionID: "session", Seq: worker + 1,
			})
		}()
	}

	close(start)

	var committed int
	for range workers {
		err := <-errs
		if err == nil {
			committed++
			continue
		}

		if !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("create error = %v", err)
		}
	}

	if committed != 1 {
		t.Fatalf("successful creates = %d, want 1", committed)
	}
}

func TestSecondaryIndexesSurviveReplayAndDelete(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db := Open[indexedMessage]()
	message := &indexedMessage{MessageID: "durable", SessionID: "session", Seq: 7}
	if err := db.Create(message); err != nil {
		t.Fatal(err)
	}

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db = Open[indexedMessage]()
	if _, err := db.FindOneBy("MessageID", "durable"); err != nil {
		t.Fatal(err)
	}

	if err := db.Delete(message.Id); err != nil {
		t.Fatal(err)
	}

	if _, err := db.FindOneBy("MessageID", "durable"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted unique key error = %v, want ErrNotFound", err)
	}
}

func TestUnsafeFlushValidatesUniqueIndexes(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db := Open[indexedMessage]()
	first := &indexedMessage{MessageID: "first", SessionID: "session", Seq: 1}
	second := &indexedMessage{MessageID: "second", SessionID: "session", Seq: 2}
	db.Unsafe().Create(first)
	db.Unsafe().Create(second)
	if err := db.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}

	live, err := db.Unsafe().Get(second.Id)
	if err != nil {
		t.Fatal(err)
	}

	live.MessageID = first.MessageID
	if err := db.Unsafe().Flush(); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("unsafe duplicate error = %v, want ErrUniqueViolation", err)
	}

	live.MessageID = "second"
	if err := db.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterRejectsUnsupportedIndexField(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[invalidIndexedMessage](); err == nil {
		t.Fatal("Register accepted a slice unique index")
	}
}

func TestViewManyReturnsIDOrderAndDeduplicates(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db := Open[indexedMessage]()
	for index := 1; index <= 3; index++ {
		if err := db.Create(&indexedMessage{MessageID: string(rune('a' + index)), SessionID: "s", Seq: index}); err != nil {
			t.Fatal(err)
		}
	}

	var ids []int
	if err := db.ViewMany([]int{3, 1, 3, 2}, func(messages []*indexedMessage) error {
		for _, message := range messages {
			ids = append(ids, message.Id)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(ids, []int{1, 2, 3}) {
		t.Fatalf("ids = %v", ids)
	}
}

func TestDeleteByIndexPersistsAsOneTransaction(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db := Open[indexedMessage]()
	for sequence := 1; sequence <= 100; sequence++ {
		if err := db.Create(&indexedMessage{
			MessageID: "delete-" + strconv.Itoa(sequence), SessionID: "delete", Seq: sequence,
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Create(&indexedMessage{MessageID: "keep", SessionID: "keep", Seq: 1}); err != nil {
		t.Fatal(err)
	}

	if err := db.DeleteByIndex("session_seq", []any{"delete"}); err != nil {
		t.Fatal(err)
	}

	if got := db.Len(); got != 1 {
		t.Fatalf("rows after indexed delete = %d, want 1", got)
	}

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db = Open[indexedMessage]()
	if got := db.Len(); got != 1 {
		t.Fatalf("rows after replay = %d, want 1", got)
	}

	if _, err := db.FindOneBy("MessageID", "keep"); err != nil {
		t.Fatal(err)
	}
}

func TestSecondaryIndexBatchMaintainsOrderAndLookups(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db := Open[indexedMessage]()
	messages := make([]*indexedMessage, 128)
	if err := db.Transaction(func(tx *Tx[indexedMessage]) error {
		for index := range messages {
			sequence := len(messages) - index
			message := &indexedMessage{
				MessageID: "batch-" + strconv.Itoa(sequence),
				SessionID: "session",
				Seq:       sequence,
			}
			messages[index] = message
			if err := tx.Create(message); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	assertIndexedSequence(t, db, "session", 1, 128)
	if _, err := db.FindOneBy("MessageID", "batch-64"); err != nil {
		t.Fatal(err)
	}

	if err := db.Transaction(func(tx *Tx[indexedMessage]) error {
		for index, message := range messages {
			if index%2 == 0 {
				if err := tx.Delete(message.Id); err != nil {
					return err
				}

				continue
			}

			updated, err := tx.Get(message.Id)
			if err != nil {
				return err
			}

			updated.MessageID = "updated-" + strconv.Itoa(updated.Seq)
			updated.Seq += 1_000
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var sequences []int
	if err := db.ViewRange("session_seq", []any{"session"}, func(entries []*indexedMessage) error {
		for _, entry := range entries {
			sequences = append(sequences, entry.Seq)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(sequences) != 64 {
		t.Fatalf("remaining rows = %d, want 64", len(sequences))
	}

	for index, sequence := range sequences {
		if index > 0 && sequence <= sequences[index-1] {
			t.Fatalf("sequence is not ordered: %v", sequences)
		}

		if _, err := db.FindOneBy("MessageID", "updated-"+strconv.Itoa(sequence-1_000)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestViewRangeAfterUsesNextCompositeField(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[indexedMessage](); err != nil {
		t.Fatal(err)
	}

	db := Open[indexedMessage]()
	if err := db.Transaction(func(tx *Tx[indexedMessage]) error {
		for sequence := 1; sequence <= 5; sequence++ {
			if err := tx.Create(&indexedMessage{
				MessageID: "s1-" + strconv.Itoa(sequence), SessionID: "s1", Seq: sequence,
			}); err != nil {
				return err
			}

			if err := tx.Create(&indexedMessage{
				MessageID: "s2-" + strconv.Itoa(sequence), SessionID: "s2", Seq: sequence,
			}); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var sequences []int
	if err := db.ViewRangeAfter("session_seq", []any{"s1"}, 2, func(entries []*indexedMessage) error {
		for _, entry := range entries {
			sequences = append(sequences, entry.Seq)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sequences, []int{3, 4, 5}) {
		t.Fatalf("sequence after cursor = %v, want [3 4 5]", sequences)
	}

	if err := db.ViewRangeAfter("session_seq", []any{"s1"}, "2", func([]*indexedMessage) error {
		return nil
	}); err == nil {
		t.Fatal("ViewRangeAfter accepted a cursor with the wrong type")
	}

	if err := db.ViewRangeAfter("session_seq", []any{"s1", 2}, 3, func([]*indexedMessage) error {
		return nil
	}); err == nil {
		t.Fatal("ViewRangeAfter accepted a cursor after a complete index key")
	}
}

func assertIndexedSequence(t *testing.T, db *DB[indexedMessage], session string, first, last int) {
	t.Helper()
	position := first
	if err := db.ViewRange("session_seq", []any{session}, func(entries []*indexedMessage) error {
		for _, entry := range entries {
			if entry.Seq != position {
				t.Fatalf("sequence at %d = %d, want %d", position-first, entry.Seq, position)
			}

			position++
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if position != last+1 {
		t.Fatalf("last sequence position = %d, want %d", position, last+1)
	}
}

func BenchmarkIndexedBatchCreate(b *testing.B) {
	benchmarkIndexedBatch(b, func(b *testing.B, db *DB[indexedMessage], size int) {
		b.StartTimer()
		err := db.Transaction(func(tx *Tx[indexedMessage]) error {
			for sequence := 1; sequence <= size; sequence++ {
				if err := tx.Create(&indexedMessage{
					MessageID: "create-" + strconv.Itoa(sequence),
					SessionID: "session",
					Seq:       sequence,
				}); err != nil {
					return err
				}
			}

			return nil
		})
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
	})
}

func BenchmarkIndexedBatchDelete(b *testing.B) {
	benchmarkIndexedBatch(b, func(b *testing.B, db *DB[indexedMessage], size int) {
		if err := db.Transaction(func(tx *Tx[indexedMessage]) error {
			for sequence := 1; sequence <= size; sequence++ {
				if err := tx.Create(&indexedMessage{
					MessageID: "delete-" + strconv.Itoa(sequence),
					SessionID: "session",
					Seq:       sequence,
				}); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		err := db.DeleteByIndex("session_seq", []any{"session"})
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
	})
}

func BenchmarkIndexedRangeView(b *testing.B) {
	type benchmarkCase struct {
		name      string
		hasCursor bool
		after     int
		want      int
	}

	for _, benchmark := range []benchmarkCase{
		{name: "full", want: 100_000},
		{name: "delta-10", hasCursor: true, after: 99_990, want: 10},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			originalDir := DataDir
			DataDir = b.TempDir()
			b.Cleanup(func() {
				DataDir = originalDir
				resetRegistries()
			})

			resetRegistries()
			if err := Register[indexedMessage](); err != nil {
				b.Fatal(err)
			}

			db := Open[indexedMessage]()
			if err := db.Transaction(func(tx *Tx[indexedMessage]) error {
				for sequence := 1; sequence <= 100_000; sequence++ {
					if err := tx.Create(&indexedMessage{
						MessageID: "range-" + strconv.Itoa(sequence),
						SessionID: "session",
						Seq:       sequence,
					}); err != nil {
						return err
					}
				}

				return nil
			}); err != nil {
				b.Fatal(err)
			}

			visit := func(entries []*indexedMessage) error {
				if len(entries) != benchmark.want {
					b.Fatalf("entries = %d, want %d", len(entries), benchmark.want)
				}

				return nil
			}
			b.ResetTimer()
			for b.Loop() {
				var err error
				if !benchmark.hasCursor {
					err = db.ViewRange("session_seq", []any{"session"}, visit)
				} else {
					err = db.ViewRangeAfter("session_seq", []any{"session"}, benchmark.after, visit)
				}

				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkScalarLookupDuringIndexedHydration(b *testing.B) {
	originalDir := DataDir
	b.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	for range b.N {
		b.StopTimer()
		resetRegistries()
		DataDir = b.TempDir()
		if err := Register[indexedMessage](); err != nil {
			b.Fatal(err)
		}

		db := Open[indexedMessage]()
		stable := &indexedMessage{MessageID: "stable", SessionID: "stable", Seq: 1}
		if err := db.Create(stable); err != nil {
			b.Fatal(err)
		}

		if err := createIndexedBatch(db, "seed", 100_000); err != nil {
			b.Fatal(err)
		}

		done := make(chan error, 1)
		start := make(chan struct{})
		go func() {
			close(start)
			done <- createIndexedBatch(db, "hydrate", 50_000)
		}()
		<-start

		latencies := make([]int64, 0, 16_384)
		b.StartTimer()
	benchmarkLoop:
		for {
			select {
			case err := <-done:
				if err != nil {
					b.Fatal(err)
				}

				break benchmarkLoop
			default:
			}

			started := time.Now()
			if _, err := db.FindOneBy("MessageID", "stable"); err != nil {
				b.Fatal(err)
			}

			latencies = append(latencies, time.Since(started).Nanoseconds())
		}

		b.StopTimer()

		sort.Slice(latencies, func(left, right int) bool {
			return latencies[left] < latencies[right]
		})
		b.ReportMetric(float64(nearestRank(latencies, 50)), "p50-ns/op")
		b.ReportMetric(float64(nearestRank(latencies, 95)), "p95-ns/op")
		b.ReportMetric(float64(nearestRank(latencies, 99)), "p99-ns/op")
	}
}

func createIndexedBatch(db *DB[indexedMessage], prefix string, size int) error {
	return db.Transaction(func(tx *Tx[indexedMessage]) error {
		for sequence := 1; sequence <= size; sequence++ {
			if err := tx.Create(&indexedMessage{
				MessageID: prefix + "-" + strconv.Itoa(sequence),
				SessionID: prefix,
				Seq:       sequence,
			}); err != nil {
				return err
			}
		}

		return nil
	})
}

func benchmarkIndexedBatch(
	b *testing.B,
	operation func(*testing.B, *DB[indexedMessage], int),
) {
	b.Helper()
	originalDir := DataDir
	b.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				resetRegistries()
				DataDir = b.TempDir()
				if err := Register[indexedMessage](); err != nil {
					b.Fatal(err)
				}

				operation(b, Open[indexedMessage](), size)
			}
		})
	}
}
