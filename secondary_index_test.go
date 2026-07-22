package nestory

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
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
