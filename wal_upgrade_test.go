package nestory

import (
	"reflect"
	"testing"
)

// A struct that gained a field still has to read the rows written before it.
type walOld struct {
	Id    int
	Email string
	Count int64
}

type walNew struct {
	Id    int
	Email string
	Count int64
	Admin bool
	Seen  int64
}

func TestScalarRowSurvivesAnAppendedField(t *testing.T) {
	encoded, err := encodeRow(reflect.ValueOf(walOld{Id: 7, Email: "a@b.c", Count: 42}))
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != rowEncodingScalar {
		t.Fatalf("the row did not take the scalar path: %d", encoded[0])
	}

	value, err := decodeRow(encoded, reflect.TypeFor[walNew]())
	if err != nil {
		t.Fatalf("an older row no longer decodes: %v", err)
	}
	got := value.Interface().(walNew)
	if got.Id != 7 || got.Email != "a@b.c" || got.Count != 42 {
		t.Fatalf("the fields that existed changed: %+v", got)
	}
	if got.Admin || got.Seen != 0 {
		t.Fatalf("the new fields should be zero: %+v", got)
	}
}

func TestScalarRowStillRejectsACorruptedOne(t *testing.T) {
	encoded, err := encodeRow(reflect.ValueOf(walNew{Id: 1, Email: "a@b.c", Count: 2, Admin: true, Seen: 3}))
	if err != nil {
		t.Fatal(err)
	}
	// Cut a field in half: that is damage, not an older layout.
	if _, err := decodeRow(encoded[:len(encoded)-4], reflect.TypeFor[walNew]()); err == nil {
		t.Fatal("a truncated field was accepted")
	}
	// Bytes nobody asked for mean the layout does not match at all.
	if _, err := decodeRow(append(encoded, 0, 0), reflect.TypeFor[walNew]()); err == nil {
		t.Fatal("trailing bytes were accepted")
	}
}
