package nestory

import (
	"encoding/binary"
	"os"
	"reflect"
	"testing"
)

type walTestRow struct {
	Id   int
	Name string
}

type walComplexRow struct {
	Id     int
	Values []string
}

func (row walComplexRow) GetId() int { return row.Id }

func TestWALRowCodecs(t *testing.T) {
	tests := []struct {
		name     string
		row      any
		encoding byte
	}{
		{name: "scalar", row: walTestRow{Id: -7, Name: "nestory"}, encoding: rowEncodingScalar},
		{name: "gob fallback", row: walComplexRow{Id: 9, Values: []string{"a", "b"}}, encoding: rowEncodingGob},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := reflect.ValueOf(test.row)
			encoded, err := encodeRow(value)
			if err != nil {
				t.Fatal(err)
			}

			if encoded[0] != test.encoding {
				t.Fatalf("encoding = %d, want %d", encoded[0], test.encoding)
			}

			decoded, err := decodeRow(encoded, value.Type())
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(decoded.Interface(), test.row) {
				t.Fatalf("decoded row = %#v, want %#v", decoded.Interface(), test.row)
			}
		})
	}
}

func TestScalarWALRowRejectsCorruption(t *testing.T) {
	encoded, err := encodeRow(reflect.ValueOf(walTestRow{Id: 1, Name: "row"}))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := decodeRow(encoded[:len(encoded)-1], reflect.TypeOf(walTestRow{})); err == nil {
		t.Fatal("accepted truncated scalar row")
	}

	if _, err := decodeRow([]byte{99}, reflect.TypeOf(walTestRow{})); err == nil {
		t.Fatal("accepted unknown row encoding")
	}
}

func TestWALFrameRoundTrip(t *testing.T) {
	want := walFrame{Rows: []walRow{
		{Id: 7, Row: []byte("first")},
		{Id: 42, Row: []byte("second")},
	}}

	frame, err := encodeWALFrame(want)
	if err != nil {
		t.Fatal(err)
	}

	bodySize := int(binary.BigEndian.Uint32(frame))
	if bodySize != len(frame)-4 {
		t.Fatalf("body size = %d, want %d", bodySize, len(frame)-4)
	}

	got, valid := decodeWALFrame(frame[4:])
	if !valid {
		t.Fatal("encoded WAL frame was rejected")
	}

	if !reflect.DeepEqual(got, want.Rows) {
		t.Fatalf("decoded rows = %#v, want %#v", got, want.Rows)
	}
}

func TestWALFrameRejectsMalformedBody(t *testing.T) {
	valid, err := encodeWALFrame(walFrame{Rows: []walRow{{Id: 1, Row: []byte("row")}}})
	if err != nil {
		t.Fatal(err)
	}

	tests := [][]byte{
		nil,
		valid[4 : len(valid)-1],
		append(valid[4:], 0),
	}
	for _, body := range tests {
		if _, ok := decodeWALFrame(body); ok {
			t.Fatalf("accepted malformed body %x", body)
		}
	}
}

func TestReplayWALIgnoresTornTail(t *testing.T) {
	path := t.TempDir() + "/wal.log"
	log := openWAL(path)

	first := walTestRow{Id: 1, Name: "committed"}
	firstBytes, err := encodeRow(reflect.ValueOf(first))
	if err != nil {
		t.Fatal(err)
	}

	if err := log.appendFrame(walFrame{Rows: []walRow{{Id: int64(first.Id), Row: firstBytes}}}); err != nil {
		t.Fatal(err)
	}

	if err := log.f.Close(); err != nil {
		t.Fatal(err)
	}

	log.f = nil
	secondBytes, err := encodeRow(reflect.ValueOf(walTestRow{Id: 2, Name: "torn"}))
	if err != nil {
		t.Fatal(err)
	}

	torn, err := encodeWALFrame(walFrame{Rows: []walRow{{Id: 2, Row: secondBytes}}})
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.Write(torn[:len(torn)-1]); err != nil {
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := replayWAL(path, reflect.TypeOf(walTestRow{}))
	if err != nil {
		t.Fatal(err)
	}

	if len(recovered) != 1 {
		t.Fatalf("recovered %d rows, want 1", len(recovered))
	}

	got := recovered[0].row.Interface().(walTestRow)
	if got != first {
		t.Fatalf("recovered row = %#v, want %#v", got, first)
	}
}

func TestComplexDirectSchemaWALSurvivesReload(t *testing.T) {
	originalDir := DataDir
	DataDir = t.TempDir()
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	resetRegistries()
	if err := Register[walComplexRow](); err != nil {
		t.Fatal(err)
	}

	db := Open[walComplexRow]()
	entity := &walComplexRow{Values: []string{"snapshot"}}
	db.Unsafe().Create(entity)
	if err := db.Unsafe().Flush(); err != nil {
		t.Fatal(err)
	}

	if err := db.UpdateWithin(entity.Id, func(row *walComplexRow) error {
		row.Values = append(row.Values, "wal")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	resetRegistries()
	if err := Register[walComplexRow](); err != nil {
		t.Fatal(err)
	}

	reloaded := Open[walComplexRow]()
	got, err := reloaded.Get(entity.Id)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"snapshot", "wal"}
	if !reflect.DeepEqual(got.Values, want) {
		t.Fatalf("reloaded values = %#v, want %#v", got.Values, want)
	}
}
