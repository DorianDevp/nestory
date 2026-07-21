package nestory

import (
	"reflect"
	"testing"
)

func BenchmarkWALAppend(b *testing.B) {
	log := openWAL(b.TempDir() + "/wal.log")
	row, err := encodeRow(reflect.ValueOf(walTestRow{Id: 1, Name: "nestory"}))
	if err != nil {
		b.Fatal(err)
	}

	frame := walFrame{Rows: []walRow{{Id: 1, Row: row}}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := log.appendFrame(frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScalarRowEncoding(b *testing.B) {
	row := reflect.ValueOf(walTestRow{Id: 1, Name: "nestory"})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := encodeRow(row); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScalarSchemaEncoding(b *testing.B) {
	db := newBenchDB(b, 1)
	item := benchItem{Id: 1, Name: "nestory", Email: "bench@nestory.dev", Age: 7}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := encodeRow(db.normalizeToSchema(item)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCommitChangeDetection(b *testing.B) {
	db := newBenchDB(b, 1)
	before := &benchItem{Id: 1, Name: "nestory", Email: "bench@nestory.dev", Age: 7}
	after := *before
	after.Age++
	resources := []touchedResource{{
		dbName: db.name, id: before.Id, original: before, work: &after,
	}}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if entityStateEqual(resources[0].original, resources[0].work) {
			b.Fatal("change was not detected")
		}

		changed, err := resourcesChangeRelationGraph(resources)
		if err != nil || changed {
			b.Fatalf("relation graph changed: %v", err)
		}
	}
}
