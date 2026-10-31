package nestory

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// What a table's own log recorded before every commit went to the shared log
// lands after the shared log's rows: an edit or delete made on its own after
// a cross-table create is not undone at the next start.
func TestLegacyWALEditsAndDeletesSurviveTheSharedLog(t *testing.T) {
	created := reflect.ValueOf("created")
	edited := reflect.ValueOf("edited")
	shared := []recoveredRow{{id: 1, row: created}, {id: 2, row: created}, {id: 3, row: created}, {id: 3, deleted: true}}
	own := []recoveredRow{{id: 1, row: edited}, {id: 2, deleted: true}, {id: 3, row: edited}}

	final := map[int]string{}
	for _, row := range mergeLegacyWAL(shared, own) {
		if row.deleted {
			delete(final, row.id)
			continue
		}
		final[row.id] = row.row.String()
	}
	if final[1] != "edited" {
		t.Fatalf("an edit made on its own was reverted: %v", final)
	}
	if _, alive := final[2]; alive {
		t.Fatalf("a row deleted on its own came back: %v", final)
	}
	if _, alive := final[3]; alive {
		t.Fatalf("an older row of the table's own log outlived a delete in the shared log: %v", final)
	}
	if got := mergeLegacyWAL(shared, nil); len(got) != len(shared) {
		t.Fatalf("without a legacy log the shared log replays as it is")
	}
}

// The high-water mark outlives the log it was read from, and a damaged one
// counts as none rather than as a smaller number.
func TestHighWaterMarkIsKept(t *testing.T) {
	dir := t.TempDir()
	if got := readHighWater(dir); got != 0 {
		t.Fatalf("no file should read as 0, got %d", got)
	}
	if err := writeHighWater(dir, 41); err != nil {
		t.Fatal(err)
	}
	if err := writeHighWater(dir, 42); err != nil {
		t.Fatal(err)
	}
	if got := readHighWater(dir); got != 42 {
		t.Fatalf("read %d, want 42", got)
	}
	if err := os.WriteFile(filepath.Join(dir, highWaterName), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := readHighWater(dir); got != 0 {
		t.Fatalf("a damaged mark read as %d", got)
	}
}
