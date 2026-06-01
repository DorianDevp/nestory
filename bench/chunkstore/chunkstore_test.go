package chunkstore

import (
	"fmt"
	"testing"
)

// Row is a 64-byte (one cache line) record, sized so scan benchmarks actually
// exercise memory locality rather than fitting trivially in registers.
type Row struct {
	Id   int      // 8
	A, B int64    // 16
	Name [40]byte // 40  -> 64 total
}

var (
	sink   int64
	ptrOut *Row
)

const benchN = 1_000_000

var limits = []int{256, 1024, 4096}

// ---------- CORRECTNESS: the whole point ----------

// TestPointerStability_SliceVsChunk demonstrates the core difference: a
// pointer into a plain []T dangles after the slice grows, while a pointer
// into the chunk store keeps tracking its element.
func TestPointerStability_SliceVsChunk(t *testing.T) {
	// --- plain []T ---
	s := make([]Row, 0, 1) // cap 1 forces reallocation on the next append
	s = append(s, Row{Id: 1, A: 100})
	p := &s[0]
	for i := 0; i < 10_000; i++ {
		s = append(s, Row{Id: i + 2})
	}
	s[0].A = 999 // write through the CURRENT backing array

	if p.A == 999 {
		t.Errorf("[]T: pointer unexpectedly still aliased s[0]; can't demonstrate the bug on this run")
	} else {
		t.Logf("[]T: pointer is STALE after growth (p.A=%d, want 999) — this is nestory's current Tier-1.1 bug", p.A)
	}

	// --- chunk store ---
	cs := New[Row](1024)
	q := cs.Append(Row{Id: 1, A: 100})
	for i := 0; i < 10_000; i++ {
		cs.Append(Row{Id: i + 2})
	}
	cs.At(0).A = 999 // write through the store

	if q.A != 999 {
		t.Fatalf("chunkstore: pointer is STALE (q.A=%d, want 999) — arena is broken", q.A)
	}
	t.Logf("chunkstore: pointer survived %d appends and sees the write (q.A=%d)", 10_000, q.A)
}

func TestTombstoneDelete(t *testing.T) {
	cs := New[Row](8)
	ptrs := make([]*Row, 20)
	for i := 0; i < 20; i++ {
		ptrs[i] = cs.Append(Row{Id: i})
	}
	cs.Delete(5)
	cs.Delete(12)

	if cs.Len() != 18 {
		t.Errorf("Len() = %d, want 18", cs.Len())
	}
	if cs.Cap() != 20 {
		t.Errorf("Cap() = %d, want 20", cs.Cap())
	}
	// Deleted slots are skipped by Range...
	seen := map[int]bool{}
	cs.Range(func(_ int, p *Row) { seen[p.Id] = true })
	if seen[5] || seen[12] {
		t.Errorf("Range yielded a tombstoned element: %v", seen)
	}
	if len(seen) != 18 {
		t.Errorf("Range visited %d live elements, want 18", len(seen))
	}
	// ...but the surviving pointers are all still valid.
	if ptrs[5].Id != 5 || ptrs[12].Id != 12 {
		t.Errorf("tombstoned values were moved/clobbered")
	}
	if ptrs[19].Id != 19 {
		t.Errorf("live pointer past a deletion is stale: %d", ptrs[19].Id)
	}
}

// ---------- APPEND ----------

func BenchmarkAppend_Slice(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var s []Row
		for j := 0; j < benchN; j++ {
			s = append(s, Row{Id: j, A: int64(j)})
		}
		sink += s[benchN-1].A
	}
}

// BenchmarkAppend_SlicePresized is the best case for []T: capacity known up
// front, so zero reallocation. Included as the lower bound.
func BenchmarkAppend_SlicePresized(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := make([]Row, 0, benchN)
		for j := 0; j < benchN; j++ {
			s = append(s, Row{Id: j, A: int64(j)})
		}
		sink += s[benchN-1].A
	}
}

func BenchmarkAppend_Chunk(b *testing.B) {
	for _, limit := range limits {
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cs := New[Row](limit)
				for j := 0; j < benchN; j++ {
					cs.Append(Row{Id: j, A: int64(j)})
				}
				sink += cs.At(benchN - 1).A
			}
		})
	}
}

// ---------- SCAN (locality) ----------

func BenchmarkScan_Slice(b *testing.B) {
	s := make([]Row, 0, benchN)
	for j := 0; j < benchN; j++ {
		s = append(s, Row{Id: j, A: int64(j)})
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var sum int64
		for k := range s {
			sum += s[k].A
		}
		sink += sum
	}
}

// BenchmarkScan_ChunkDirect is the apples-to-apples locality test: nested
// range over the raw chunk slices, no closure, no tombstone check.
func BenchmarkScan_ChunkDirect(b *testing.B) {
	for _, limit := range limits {
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			cs := New[Row](limit)
			for j := 0; j < benchN; j++ {
				cs.Append(Row{Id: j, A: int64(j)})
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var sum int64
				for _, ch := range cs.Chunks() {
					for k := range ch {
						sum += ch[k].A
					}
				}
				sink += sum
			}
		})
	}
}

// BenchmarkScan_ChunkRange measures the ergonomic Range API: includes the
// per-element closure call and the tombstone check.
func BenchmarkScan_ChunkRange(b *testing.B) {
	for _, limit := range limits {
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			cs := New[Row](limit)
			for j := 0; j < benchN; j++ {
				cs.Append(Row{Id: j, A: int64(j)})
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var sum int64
				cs.Range(func(_ int, p *Row) { sum += p.A })
				sink += sum
			}
		})
	}
}
