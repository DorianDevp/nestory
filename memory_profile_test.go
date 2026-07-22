package nestory

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
)

const (
	memoryProfileWorkerEnv = "NESTORY_MEMORY_PROFILE_WORKER"
	memoryProfileRunEnv    = "NESTORY_MEMORY_PROFILE"
)

type memoryProfileEntry struct {
	Id        int    `key:"primary"`
	MessageID string `key:"unique"`
	SessionID int    `index:"session_seq,1,unique"`
	Seq       int    `index:"session_seq,2,unique"`
	Kind      string
	Data      []byte
}

func (entry memoryProfileEntry) GetId() int { return entry.Id }

type memoryProfileSample struct {
	Variant      string `json:"variant"`
	Entries      int    `json:"entries"`
	PayloadBytes uint64 `json:"payloadBytes"`
	HeapAlloc    uint64 `json:"heapAlloc"`
	HeapInuse    uint64 `json:"heapInuse"`
	RSS          uint64 `json:"rss"`
}

func TestIndexedMemoryProfile(t *testing.T) {
	if worker := os.Getenv(memoryProfileWorkerEnv); worker != "" {
		runMemoryProfileWorker(t, worker)

		return
	}

	if os.Getenv(memoryProfileRunEnv) == "" {
		t.Skip("set NESTORY_MEMORY_PROFILE=1 to run the indexed memory profile")
	}

	variants := []string{"runtime", "store", "primary", "ordered", "unique"}
	samples := make([]memoryProfileSample, 0, len(variants))
	for _, variant := range variants {
		samples = append(samples, memoryProfileSubprocess(t, variant))
	}

	t.Log("component\tbytes/entry\tlive heap\theap in-use\tRSS")
	previous := samples[0]
	t.Logf("runtime\t-\t%s\t%s\t%s", formatBytes(previous.HeapAlloc), formatBytes(previous.HeapInuse), formatBytes(previous.RSS))
	for position := 1; position < len(samples); position++ {
		current := samples[position]
		delta := positiveDifference(current.HeapAlloc, previous.HeapAlloc)
		t.Logf(
			"%s\t%d\t%s\t%s\t%s",
			current.Variant,
			delta/uint64(current.Entries),
			formatBytes(current.HeapAlloc),
			formatBytes(current.HeapInuse),
			formatBytes(current.RSS),
		)
		previous = current
	}

	full := samples[len(samples)-1]
	t.Logf(
		"logical payload: %s (%d B/entry); total live heap: %d B/entry",
		formatBytes(full.PayloadBytes),
		full.PayloadBytes/uint64(full.Entries),
		full.HeapAlloc/uint64(full.Entries),
	)
}

func memoryProfileSubprocess(t *testing.T, variant string) memoryProfileSample {
	t.Helper()

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	command := exec.Command(executable, "-test.run=^TestIndexedMemoryProfile$", "-test.v")
	command.Env = append(os.Environ(), memoryProfileWorkerEnv+"="+variant)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("memory profile %s: %v\n%s", variant, err, output)
	}

	const marker = "NESTORY_MEMORY_SAMPLE="
	for _, line := range strings.Split(string(output), "\n") {
		position := strings.Index(line, marker)
		if position < 0 {
			continue
		}

		var sample memoryProfileSample
		if err := json.Unmarshal([]byte(line[position+len(marker):]), &sample); err != nil {
			t.Fatalf("decode memory profile %s: %v", variant, err)
		}

		return sample
	}

	t.Fatalf("memory profile %s returned no sample:\n%s", variant, output)

	return memoryProfileSample{}
}

func runMemoryProfileWorker(t *testing.T, variant string) {
	t.Helper()

	entries := memoryProfileIntEnv("NESTORY_MEMORY_ENTRIES", 100_000)
	payloadSize := memoryProfileIntEnv("NESTORY_MEMORY_PAYLOAD_BYTES", 128)
	if entries < 1 || payloadSize < 0 {
		t.Fatal("memory profile sizes must be non-negative and include at least one entry")
	}

	DataDir = t.TempDir()
	resetRegistries()
	registerForTest[memoryProfileEntry](t)
	db := Open[memoryProfileEntry]()
	if variant != "runtime" {
		for position := range entries {
			payload := make([]byte, payloadSize)
			if len(payload) > 0 {
				payload[0] = byte(position)
			}

			entry := &memoryProfileEntry{
				Id:        position + 1,
				MessageID: fmt.Sprintf("msg-%016x", position),
				SessionID: 1,
				Seq:       position + 1,
				Kind:      "assistant",
				Data:      payload,
			}
			db.add(entry)
		}

		if err := db.rebuildSecondaryIndices(); err != nil {
			t.Fatal(err)
		}

		pruneMemoryProfileVariant(db, variant)
	}

	debug.FreeOSMemory()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	sample := memoryProfileSample{
		Variant:      variant,
		Entries:      entries,
		PayloadBytes: uint64(entries * payloadSize),
		HeapAlloc:    stats.HeapAlloc,
		HeapInuse:    stats.HeapInuse,
		RSS:          processRSS(),
	}
	runtime.KeepAlive(db)

	encoded, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("NESTORY_MEMORY_SAMPLE=%s", encoded)
}

func pruneMemoryProfileVariant(db *DB[memoryProfileEntry], variant string) {
	switch variant {
	case "store":
		db.resById = nil
		db.secondary = nil
	case "primary":
		db.secondary = nil
	case "ordered":
		for _, index := range db.secondary {
			index.compositeKeys = nil
			index.stringKeys = nil
			index.signedKeys = nil
			index.unsignedKeys = nil
			index.boolKeys = [2]int{}
		}
	case "unique":
	case "runtime":
	default:
		panic("unknown memory profile variant: " + variant)
	}
}

func processRSS() uint64 {
	status, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}

	fields := strings.Fields(string(status))
	if len(fields) < 2 {
		return 0
	}

	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}

	return pages * uint64(os.Getpagesize())
}

func memoryProfileIntEnv(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		panic(fmt.Sprintf("invalid %s: %v", name, err))
	}

	return value
}

func positiveDifference(current, previous uint64) uint64 {
	if current <= previous {
		return 0
	}

	return current - previous
}

func formatBytes(value uint64) string {
	const mebibyte = 1024 * 1024

	return fmt.Sprintf("%.1f MiB", float64(value)/mebibyte)
}
