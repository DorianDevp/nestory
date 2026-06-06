package nestory

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"os"
	"reflect"
	"sync"
)

// Append-only log so a commit is durable without rewriting a 512-row chunk:
// append one frame, fsync, then mutate memory. Chunk files are the snapshot;
// the WAL holds committed mutations since the last Flush (which compacts +
// truncates it). On load, chunks hydrate first, then the WAL replays on top.
// One frame == one transaction, so a torn tail is discarded whole on replay.

type walRow struct {
	Id  int64
	Row []byte // gob bytes of the flat schema row
}

type walFrame struct {
	Rows []walRow
}

type wal struct {
	mu   sync.Mutex
	path string
	f    *os.File // opened lazily, closed on truncate
}

func openWAL(path string) *wal { return &wal{path: path} }

// appendFrame writes one frame ([uint32 len][gob walFrame]) and fsyncs. The fsync
// is the durable commit point.
func (w *wal) appendFrame(rec walFrame) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.f == nil {
		f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}

		w.f = f
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(rec); err != nil {
		return err
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(buf.Len()))

	if _, err := w.f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(buf.Bytes()); err != nil {
		return err
	}

	return w.f.Sync()
}

func (w *wal) truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.f != nil {
		w.f.Close()
		w.f = nil
	}

	if err := os.Remove(w.path); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

type recoveredRow struct {
	id  int
	row reflect.Value
}

// replayWAL reads every committed frame in order. A torn tail (short read or
// undecodable frame) ends the durable region — that tx never committed.
func replayWAL(path string, rowType reflect.Type) ([]recoveredRow, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	defer f.Close()

	var out []recoveredRow

	for {
		var hdr [4]byte
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			break
		}

		n := binary.BigEndian.Uint32(hdr[:])

		body := make([]byte, n)
		if _, err := io.ReadFull(f, body); err != nil {
			break
		}

		var rec walFrame
		if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&rec); err != nil {
			break
		}

		for _, p := range rec.Rows {
			rv := reflect.New(rowType)

			if derr := gob.NewDecoder(bytes.NewReader(p.Row)).DecodeValue(rv); derr != nil {
				return nil, derr
			}

			out = append(out, recoveredRow{id: int(p.Id), row: rv.Elem()})
		}
	}

	return out, nil
}

// encodeRow gob-encodes one flat schema row (no interface boxing, so no
// gob.Register needed).
func encodeRow(v reflect.Value) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).EncodeValue(v); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
