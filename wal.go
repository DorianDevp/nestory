package nestory

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
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

// appendFrame writes one length-prefixed binary frame and fsyncs. The fsync is
// the durable commit point.
func (w *wal) appendFrame(rec walFrame) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.f == nil {
		f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}

		w.f = f
	}

	frame, err := encodeWALFrame(rec)
	if err != nil {
		return err
	}

	if _, err := w.f.Write(frame); err != nil {
		return err
	}

	return w.f.Sync()
}

func encodeWALFrame(rec walFrame) ([]byte, error) {
	const (
		frameHeader = 4
		rowHeader   = 12
		maxUint32   = uint64(^uint32(0))
	)

	bodySize := uint64(frameHeader)
	for _, row := range rec.Rows {
		bodySize += rowHeader + uint64(len(row.Row))
	}

	if bodySize > maxUint32 || uint64(len(rec.Rows)) > maxUint32 {
		return nil, fmt.Errorf("nestory: WAL frame is too large")
	}

	frame := make([]byte, frameHeader+int(bodySize))
	binary.BigEndian.PutUint32(frame, uint32(bodySize))
	binary.BigEndian.PutUint32(frame[frameHeader:], uint32(len(rec.Rows)))

	offset := frameHeader * 2
	for _, row := range rec.Rows {
		binary.BigEndian.PutUint64(frame[offset:], uint64(row.Id))
		binary.BigEndian.PutUint32(frame[offset+8:], uint32(len(row.Row)))
		copy(frame[offset+rowHeader:], row.Row)
		offset += rowHeader + len(row.Row)
	}

	return frame, nil
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

		rows, valid := decodeWALFrame(body)
		if !valid {
			break
		}

		for _, p := range rows {
			rv := reflect.New(rowType)

			if derr := gob.NewDecoder(bytes.NewReader(p.Row)).DecodeValue(rv); derr != nil {
				return nil, derr
			}

			out = append(out, recoveredRow{id: int(p.Id), row: rv.Elem()})
		}
	}

	return out, nil
}

func decodeWALFrame(body []byte) ([]walRow, bool) {
	const rowHeader = 12
	if len(body) < 4 {
		return nil, false
	}

	count := int(binary.BigEndian.Uint32(body))
	if count > (len(body)-4)/rowHeader {
		return nil, false
	}

	rows := make([]walRow, 0, count)
	offset := 4
	for range count {
		if len(body)-offset < rowHeader {
			return nil, false
		}

		id := int64(binary.BigEndian.Uint64(body[offset:]))
		rowSize := int(binary.BigEndian.Uint32(body[offset+8:]))
		offset += rowHeader
		if rowSize > len(body)-offset {
			return nil, false
		}

		rows = append(rows, walRow{Id: id, Row: body[offset : offset+rowSize]})
		offset += rowSize
	}

	return rows, offset == len(body)
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
