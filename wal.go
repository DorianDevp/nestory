package nestory

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
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
		file := w.f
		w.f = nil
		if err := file.Close(); err != nil {
			return err
		}
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

	defer func() { _ = f.Close() }()

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
			rv, derr := decodeRow(p.Row, rowType)
			if derr != nil {
				return nil, derr
			}

			out = append(out, recoveredRow{id: int(p.Id), row: rv})
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

const (
	rowEncodingGob byte = iota
	rowEncodingScalar
)

// encodeRow uses a compact codec for scalar schemas and falls back to gob for
// arbitrary Go fields.
func encodeRow(v reflect.Value) ([]byte, error) {
	if size, scalar := scalarRowSize(v); scalar {
		row := make([]byte, size)
		row[0] = rowEncodingScalar
		encodeScalarRow(row[1:], v)

		return row, nil
	}

	var buf bytes.Buffer
	buf.WriteByte(rowEncodingGob)
	if err := gob.NewEncoder(&buf).EncodeValue(v); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeRow(data []byte, rowType reflect.Type) (reflect.Value, error) {
	if len(data) == 0 {
		return reflect.Value{}, fmt.Errorf("nestory: empty WAL row")
	}

	row := reflect.New(rowType)
	switch data[0] {
	case rowEncodingGob:
		if err := gob.NewDecoder(bytes.NewReader(data[1:])).DecodeValue(row); err != nil {
			return reflect.Value{}, err
		}
	case rowEncodingScalar:
		if err := decodeScalarRow(data[1:], row.Elem()); err != nil {
			return reflect.Value{}, err
		}
	default:
		return reflect.Value{}, fmt.Errorf("nestory: unknown WAL row encoding %d", data[0])
	}

	return row.Elem(), nil
}

func scalarRowSize(row reflect.Value) (int, bool) {
	if row.Kind() != reflect.Struct {
		return 0, false
	}

	size := 1
	for i := range row.NumField() {
		field := row.Field(i)
		switch field.Kind() {
		case reflect.Bool:
			size++
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			size += 8
		case reflect.String:
			if field.Len() > math.MaxUint32 || size > math.MaxInt-4-field.Len() {
				return 0, false
			}

			size += 4 + field.Len()
		default:
			return 0, false
		}
	}

	return size, true
}

func encodeScalarRow(out []byte, row reflect.Value) {
	offset := 0
	for i := range row.NumField() {
		field := row.Field(i)
		switch field.Kind() {
		case reflect.Bool:
			if field.Bool() {
				out[offset] = 1
			}

			offset++
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			binary.BigEndian.PutUint64(out[offset:], uint64(field.Int()))
			offset += 8
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			binary.BigEndian.PutUint64(out[offset:], field.Uint())
			offset += 8
		case reflect.Float32, reflect.Float64:
			binary.BigEndian.PutUint64(out[offset:], math.Float64bits(field.Float()))
			offset += 8
		case reflect.String:
			value := field.String()
			binary.BigEndian.PutUint32(out[offset:], uint32(len(value)))
			offset += 4
			offset += copy(out[offset:], value)
		}
	}
}

func decodeScalarRow(data []byte, row reflect.Value) error {
	offset := 0
	for i := range row.NumField() {
		field := row.Field(i)
		switch field.Kind() {
		case reflect.Bool:
			if len(data)-offset < 1 || data[offset] > 1 {
				return fmt.Errorf("nestory: malformed scalar WAL row")
			}

			field.SetBool(data[offset] == 1)
			offset++
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if len(data)-offset < 8 {
				return fmt.Errorf("nestory: malformed scalar WAL row")
			}

			field.SetInt(int64(binary.BigEndian.Uint64(data[offset:])))
			offset += 8
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if len(data)-offset < 8 {
				return fmt.Errorf("nestory: malformed scalar WAL row")
			}

			field.SetUint(binary.BigEndian.Uint64(data[offset:]))
			offset += 8
		case reflect.Float32, reflect.Float64:
			if len(data)-offset < 8 {
				return fmt.Errorf("nestory: malformed scalar WAL row")
			}

			field.SetFloat(math.Float64frombits(binary.BigEndian.Uint64(data[offset:])))
			offset += 8
		case reflect.String:
			if len(data)-offset < 4 {
				return fmt.Errorf("nestory: malformed scalar WAL row")
			}

			size := int(binary.BigEndian.Uint32(data[offset:]))
			offset += 4
			if size > len(data)-offset {
				return fmt.Errorf("nestory: malformed scalar WAL row")
			}

			field.SetString(string(data[offset : offset+size]))
			offset += size
		default:
			return fmt.Errorf("nestory: scalar WAL row contains %s", field.Kind())
		}
	}

	if offset != len(data) {
		return fmt.Errorf("nestory: malformed scalar WAL row")
	}

	return nil
}
