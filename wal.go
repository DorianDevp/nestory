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

type transactionWALRow struct {
	Type string
	walRow
}

type transactionWALFrame struct {
	Rows []transactionWALRow
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

func encodeTransactionWALFrame(rec transactionWALFrame) ([]byte, error) {
	const (
		frameHeader = 4
		rowHeader   = 16
		maxUint32   = uint64(^uint32(0))
	)

	bodySize := uint64(frameHeader)
	for _, row := range rec.Rows {
		bodySize += rowHeader + uint64(len(row.Type)) + uint64(len(row.Row))
	}

	if bodySize > maxUint32 || uint64(len(rec.Rows)) > maxUint32 {
		return nil, fmt.Errorf("nestory: transaction WAL frame is too large")
	}

	frame := make([]byte, frameHeader+int(bodySize))
	binary.BigEndian.PutUint32(frame, uint32(bodySize))
	binary.BigEndian.PutUint32(frame[frameHeader:], uint32(len(rec.Rows)))

	offset := frameHeader * 2
	for _, row := range rec.Rows {
		binary.BigEndian.PutUint32(frame[offset:], uint32(len(row.Type)))
		binary.BigEndian.PutUint64(frame[offset+4:], uint64(row.Id))
		binary.BigEndian.PutUint32(frame[offset+12:], uint32(len(row.Row)))
		copy(frame[offset+rowHeader:], row.Type)
		copy(frame[offset+rowHeader+len(row.Type):], row.Row)
		offset += rowHeader + len(row.Type) + len(row.Row)
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
	id      int
	row     reflect.Value
	deleted bool
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
			if p.Id < 0 && len(p.Row) == 0 {
				out = append(out, recoveredRow{id: -int(p.Id), deleted: true})
				continue
			}

			if p.Id <= 0 {
				return nil, fmt.Errorf("nestory: malformed WAL row id %d", p.Id)
			}

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

func decodeTransactionWALFrame(body []byte) ([]transactionWALRow, bool) {
	const rowHeader = 16
	if len(body) < 4 {
		return nil, false
	}

	count := int(binary.BigEndian.Uint32(body))
	if count > (len(body)-4)/rowHeader {
		return nil, false
	}

	rows := make([]transactionWALRow, 0, count)
	offset := 4
	for range count {
		if len(body)-offset < rowHeader {
			return nil, false
		}

		typeSize := int(binary.BigEndian.Uint32(body[offset:]))
		id := int64(binary.BigEndian.Uint64(body[offset+4:]))
		rowSize := int(binary.BigEndian.Uint32(body[offset+12:]))
		offset += rowHeader
		if typeSize > len(body)-offset || rowSize > len(body)-offset-typeSize {
			return nil, false
		}

		typeName := string(body[offset : offset+typeSize])
		offset += typeSize
		rows = append(rows, transactionWALRow{
			Type:   typeName,
			walRow: walRow{Id: id, Row: body[offset : offset+rowSize]},
		})
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

	size, scalar := scalarValueSize(row)
	if !scalar || size == math.MaxInt {
		return 0, false
	}

	return size + 1, true
}

func scalarValueSize(value reflect.Value) (int, bool) {
	switch value.Kind() {
	case reflect.Bool:
		return 1, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return 8, true
	case reflect.String:
		if value.Len() > math.MaxUint32 {
			return 0, false
		}

		return 4 + value.Len(), true
	case reflect.Slice:
		if value.Type().Elem().Kind() != reflect.Uint8 || value.Len() >= math.MaxUint32 {
			return 0, false
		}

		return 4 + value.Len(), true
	case reflect.Array:
		return scalarAggregateSize(value.Len(), value.Index)
	case reflect.Struct:
		for index := range value.NumField() {
			if !value.Type().Field(index).IsExported() {
				return 0, false
			}
		}

		return scalarAggregateSize(value.NumField(), value.Field)
	default:
		return 0, false
	}
}

func scalarAggregateSize(count int, field func(int) reflect.Value) (int, bool) {
	size := 0
	for index := range count {
		fieldSize, scalar := scalarValueSize(field(index))
		if !scalar || size > math.MaxInt-fieldSize {
			return 0, false
		}

		size += fieldSize
	}

	return size, true
}

func encodeScalarRow(out []byte, row reflect.Value) {
	encodeScalarValue(out, row)
}

func encodeScalarValue(out []byte, value reflect.Value) int {
	switch value.Kind() {
	case reflect.Bool:
		if value.Bool() {
			out[0] = 1
		}

		return 1
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		binary.BigEndian.PutUint64(out, uint64(value.Int()))
		return 8
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		binary.BigEndian.PutUint64(out, value.Uint())
		return 8
	case reflect.Float32, reflect.Float64:
		binary.BigEndian.PutUint64(out, math.Float64bits(value.Float()))
		return 8
	case reflect.String:
		text := value.String()
		binary.BigEndian.PutUint32(out, uint32(len(text)))
		return 4 + copy(out[4:], text)
	case reflect.Slice:
		if value.IsNil() {
			binary.BigEndian.PutUint32(out, 0)

			return 4
		}

		binary.BigEndian.PutUint32(out, uint32(value.Len()+1))

		return 4 + copy(out[4:], value.Bytes())
	case reflect.Array:
		return encodeScalarAggregate(out, value.Len(), value.Index)
	case reflect.Struct:
		return encodeScalarAggregate(out, value.NumField(), value.Field)
	default:
		panic("nestory: unsupported scalar WAL value")
	}
}

func encodeScalarAggregate(out []byte, count int, field func(int) reflect.Value) int {
	offset := 0
	for index := range count {
		offset += encodeScalarValue(out[offset:], field(index))
	}

	return offset
}

func decodeScalarRow(data []byte, row reflect.Value) error {
	offset, err := decodeScalarValue(data, row)
	if err != nil {
		return err
	}

	if offset != len(data) {
		return fmt.Errorf("nestory: malformed scalar WAL row")
	}

	return nil
}

func decodeScalarValue(data []byte, value reflect.Value) (int, error) {
	switch value.Kind() {
	case reflect.Bool:
		if len(data) < 1 || data[0] > 1 {
			return 0, fmt.Errorf("nestory: malformed scalar WAL row")
		}

		value.SetBool(data[0] == 1)
		return 1, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if len(data) < 8 {
			return 0, fmt.Errorf("nestory: malformed scalar WAL row")
		}

		value.SetInt(int64(binary.BigEndian.Uint64(data)))
		return 8, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if len(data) < 8 {
			return 0, fmt.Errorf("nestory: malformed scalar WAL row")
		}

		value.SetUint(binary.BigEndian.Uint64(data))
		return 8, nil
	case reflect.Float32, reflect.Float64:
		if len(data) < 8 {
			return 0, fmt.Errorf("nestory: malformed scalar WAL row")
		}

		value.SetFloat(math.Float64frombits(binary.BigEndian.Uint64(data)))
		return 8, nil
	case reflect.String:
		if len(data) < 4 {
			return 0, fmt.Errorf("nestory: malformed scalar WAL row")
		}

		size := int(binary.BigEndian.Uint32(data))
		if size > len(data)-4 {
			return 0, fmt.Errorf("nestory: malformed scalar WAL row")
		}

		value.SetString(string(data[4 : 4+size]))
		return 4 + size, nil
	case reflect.Slice:
		if len(data) < 4 || value.Type().Elem().Kind() != reflect.Uint8 {
			return 0, fmt.Errorf("nestory: malformed scalar WAL row")
		}

		encodedSize := binary.BigEndian.Uint32(data)
		if encodedSize == 0 {
			value.SetZero()

			return 4, nil
		}

		size := int(encodedSize - 1)
		if size > len(data)-4 {
			return 0, fmt.Errorf("nestory: malformed scalar WAL row")
		}

		decoded := reflect.MakeSlice(value.Type(), size, size)
		reflect.Copy(decoded, reflect.ValueOf(data[4:4+size]))
		value.Set(decoded)

		return 4 + size, nil
	case reflect.Array:
		return decodeScalarAggregate(data, value.Len(), value.Index)
	case reflect.Struct:
		return decodeScalarAggregate(data, value.NumField(), value.Field)
	default:
		return 0, fmt.Errorf("nestory: scalar WAL row contains %s", value.Kind())
	}
}

func decodeScalarAggregate(data []byte, count int, field func(int) reflect.Value) (int, error) {
	offset := 0
	for index := range count {
		read, err := decodeScalarValue(data[offset:], field(index))
		if err != nil {
			return 0, err
		}

		offset += read
	}

	return offset, nil
}
