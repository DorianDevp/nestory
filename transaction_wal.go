package nestory

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
)

const transactionWALName = "transactions.wal"

type transactionWAL struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

func openTransactionWAL(path string) *transactionWAL {
	return &transactionWAL{path: path}
}

func (w *transactionWAL) appendFrame(rec transactionWALFrame) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.f == nil {
		if err := os.MkdirAll(filepath.Dir(w.path), 0o700); err != nil {
			return err
		}

		file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}

		w.f = file
	}

	frame, err := encodeTransactionWALFrame(rec)
	if err != nil {
		return err
	}

	if _, err := w.f.Write(frame); err != nil {
		return err
	}

	return w.f.Sync()
}

func (w *transactionWAL) truncate() error {
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

func (w *transactionWAL) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.f == nil {
		return nil
	}

	file := w.f
	w.f = nil

	return file.Close()
}

var transactionWALRegistry struct {
	sync.Mutex
	path string
	log  *transactionWAL
}

func sharedTransactionWAL() *transactionWAL {
	path := filepath.Join(DataDir, transactionWALName)
	transactionWALRegistry.Lock()
	defer transactionWALRegistry.Unlock()

	if transactionWALRegistry.log != nil && transactionWALRegistry.path == path {
		return transactionWALRegistry.log
	}

	if transactionWALRegistry.log != nil {
		_ = transactionWALRegistry.log.close()
	}

	transactionWALRegistry.path = path
	transactionWALRegistry.log = openTransactionWAL(path)

	return transactionWALRegistry.log
}

func resetSharedTransactionWAL() {
	transactionWALRegistry.Lock()
	defer transactionWALRegistry.Unlock()

	if transactionWALRegistry.log != nil {
		_ = transactionWALRegistry.log.close()
	}

	transactionWALRegistry.path = ""
	transactionWALRegistry.log = nil
}

func replayTransactionWAL(path, typeName string, rowType reflect.Type) ([]recoveredRow, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	defer func() { _ = file.Close() }()

	var recovered []recoveredRow
	for {
		var header [4]byte
		if _, err := io.ReadFull(file, header[:]); err != nil {
			break
		}

		body := make([]byte, binary.BigEndian.Uint32(header[:]))
		if _, err := io.ReadFull(file, body); err != nil {
			break
		}

		rows, valid := decodeTransactionWALFrame(body)
		if !valid {
			break
		}

		for _, row := range rows {
			if row.Type != typeName {
				continue
			}

			if row.Id < 0 && len(row.Row) == 0 {
				recovered = append(recovered, recoveredRow{id: -int(row.Id), deleted: true})
				continue
			}

			if row.Id <= 0 {
				return nil, fmt.Errorf("nestory: malformed transaction WAL row id %d", row.Id)
			}

			value, err := decodeRow(row.Row, rowType)
			if err != nil {
				return nil, err
			}

			recovered = append(recovered, recoveredRow{id: int(row.Id), row: value})
		}
	}

	return recovered, nil
}
