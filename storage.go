package nestory

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Each type persists as a dir of per-chunk gob files: <DataDir>/<Type>/<n>.gob,
// one file per in-memory chunk. Only dirty chunks are rewritten, each in its
// own goroutine; load decodes + inflates every file concurrently and reassembles
// in index order so file ↔ chunk alignment holds.

func chunkDirFor(typeName string) string {
	return filepath.Join(DataDir, typeName)
}

func (db *DB[T]) chunkDir() string { return chunkDirFor(db.name) }

// loadStore reads every chunk file for T and rebuilds the store. Decode +
// inflate run in parallel; assembly is sequential to keep file index == chunk index.
func loadStore[T Entity]() (*chunkStore[T], error) {
	creator := &dbCreator{}
	store := newChunkStore[T]()

	rowType := creator.createSchemaStruct(*new(T)).Type()
	sliceType := reflect.SliceOf(rowType)

	dir := chunkDirFor(reflect.TypeFor[T]().Name())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("nestory: mkdir %s: %w", dir, err)
	}

	paths, err := sortedChunkFiles(dir)
	if err != nil {
		return nil, err
	}

	if len(paths) == 0 {
		return store, nil
	}

	results := make([][]T, len(paths))
	errs := make([]error, len(paths))

	var wg sync.WaitGroup
	for i, p := range paths {
		wg.Add(1)
		go func(i int, p string) {
			defer wg.Done()
			slice, derr := decodeSlice(p, sliceType)
			if derr != nil {
				errs[i] = derr
				return
			}

			vals, ierr := inflateSlice[T](creator, slice)
			if ierr != nil {
				errs[i] = ierr
				return
			}

			results[i] = vals
		}(i, p)
	}

	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}

	for i := range results {
		store.loadChunk(results[i])
	}

	// WAL rows are newer than the snapshot (committed since last compaction), so
	// they win.
	walRows, werr := replayWAL(filepath.Join(dir, "wal.log"), rowType)
	if werr != nil {
		return nil, werr
	}

	if len(walRows) > 0 {
		rows := reflect.MakeSlice(sliceType, 0, len(walRows))
		ids := make([]int, len(walRows))
		for i, wr := range walRows {
			rows = reflect.Append(rows, wr.row)
			ids[i] = wr.id
		}

		vals, ierr := inflateSlice[T](creator, rows)
		if ierr != nil {
			return nil, ierr
		}

		byId := make(map[int]*resourceSlot[T], store.Len())
		store.rangeResources(func(r *resourceSlot[T]) { byId[(*r.item).GetId()] = r })
		for i := range vals {
			if r, ok := byId[ids[i]]; ok {
				*r.item = vals[i]
			}
		}
	}

	return store, nil
}

// sortedChunkFiles lists "<n>.gob" files in dir, ordered by n.
func sortedChunkFiles(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("nestory: read dir %s: %w", dir, err)
	}

	type chunkFile struct {
		idx  int
		path string
	}
	var cfs []chunkFile
	for _, e := range ents {
		if e.IsDir() {
			continue
		}

		name := e.Name()
		if !strings.HasSuffix(name, ".gob") {
			continue
		}

		idx, err := strconv.Atoi(strings.TrimSuffix(name, ".gob"))
		if err != nil {
			continue // ignore stray files (e.g. leftover .tmp, old single-file format)
		}

		cfs = append(cfs, chunkFile{idx, filepath.Join(dir, name)})
	}

	sort.Slice(cfs, func(a, b int) bool { return cfs[a].idx < cfs[b].idx })

	paths := make([]string, len(cfs))
	for k, c := range cfs {
		paths[k] = c.path
	}

	return paths, nil
}

// decodeSlice gob-decodes one chunk file into a sliceType value. Empty file →
// empty slice.
func decodeSlice(path string, sliceType reflect.Type) (reflect.Value, error) {
	f, err := os.Open(path)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("nestory: open %s: %w", path, err)
	}

	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return reflect.Value{}, fmt.Errorf("nestory: stat %s: %w", path, err)
	}

	slicePtr := reflect.New(sliceType)
	if info.Size() == 0 {
		return slicePtr.Elem(), nil
	}

	if err := gob.NewDecoder(f).DecodeValue(slicePtr); err != nil {
		return reflect.Value{}, fmt.Errorf("nestory: decode %s: %w", path, err)
	}

	return slicePtr.Elem(), nil
}

// save writes every dirty chunk in parallel, then marks them clean. Clean chunks
// are skipped — the win over a whole-file rewrite. Assumes no commit is running
// concurrently (admin-only, like Flush).
func (db *DB[T]) save() error {
	idxs := db.store.dirtyIndices()
	if len(idxs) == 0 {
		return nil
	}

	dir := db.chunkDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("nestory: mkdir %s: %w", dir, err)
	}

	sliceType := reflect.SliceOf(db.createSchemaStruct().Type())

	errs := make([]error, len(idxs))
	var wg sync.WaitGroup
	for k, ci := range idxs {
		wg.Add(1)
		go func(k, ci int) {
			defer wg.Done()
			errs[k] = db.saveChunk(dir, ci, sliceType)
		}(k, ci)
	}

	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	db.store.clearDirty(idxs)

	return nil
}

// saveChunk writes one chunk atomically: encode to "<ci>.gob.tmp", fsync, rename
// onto "<ci>.gob". A crash mid-encode leaves the previous good file untouched.
func (db *DB[T]) saveChunk(dir string, ci int, sliceType reflect.Type) error {
	var contents any
	if db.directSchema {
		rows := make([]T, 0, db.store.chunkSlots(ci))
		db.store.chunkLive(ci, func(p *T) {
			rows = append(rows, *p)
		})
		contents = rows
	} else {
		slice := reflect.MakeSlice(sliceType, 0, db.store.chunkSlots(ci))
		db.store.chunkLive(ci, func(p *T) {
			slice = reflect.Append(slice, db.normalizeToSchema(*p))
		})
		contents = slice.Interface()
	}

	finalPath := filepath.Join(dir, fmt.Sprintf("%d.gob", ci))
	tmpPath := finalPath + ".tmp"

	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("nestory: create %s: %w", tmpPath, err)
	}

	if err := gob.NewEncoder(file).Encode(contents); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("nestory: encode %s: %w", tmpPath, err)
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("nestory: fsync %s: %w", tmpPath, err)
	}

	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("nestory: close %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("nestory: rename %s -> %s: %w", tmpPath, finalPath, err)
	}

	return nil
}
