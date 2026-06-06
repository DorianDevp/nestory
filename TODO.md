# TODO

Stuff I still want to do, roughly most-important first. Data safety before nice-to-haves.

## Done

- [x] Atomic save (tmp + fsync + rename), now per chunk.
- [x] Chunked arena so `*T` addresses stay stable as the dataset grows — relations never dangle.
- [x] Auto-id counter seeded from disk on Open (no id reuse after reload).
- [x] `FindOneBy("Id", …)` hits the index, O(1).
- [x] Tombstone deletes (no shifting, pointers stay valid).
- [x] Per-chunk save in parallel — only dirty chunks get rewritten.
- [x] Optimistic transactions: Get / Update / UpdateWithin, per-row version + mutex,
      first writer wins, ErrConflict + snapshot refresh. Race-tested.
- [x] Write-ahead log per type — a commit is durable without a full Flush.
- [x] Idempotent Open (one DB per type, shared by the engine).
- [x] Benchmarks against SQLite, bbolt, go-memdb, buntdb, badger.

## Data safety — do these first

- [ ] The non-transactional paths (Flush, FindOneBy, Filter, PatchById, the persist/delete
      queues) don't take `mu`. Either lock them or write down that they're single-goroutine.
- [ ] Flush + a running transaction is a data race — compaction reads `item` without the
      per-row lock. For now it's "don't run them at once"; make that real or coordinate the locks.
- [ ] Per-chunk save isn't atomic across chunks. A crash mid-Flush can leave some chunks new,
      some old. Need a generation marker, or stage everything then rename.
- [ ] The WAL is per type, so a transaction touching two types writes two logs, fsynced
      separately — half of it can survive a crash. Need one shared log or a 2-phase marker.
      (Blocks cascade.)
- [ ] Deleting a parent that something points to → next Open panics on the dangling FK.
      Need a policy: cascade delete, null the FK, or skip with a warning.
- [ ] Nobody fsyncs the directory after rename/create, so a crash can lose the rename even
      though the bytes are on disk. Open the dir and Sync it.

## Should fix

- [ ] Stop panicking in the library. fillRelation, schema building, Register/Open, and Flush
      all kill the process on ordinary errors. Return errors instead.
- [ ] `merge` skips zero values, so you can't patch a field back to 0 / "" / false.
- [ ] `FindOneBy` on a slice/map field panics (== on non-comparable). Guard it.
- [ ] A couple of returned errors are capitalized and end in `\n`. Lowercase, no newline.

## Transactions — still open

- [ ] Snapshot is a shallow copy, so only scalar fields are safe to mutate. Relations need a
      cascade-bounded deep copy (the graph is cyclic), with each node tracked by (type, id).
- [ ] apply rewrites the whole row + bumps version; a field diff would only touch what changed.
- [ ] Updating an indexed field doesn't update the index yet.
- [ ] Two separate Gets can't share one transaction (no explicit Begin), and a read-only Get
      never gets cleaned up if Update never comes (timeout? explicit close?).

## Performance

- [ ] Empty Flush still scans the whole store to build the insert-vs-patch set. Skip it when
      the queue is empty.
- [ ] Every commit does a NormalizeToSchema + gob encode under the WAL lock. Cache the row type,
      reuse buffers, maybe batch fsyncs under load.
- [ ] Deleted slots stay in memory until the next Open. A Compact() for long-running processes.

## Later

- [ ] Secondary indexes (only Id is indexed today).
- [ ] Schema migration (add/remove/rename fields without losing the files).
- [ ] Event-sourcing bits: versioned snapshots, Apply(event, handler), AutoSnapshot.

## Tests I'm missing

- [ ] Delete + reload (tombstone, index/resById eviction).
- [ ] PatchById partial updates, including the zero-value limit.
- [ ] WAL torn-tail recovery (truncated last frame dropped, earlier ones replayed).
- [ ] Crash between chunk renames, once cross-chunk atomicity lands.
