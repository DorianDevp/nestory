# nestory — hardening backlog

Known gaps and follow-ups, roughly in priority order. Data-safety items first.
This is the "don't lose track of it" list; the README's Roadmap is the
public-facing summary.

## ✅ Recently done

- [x] Atomic save — tmp + fsync + rename (`storage.go`).
- [x] Chunked-arena backing store — stable `*T` addresses across growth, so
      relations never dangle on insert (`chunkstore.go`). Regression-locked by
      `TestPointerStableAcrossInserts`.
- [x] O(1) auto-id via monotonic counter, seeded from disk on `Open`
      (`db.go: seedCounter`). No reuse across restarts. Locked by
      `TestRegression_CounterSeededAfterReload`.
- [x] `FindOneBy("Id", …)` uses the index (O(1)); the per-item goroutine scan in
      the old write path is gone.
- [x] Tombstone deletes — no in-place shift, surviving pointers stay valid.

## P0 — correctness & data safety

- [ ] **Goroutine safety.** `gb.mu` is only taken in `Add`; `Flush`,
      `AddToPersistQueue`, `FindOneBy`, `Filter`, `PatchById`, `QueueDelete` and
      the `chunkStore` are all unsynchronized. Either wire `mu` through every
      read/write path (RLock for reads) or document the store as single-
      goroutine. Add a `-race` test once done.
- [ ] **Dangling FK on delete.** `QueueDelete` removes a parent without touching
      children that `relto` it. On the next `Open`, `fillRelation` (`relations.go`)
      `log.Panicf`s on the unresolved FK → the DB becomes unopenable. Need a
      policy: cascade-delete, null-the-FK, or skip-with-warning (and make
      `fillRelation` tolerant rather than panicking).
- [ ] **Cross-base atomicity.** Each `DB.Flush` writes its own file; a crash
      between flushing two related bases leaves a half-committed graph. No way to
      roll back. Needs a multi-file commit (e.g. write all tmp files, fsync,
      then rename together) or an explicit transaction boundary.
- [ ] **Directory fsync.** `save()` fsyncs the file and renames, but does not
      fsync the parent directory — after a crash the rename can be lost even
      though the bytes were durable. Open + `Sync()` the dir after rename.

## P1 — robustness & error handling

- [ ] **Stop panicking in library paths.** `CreateDB` (`storage.go`),
      `fillRelation`/`NormalizeToSchema`/`createSchemaStruct` (`schema.go`),
      `Register`/`Open` (`db.go`), and especially `Flush`'s `log.Panicln` on a
      save error (`persist.go`) all kill the host process on ordinary runtime
      failures. Return errors instead; reserve panic for true programmer errors.
- [ ] **`merge` can't write zero values.** It skips zero-valued fields
      (`persist.go`), so you can never patch a field back to `0`/`""`/`false`.
      Consider a field mask or pointer-field semantics for partial updates.
- [ ] **`FindOneBy` on a non-comparable field panics.** `f.Interface() == withValue`
      (`query.go`) panics if the keyed field is a slice/map. Guard with a
      comparability check.
- [ ] **Error-string conventions.** `QueueDelete`/`PatchById` return capitalized,
      newline-terminated errors (`persist.go`). Lowercase, no trailing newline.

## P2 — API surface & cleanup

- [ ] **Shrink the public surface.** `Index`, `Indices`, `Name`, `Identifier`,
      `Type` are exported and mutable; callers can corrupt internal state.
      Unexport what isn't part of the contract; expose behavior via methods.
- [ ] **Dead code.** `GetEntityRegistry`, `baseRegistry`, `isInterfaceSchemaCompliant`,
      the unused `Type`/`schemaStruct`/`creator` fields — remove before 1.0.
- [ ] **Quiet by default.** `log.*`/`fmt.Print*` on every Flush/merge/relation
      (`persist.go`, `relations.go`, `storage.go`). Inject a `*slog.Logger` or
      drop the chatter; a library shouldn't write to stdout.
- [ ] **`AddToPersistQueue` always returns `nil` error** — either make it
      meaningful or drop the return.

## P2 — performance

- [ ] **Whole-file rewrite on every `Flush`** (write amplification: cost scales
      with dataset size, not change size). Acceptable for the < 100 MB target;
      revisit only if a profile demands incremental/segmented snapshots.
- [ ] **In-memory tombstone reclamation.** Deleted slots stay resident until the
      next `Open` (disk is already compacted, since `save` skips tombstones). Add
      an explicit `Compact()` for long-running processes that delete a lot
      (note: it invalidates pointers, so only at a safe rebuild point).

## P3 — features (roadmap)

- [ ] Secondary indexes (only `Id` is indexed today).
- [ ] Schema migration (add/remove/rename fields without losing the gob file).
- [ ] Versioned snapshots + event-offset metadata; `Apply(event, handler)` /
      `AutoSnapshot` for event-sourced read models.

## Testing gaps

- [ ] `-race` test once locking lands.
- [ ] Delete path (`QueueDelete` + Flush tombstone), including delete-then-reload.
- [ ] `PatchById` partial-update semantics (incl. the zero-value limitation).
- [ ] Error paths (currently they panic, so they're untestable until P1 lands).
- [ ] Real same-machine comparison harness under `bench/` vs SQLite / bbolt /
      go-memdb (web figures are in `bench/COMPARISON.md` as placeholders).
