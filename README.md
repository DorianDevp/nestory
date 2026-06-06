# nestory

Persistent in-memory object graph for Go. The whole dataset lives in RAM as
real structs; relations are real `*T` pointers, rewired from disk on `Open`.
No queries, no joins — you walk the graph.

```go
type User struct {
    Id      int      `key:"primary"`
    Name    string
    Profile *Profile `relto:"Id"`     // o2o / m2o
    Posts   []*Post  `mapby:"UserId"` // o2m
}

// register every type in the graph before Open
nestory.Register[Settings]()
nestory.Register[Profile]()
nestory.Register[Post]()
nestory.Register[User]()

db := nestory.Open[User]()
u, _ := db.FindOneBy("Id", 1)

u.Profile.Settings.Theme // already wired, no second lookup
u.Posts[0].Title         // slice already filled
```

Writes go through an optimistic transaction:

```go
// snapshot -> mutate a detached copy -> commit
db.UpdateWithin(1, func(u *User) { u.Name = "bob" })
```

First commit wins; a concurrent one gets `ErrConflict` and a refreshed snapshot.

## How it works

- Entities live in a chunked arena (fixed 512-row blocks, never moved), so a
  `*T` you hold never dangles when the dataset grows. That's what makes the
  pointer graph safe.
- Each type persists as a directory of per-chunk gob snapshots
  (`<Type>/<n>.gob`). Only changed chunks are rewritten, in parallel.
- Transactional commits are durable via a per-type write-ahead log: append one
  fsynced frame, then mutate memory. `Open` replays the log over the snapshots;
  `Flush` compacts the chunks and truncates the log.

## Tags

| Tag             | Where            | Meaning |
|-----------------|------------------|---------|
| `key:"primary"` | `int` field      | Primary key. Required on every entity. |
| `relto:"Id"`    | pointer field    | o2o / m2o. Stored as a flattened FK, rehydrated to a real `*T` on load. |
| `mapby:"UserId"`| slice-of-pointer | o2m. On load, filled with every `*T` whose `UserId` equals this entity's `Id`. |

For m2m, use a junction entity with two plain `int` FKs and `mapby` on each side.

## Status

Alpha. API will change. Don't store anything you can't lose. Enhoy the db in 
your weekend project

**Solid:**
- Zero dependencies — stdlib `encoding/gob` only.
- RAM-speed reads — O(1) `Id` lookups, contiguous scans.
  badger on point read and scan by a wide margin (`bench/compare`).
- Per-chunk durable writes; durable single-row write ~9µs (WAL).
- OCC transaction path (`Get` / `Update` / `UpdateWithin`) is goroutine-safe,
  race-tested.

**Not there yet:**
- Not fully ACID. Per-chunk `Flush` isn't cross-chunk atomic; the WAL is
  per-type so a cross-type transaction isn't atomic either. Isolation only
  covers the transaction path — `FindOneBy` / `Filter` / `Flush` still assume a
  single goroutine.
- Dataset must fit in RAM (target < 100 MB).
- Non-`Id` queries are full scans.
- Deleting a referenced parent panics on the next `Open` (no cascade / null-FK).
- Field rename breaks the gob files until schema migration lands.
