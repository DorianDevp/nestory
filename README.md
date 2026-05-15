# nestory

> **A cozy little graph store for Go.** Your domain lives in a nest. Your events become a story.

`nestory` persists your Go object graph as a folder of `.gob` files and
rehydrates the **entire graph of pointers** on load. There are no queries,
no joins, no DSL — `user.Profile.Settings.Theme` just works because `Profile`
and `Settings` were rewired to real pointers the moment you called `Open`.

```go
type User struct {
    Id      int      `key:"primary"`
    Name    string
    Profile *Profile `relto:"Id"`     // one-to-one
    Posts   []*Post  `mapby:"UserId"` // one-to-many
}

db, _ := nestory.Open[User](/* ... */)
u, _  := db.FindOneBy("Id", 1)

fmt.Println(u.Profile.Settings.Theme) // already there, no second call
fmt.Println(u.Posts[0].Title)         // slice already filled
```

## Status

**Alpha.** API will change before 1.0. Do not use for data you cannot lose.

## Why nestory?

Most embedded Go stores (Storm, BoltHold, BadgerHold) treat your structs as
**rows** and force you to query for relations. nestory flips the model: the
whole dataset lives in memory, relations are real Go pointers, and `Open`
rebuilds the graph from disk in a single pass.

The same primitives also make it a natural fit for **event-sourced read
models** — your projection is a graph of Go pointers, snapshotted to disk,
rebuilt from events whenever you want.

## Trade-offs

**What you get**
- Pure stdlib (only `encoding/gob`). Your `go.sum` stays clean.
- Domain graph as Go pointers — type safety, autocomplete, refactor-friendly.
- Snapshot files are greppable, diffable, shippable as CI fixtures.
- Single binary, no external database process.

**What you give up**
- The dataset must fit in RAM (sweet spot: < 100 MB).
- `Flush()` rewrites the whole file (atomic only after roadmap item #1).
- No transactions, no MVCC, no multi-writer concurrency.
- Non-`Id` queries are full scans.
- Field rename breaks the gob file until schema migration lands.

## Tags

| Tag             | Where             | Meaning |
|-----------------|-------------------|---------|
| `key:"primary"` | `int` field       | Marks the primary key. Required on every entity. |
| `relto:"Id"`    | pointer field     | One-to-one or many-to-one. Stored as a flattened FK; rehydrated to a real `*T` on load. |
| `mapby:"UserId"`| slice-of-pointer  | One-to-many. On load, the slice is filled with every `*T` whose `T.UserId` equals this entity's `Id`. |

For many-to-many, use a junction entity with two plain `int` foreign-key
fields and `mapby` on each parent side.

## Roadmap to 1.0

- [ ] Atomic save (write `.tmp` + rename)
- [ ] Versioned snapshots with event-offset metadata (event sourcing)
- [ ] `Apply(event, handler)` + `AutoSnapshot` API for ES projections
- [ ] Schema migration (rename / add / remove fields without losing data)
- [ ] Secondary indexes (currently only `Id` is indexed)
- [ ] Benchmarks vs Storm, BoltHold, JSON-file
- [ ] Public-API cleanup (rename `RegisterGoBase`/`NewGoBase` heritage)

## License

MIT.
