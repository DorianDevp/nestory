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

// Register every type used by the graph BEFORE opening any base.
nestory.Register[Settings]()
nestory.Register[Profile]()
nestory.Register[Post]()
nestory.Register[User]()

db := nestory.Open[User]()        // wires the in-memory pointer graph
u, _ := db.FindOneBy("Id", 1)

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
- **Stable pointers.** Entities live in a chunked arena, so a `*T` you hold (or a
  relation pointing at it) never dangles when the dataset grows.
- RAM-speed reads: O(1) primary-key lookups, contiguous full scans.
- O(1) auto-incrementing ids (seeded from disk on load — no reuse across restarts).
- Snapshot files are greppable, diffable, shippable as CI fixtures.
- Single binary, no external database process.

**What you give up**
- The dataset must fit in RAM (sweet spot: < 100 MB).
- `Flush()` rewrites the **whole** file each call — cost scales with dataset size,
  not change size. The write itself is atomic (tmp + fsync + rename).
- No transactions across bases, no MVCC, no multi-writer concurrency (single
  writer; the store is **not** yet goroutine-safe — see [TODO.md](TODO.md)).
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

- [x] Atomic save (write `.tmp` + fsync + rename)
- [x] Stable-address backing store (chunked arena) so relations survive growth
- [x] O(1) auto-ids seeded across reloads
- [ ] Goroutine-safe access (the `RWMutex` is not yet wired through)
- [ ] Cross-base transactions / atomic multi-file commit
- [ ] Cascade / null on delete (deleting a referenced parent currently panics on next `Open`)
- [ ] Versioned snapshots with event-offset metadata (event sourcing)
- [ ] `Apply(event, handler)` + `AutoSnapshot` API for ES projections
- [ ] Schema migration (rename / add / remove fields without losing data)
- [ ] Secondary indexes (currently only `Id` is indexed)
- [ ] Public-API cleanup (shrink the exported surface)

See **[TODO.md](TODO.md)** for the detailed hardening backlog and known gaps.

## License

MIT.
