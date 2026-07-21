# nestory — benchmarks & comparison

Same-machine, same-session comparison of nestory against five pure-Go stores,
on an identical record shape.

> **Methodology.** All numbers measured on the machine below, `benchtime=300ms`.
> nestory's numbers come from its in-package benchmark (`../bench_test.go` — it
> needs the unexported `resetRegistries` between iterations); the competitors
> come from `./compare` (own go.mod, so the library's go.sum stays clean). The
> record is the same everywhere: `{Id int, Name, Email string, Age int}`.
>
> These are *indicative*, not publication-grade: one machine, one run, short
> benchtime. The durability and write-amplification models differ — read the
> caveats before quoting any single ratio.

## Environment

- CPU: Intel Core i5-8600K @ 3.60 GHz (6 cores), linux/amd64, Go test harness.
- Competitors: `modernc.org/sqlite` (`synchronous=FULL`), `go.etcd.io/bbolt`,
  `tidwall/buntdb` (`SyncPolicy=Always`), `dgraph-io/badger/v4`
  (`SyncWrites=true`), and `hashicorp/go-memdb` (**no durability**).

## Workloads (matched across engines)

| Workload | nestory | SQLite / bbolt | go-memdb |
|---|---|---|---|
| **Bulk insert** | queue n + 1 `Flush` (1 fsync) | n inserts in 1 txn (1 fsync) | n inserts in 1 txn (no fsync) |
| **Point read** | `View`, detached `Get`, or raw `Unsafe.Get` | `SELECT … WHERE id=?` / `Get` | `First("id")` |
| **Point write** | `UpdateWithin` + fsynced WAL frame | one fsynced transaction | one in-memory transaction |
| **Scan** | `Filter` (Age==42) | `SELECT … WHERE age=?` / cursor | iterate + filter |

## Durable bulk insert — time per batch (lower = better)

| n | nestory | SQLite | bbolt | BuntDB | Badger | go-memdb* |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | **89 µs** | 280 µs | 279 µs | 234 µs | 310 µs | 74 µs |
| 1,000 | **0.51 ms** | 1.79 ms | 2.64 ms | 2.32 ms | 2.66 ms | 0.75 ms |
| 10,000 | **5.43 ms** | 16.6 ms | 32.9 ms | 24.6 ms | 28.8 ms | 8.27 ms |

\* go-memdb is **not durable** — no fsync — so its write number isn't comparable
to the others; it's the in-memory floor.

At n=10k that is about 1.84 million durable rows/s. In this harness nestory is
roughly 3× faster than SQLite and 6× faster than bbolt; even go-memdb's
non-durable MVCC batch is slower.

## Point read by id — ns/op (lower = better)

| engine / API | n=100 | n=1,000 | n=10,000 | allocs/op |
|---|---:|---:|---:|---:|
| **nestory `Unsafe.Get`** | 27 | 28 | **28** | **0** |
| **nestory `View`** | 145 | 145 | **145** | **0** |
| **nestory detached `Get`** | 200 | 201 | **211** | 2 |
| go-memdb | 210 | 229 | 228 | 5–6 |
| SQLite | 10,343 | 10,481 | 10,523 | 22–24 |
| BuntDB | 11,156 | 11,558 | 10,920 | 168 |
| Badger | 11,361 | 11,356 | 11,597 | 173 |
| bbolt | 11,765 | 11,642 | 11,936 | 173–186 |

`View` is the closest safe comparison: it read-locks the live ownership branch
for the callback without copying. `Unsafe.Get` is the exclusive-access floor;
detached `Get` pays for a mutable branch copy.

## Durable point write — µs/op (lower = better)

| engine | n=100 | n=1,000 | n=10,000 | allocs/op at 10k |
|---|---:|---:|---:|---:|
| **nestory** | **2.58** | **2.61** | **2.67** | **12** |
| BuntDB | 3.92 | 4.07 | 3.99 | 32 |
| go-memdb* | 3.02 | 4.11 | 4.38 | 62 |
| Badger | 10.4 | 10.6 | 10.6 | 61 |
| bbolt | 13.2 | 14.5 | 17.2 | 115 |
| SQLite | 48.3 | 49.1 | 49.3 | 7 |

Nestory appends and fsyncs one compact WAL frame before publishing the in-memory
update. On this filesystem it beats every durable comparator in the point-write
workload. go-memdb remains a non-durable reference floor.

## Durable structural graph mutation — µs/op (lower = better)

This workload uses a binary dialog tree through the public safe API. “Add”
attaches a new three-node branch; “reparent” moves an existing subtree between
two parents. Both include detached branch creation, relation validation, WAL
commit, publication, and relation rewiring.

| operation | 50 existing nodes | 100 existing nodes | allocs/op at 100 |
|---|---:|---:|---:|
| add three-node branch | 212 µs | 379 µs | 1,985 |
| reparent subtree | 159 µs | 305 µs | 1,696 |

Transaction validation reuses the last committed relation model and rescans
only changed holders. Changes to a relation match key, a duplicate target key,
or a newly introduced entity type deliberately fall back to a complete index
rebuild. The end-to-end cost still grows with the ownership branch because the
safe API returns and compares a detached mutable copy of that branch.

### Targeted mutation versus total graph size

To separate detached-branch copying from global graph maintenance, a second
workload keeps the mutated local subtree constant while widening the unrelated
part of the registered graph. Both operations address the mutated node or its
two parents directly by ID.

| operation | 100 total nodes | 1,000 total nodes | 10,000 total nodes |
|---|---:|---:|---:|
| scalar write | 5.24 µs | 5.30 µs | 5.23 µs |
| reparent subtree | 130 µs | 1.21 ms | 12.25 ms |

The scalar control stays flat, proving that ID-targeted access already avoids
copying unrelated ownership branches. Structural mutation remains linear in
the complete registered graph: the current transaction model copies global
node/owner/reference indexes and relation rewiring scans every live relation
store. Persistent relation indexes and targeted rewiring therefore take
priority over transparent object-level copy-on-write, which ordinary mutable
Go pointers cannot intercept without changing Nestory's struct-first API.

## Full scan + filter — time per scan (lower = better)

| engine | n=100 | n=1,000 | n=10,000 |
|---|---:|---:|---:|
| **nestory** | 435 ns | 4.68 µs | **46.9 µs** |
| go-memdb | 739 ns | 5.72 µs | 57.8 µs |
| SQLite | 26.3 µs | 83.0 µs | 625 µs |
| bbolt | 1.11 ms | 11.1 ms | 112 ms |

**nestory wins again** — contiguous `[]T` scan, no per-row deserialization.
~1.4× faster than go-memdb, ~15× SQLite, ~2,700× bbolt. (bbolt is so slow here
because every value is gob-decoded on read — inherent to a KV store of blobs.)

## How to read this — the honest caveats

1. **Reads are nestory's home turf, and it dominates.** Point reads and scans
   are pure-RAM pointer/map/slice work with no transaction and no
   (de)serialization. It beats even go-memdb and is 2–3 orders faster than the
   disk-backed engines. This is the real edge.

2. **Flat writes and graph writes are different workloads.** A scalar point
   update uses the WAL fast path. Structural transactions now validate relation
   deltas, but still copy and compare the detached ownership branch, so their
   end-to-end cost scales with branch size.

3. **go-memdb's writes aren't durable** — exclude them from the durability
   comparison.

4. **KV competitors could be tuned** — a different value encoding would cut
   their scan cost;
   the gob-per-value here is a reasonable but not optimal choice.

5. **Concurrency models differ.** Nestory's safe APIs use graph and per-resource
   locks; `View` read-locks an ownership branch. `Unsafe` deliberately provides
   no isolation or locking and requires caller-managed exclusive access.

## Verdict

For its niche — an in-RAM, pointer-native graph with WAL durability — nestory is
the fastest measured engine on flat reads, scans, durable point writes and
durable bulk inserts in this harness. Structural graph mutation now avoids a
global reflective validation pass; detached branch copying remains its main
size-dependent cost.

## Reproduce

```sh
# nestory (from repo root — needs the in-package harness):
go test -run='^$' -bench='BatchInsertFlush|SafeGetByIDOnly|UnsafeGetByID|RelationView|PointWrite|Filter|DialogTree' -benchmem -benchtime=300ms

# competitors (from ./bench):
go test ./compare/ -run='^$' -bench=. -benchmem -benchtime=300ms
```

Not yet measured here (need extra infra): cgo SQLite (`mattn/go-sqlite3`, needs a
C toolchain) and server RDBMS (Postgres/MySQL via Docker).
