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

## Ordered hot-history operations

This in-package diagnostic models the flat append log used by the OpenCode
experiment: a unique message ID plus a unique ordered `(SessionID, Seq)` index.
Create and delete are one durable transaction per complete batch. These are
the medians of three `benchtime=1x` samples intended to expose scaling, not
cross-engine results.

| operation | 1,000 rows | 10,000 rows | 100,000 rows |
|---|---:|---:|---:|
| indexed batch create | 2.12 ms | 26.5 ms | 342 ms |
| indexed batch delete | 1.77 ms | 18.2 ms | 241 ms |

Batch index maintenance filters removals in one pass and sort-merges additions.
Before that path, the same 100,000-row create and delete samples took 1.26 s and
1.27 s because every row shifted the sorted index separately: the batch path is
about 3.7× and 5.3× faster respectively.

Index ordering for a pure-create batch is prepared outside the database mutex
and published after the rows are live. In a diagnostic with 100,000 existing
rows and a concurrent 50,000-row hydration, a stable single-field lookup
measured 399 ns p50, 1.87 µs p95, and 2.77 µs p99 inside Nestory. This isolates
engine lock latency; a sidecar still adds scheduling and protocol overhead.

### Indexed memory footprint

The opt-in memory profile builds a real 100,000-row database in isolated test
processes, forces collection and attributes retained memory cumulatively. Its
fixture uses a 128-byte payload, a unique string key and a unique ordered
two-field index.

| retained component | incremental bytes/row |
|---|---:|
| chunk store, record, strings and 128-byte payload | 297 B |
| primary resource lookup | 23 B |
| two ordered pointer slices | 19 B |
| single-field and composite uniqueness maps | 85 B |
| **complete live heap, including fixed runtime cost** | **428 B** |

The previous representation retained both a generic primary map and separate
validation and lookup maps for a unique field. It used 626 B/row in the same
profile. Sharing typed unique maps and using the resource map as the primary
lookup reduces live heap by 31.6%. Stabilized test-process RSS fell from 73.2
MiB to 54.7 MiB. This is a post-GC engine diagnostic, not a claim about peak
sidecar RSS during hydration.

Run it with:

```sh
NESTORY_MEMORY_PROFILE=1 go test -run '^TestIndexedMemoryProfile$' -count=1 -v
```

An indexed delta cursor changes the amount of work more fundamentally. On a
100,000-row `(SessionID, Seq)` index:

| safe view | time/op | bytes/op | allocs/op |
|---|---:|---:|---:|
| complete session | 14.37 ms | 2,408,448 | 3 |
| 10 rows after `Seq=99,990` | 1.08 µs | 240 | 3 |

Both paths return read-locked live pointers. The delta result measures only
Nestory's index lookup and row locking; it does not include an application
protocol, JSON encoding, or consumer-side schema decoding. A sidecar benefits
only if its client retains a validated snapshot and transfers the delta instead
of requesting the complete context again.

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
| scalar write | 3.83 µs | 3.94 µs | 3.77 µs |
| reparent subtree, before indexes | 130 µs | 1.21 ms | 12.25 ms |
| reparent subtree, indexed | 11.0 µs | 11.4 µs | 11.0 µs |

The scalar control stays flat, proving that ID-targeted access avoids copying
unrelated ownership branches. Persistent ordered relation indexes now make the
structural result independent of total graph size too: the same local reparent is
about 12×, 106×, and 1,117× faster at 100, 1,000, and 10,000 nodes respectively.
The indexed commit uses a fixed 3,040 B and 43 allocations at all three sizes,
down from 10,184 B and 107 allocations in the first persistent-index version.
Its compact delta keeps small transactions inline, retains empty adjacency
buckets for repeated moves, and avoids rebuilding a reflective relation model.

Relation-only commits still validate while holding the graph write lock. To
measure its user-visible consequence, a second benchmark continuously reparents
the branch while another goroutine performs scalar `UpdateWithin` calls:

| total nodes | scalar p50 | scalar p95 | scalar p99 |
|---:|---:|---:|---:|
| 100 | 29–30 µs | 43–44 µs | 54–59 µs |
| 10,000 | 29–30 µs | 42–44 µs | 51–55 µs |

This intentionally keeps a structural writer pending almost continuously. Even
under that pressure, scalar tail latency stays below 0.2 ms rather than inheriting
the old multi-millisecond global scan. Moving validation outside the write lock
with a graph-generation OCC check is therefore not justified by this result.

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
durable bulk inserts in this harness. ID-targeted structural graph mutation now
avoids both global validation and global rewiring; detached branch copying
remains size-dependent only when the caller chooses a large ownership root.

## Reproduce

```sh
# nestory (from repo root — needs the in-package harness):
go test -run='^$' -bench='BatchInsertFlush|SafeGetByIDOnly|UnsafeGetByID|RelationView|PointWrite|Filter|DialogTree' -benchmem -benchtime=300ms

# competitors (from ./bench):
go test ./compare/ -run='^$' -bench=. -benchmem -benchtime=300ms
```

Not yet measured here (need extra infra): cgo SQLite (`mattn/go-sqlite3`, needs a
C toolchain) and server RDBMS (Postgres/MySQL via Docker).
