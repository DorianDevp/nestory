# nestory — benchmarks & comparison

Same-machine, same-session comparison of nestory against five pure-Go embedded
stores and one server database, on an identical record shape. A companion
report, `INMEMORY.md`, compares against the same engines running purely in
memory — no files, no fsync.

> **Methodology.** All numbers measured on the machine below, `benchtime=300ms`.
> nestory's numbers come from its in-package benchmark (`../bench_test.go` — it
> needs the unexported `resetRegistries` between iterations); the competitors
> come from `./compare` (own go.mod, so the library's go.sum stays clean). The
> record is the same everywhere: `{Id int, Name, Email string, Age int}`.
>
> These are *indicative*, not publication-grade: one machine, one run, short
> benchtime. The durability and write-amplification models differ — read the
> caveats before quoting any single ratio.
>
> **Freshness.** The four cross-engine tables (bulk insert, point read, point
> write, scan), the ownership-branch write tables and the Tower memory profile
> were re-measured together in one session. The remaining nestory-only
> diagnostics — ordered hot-history, indexed memory footprint, dialog-tree
> mutation, delta cursor — were **not** re-run in that pass and carry their
> original numbers.

## Environment

- CPU: Intel Core i5-8600K @ 3.60 GHz (6 cores), linux/amd64, Go test harness.
- Embedded competitors: `modernc.org/sqlite` (`synchronous=FULL`),
  `go.etcd.io/bbolt`, `tidwall/buntdb` (`SyncPolicy=Always`),
  `dgraph-io/badger/v4` (`SyncWrites=true`), and `hashicorp/go-memdb`
  (**no durability**).
- Server competitor: MongoDB 8 in Docker, reached over loopback TCP, write
  concern `w=1, j=true`. **Not comparable head to head** — see caveat 6.

## Workloads (matched across engines)

| Workload | nestory | SQLite / bbolt | go-memdb | MongoDB |
|---|---|---|---|---|
| **Bulk insert** | queue n + 1 `Flush` (1 fsync) | n inserts in 1 txn (1 fsync) | n inserts in 1 txn (no fsync) | one `InsertMany` of n |
| **Point read** | `View`, detached `Get`, or raw `Unsafe.Get` | `SELECT … WHERE id=?` / `Get` | `First("id")` | `findOne({_id})` |
| **Point write** | `UpdateWithin` + fsynced WAL frame | one fsynced transaction | one in-memory transaction | `updateOne` journaled |
| **Scan** | `Filter` (Age==42) | `SELECT … WHERE age=?` / cursor | iterate + filter | `find({age:42})` cursor |

## Durable bulk insert — time per batch (lower = better)

| engine | n=100 | n=1,000 | n=10,000 | rows/s @10k | B/op @10k | allocs/op @10k |
|---|---:|---:|---:|---:|---:|---:|
| **nestory** | **87 µs** | **0.45 ms** | **5.58 ms** | **1.79 M** | 4.88 MB | **11,170** |
| go-memdb* | 80 µs | 0.88 ms | 10.4 ms | 962 k | 4.96 MB | 110,447 |
| SQLite | 283 µs | 1.86 ms | 20.9 ms | 477 k | **3.59 MB** | 149,000 |
| BuntDB | 253 µs | 2.55 ms | 27.3 ms | 367 k | 24.6 MB | 259,845 |
| Badger | 337 µs | 2.96 ms | 31.5 ms | 317 k | 19.1 MB | 369,722 |
| bbolt | 349 µs | 3.84 ms | 42.9 ms | 233 k | 34.9 MB | 546,061 |
| MongoDB† | 4.11 ms | 10.5 ms | 66.8 ms | 150 k | 59.2 MB | 299,968 |

\* go-memdb is **not durable** — no fsync — so its write number isn't comparable
to the others; it's the in-memory floor.

† MongoDB is a server, not an embedded store — see caveat 6.

At n=10k that is about 1.79 million durable rows/s. In this harness nestory is
roughly 3.7× faster than SQLite and 7.7× faster than bbolt; even go-memdb's
non-durable MVCC batch is slower.

## Point read by id — ns/op (lower = better)

| engine / API | n=100 | n=1,000 | n=10,000 | reads/s @10k | B/op @10k | allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| **nestory `Unsafe.Get`** | 32 | 32 | **32** | **30.9 M** | **0** | **0** |
| **nestory `View`** | 53 | 56 | **56** | **17.9 M** | **0** | **0** |
| **nestory detached `Get`** | 195 | 195 | **197** | 5.08 M | 96 | 2 |
| go-memdb | 267 | 326 | 287 | 3.49 M | 208 | 5–6 |
| SQLite | 10,577 | 10,816 | 10,988 | 91.0 k | 656 | 22–24 |
| BuntDB | 11,157 | 11,801 | 12,127 | 82.5 k | 7,480 | 168 |
| Badger | 11,965 | 12,220 | 12,163 | 82.2 k | 8,148 | 173 |
| bbolt | 14,198 | 14,409 | 13,363 | 74.8 k | 7,984 | 173–186 |
| MongoDB† | 129,702 | 126,751 | 126,571 | 7.90 k | 12,005 | 95–97 |

`View` is the closest safe comparison: it read-locks the row for the callback
without copying. `Unsafe.Get` is the exclusive-access floor; detached `Get` pays
for a mutable branch copy. At n=10,000 `View` is about 196× faster than SQLite
and 2,260× faster than MongoDB, but the MongoDB figure is dominated by the
loopback round trip rather than by its storage engine.

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

| engine | n=100 | n=1,000 | n=10,000 | writes/s @10k | B/op @10k | allocs/op @10k |
|---|---:|---:|---:|---:|---:|---:|
| **nestory** | **2.98** | **3.02** | **3.08** | **325 k** | 802 | 14 |
| BuntDB | 4.40 | 4.70 | 4.30 | 233 k | 2,224 | 32 |
| go-memdb* | 3.98 | 5.87 | 5.72 | 175 k | 9,082 | 62 |
| Badger | 11.0 | 11.1 | 11.0 | 90.6 k | 3,434 | 61 |
| bbolt | 14.8 | 16.2 | 19.6 | 51.0 k | 16,392 | 115 |
| SQLite | 51.7 | 51.6 | 50.7 | 19.7 k | **168** | **7** |
| MongoDB† | 647 | 593 | 631 | 1.58 k | 6,883 | 98 |

SQLite allocates least per write and is slowest by 16×: its cost is the fsync
protocol, not Go-side garbage. Reading the two columns together is the point —
neither alone tells you what an engine does.

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

## Scalar write inside an ownership branch — µs/op (lower = better)

No other engine here models ownership, so this one compares nestory's three ways
of doing the same write: set one scalar field on a root that owns *n* children.
`Get`+`Update` and a transaction both copy the whole branch and merge it back;
`UpdateWithin` diffs a persistent shadow; `Tracked().UpdateWithin` declares the
write and diffs only what was declared. From `BenchmarkTowerVersusBranchClone`
and `BenchmarkTrackedRootWrite`.

| owned children | `Tracked`‡ | `UpdateWithin` | transaction | `Get`+`Update` |
|---:|---:|---:|---:|---:|
| 1 | 5.28 | 6.27 | **5.91** | 6.13 |
| 2 | **5.33** | 5.59 | 6.23 | 6.18 |
| 4 | 5.94 | **5.64** | 8.64 | 8.04 |
| 10 | 6.42 | **5.78** | 14.7 | 13.0 |
| 100 | **7.76** | 8.75 | 82.1 | 80.1 |
| 1,000 | **23.4** | 38.4 | 829 | 787 |
| 10,000 | **183** | 327 | 8,275 | 8,230 |
| 100,000 | **1,818** | 3,531 | 97,297 | 97,082 |

‡ `Tracked` comes from `BenchmarkTrackedRootWrite`, the other three from
`BenchmarkTowerVersusBranchClone`. The two benchmarks share an `UpdateWithin`
column and it agrees within about 1%, which is why they can sit in one table.

Allocation is where the shape of the difference shows:

| owned children | `Tracked` | `UpdateWithin` | `Get`+`Update` |
|---:|---:|---:|---:|
| 1 | 27 / 2.0 kB | 26 / 2.0 kB | 30 / 1.7 kB |
| 100 | 27 / 2.0 kB | 26 / 2.0 kB | 440 / 61 kB |
| 1,000 | 27 / 2.0 kB | 26 / 2.0 kB | 4,047 / 620 kB |
| 10,000 | 27 / 2.0 kB | 26 / 2.0 kB | 40,111 / 7.25 MB |
| 100,000 | 27 / 2.0 kB | 26 / 2.0 kB | 400,569 / 75.4 MB |

Both shadow paths are **flat**: the replica already exists, so a write allocates
what it publishes and nothing per owned node. The copying path allocates one
clone per node in the branch, touched or not — which is also why it is the only
column whose numbers grow.

At one owned child copying wins, by about 5%: maintaining a shadow buys nothing
there. From two children up the shadow path is ahead, and the gap widens with
branch size because one side is O(nodes compared) and the other is O(nodes
copied). The declaration adds a second step change from roughly a hundred
children, where diffing the branch starts to outweigh the fixed cost of a
commit.

### What the shadow costs in RAM

The flat allocation counts above are bought with memory. `rebuild()` clones the
**complete** committed relation index, not the branch being written, so the
first `UpdateWithin` on any relation-carrying type retains a second copy of
every participating entity in the project for as long as the replica stays
valid. `TestTowerMemoryProfile` prices it: build the graph, sample the retained
heap, then repeat with one scalar root write in between.

| payload/record | nodes | graph alone | + replica | replica adds | per node | live heap |
|---|---:|---:|---:|---:|---:|---:|
| 0 B | 100,001 | 140.3 MiB | 173.4 MiB | 33.1 MiB | **347 B** | **+24%** |
| 128 B | 100,001 | 152.6 MiB | 197.9 MiB | 45.3 MiB | **475 B** | **+30%** |
| 1 KiB | 50,001 | 119.3 MiB | 184.7 MiB | 65.4 MiB | **1,371 B** | **+55%** |

The three rows decompose cleanly: 347 + 128 = 475 and 347 + 1024 = 1371. So the
cost is a **fixed ~347 B per node plus a byte-for-byte copy of every slice
field** — `cloneSliceFields` gives each shadow node its own backing array, which
is what makes the shadow safe to mutate inside a callback.

The trigger is a single call. The first `UpdateWithin` on any relation-carrying
root finds no replica and runs `rebuild()`, which copies the whole committed
index rather than the branch it was asked about:

```go
liveNodes := make(map[nodeKey]reflect.Value, len(index.nodes))
for key, value := range index.nodes {   // every node in the project
    liveNodes[key] = value
}
```

The profile splits that fixed 347 B by keeping one more part of the replica per
variant (0-byte payload, so this is bookkeeping only):

| component | B/node | what it is |
|---|---:|---|
| shadow storage | 88 | contiguous blocks + cloned slice fields — the only actual data |
| `nodes` map | 73 | `nodeKey` → shadow |
| `live` map | 73 | `nodeKey` → canonical pointer |
| `relations` baseline | 8 | copy of the owned-pointer slice |
| `branches` cache | 104 | `[]towerBranchNode` for each root written |
| **total** | **347** | |

Re-running with the 128-byte payload leaves `live`, `relations` and `branches`
unchanged at 73, 7 and 104 B/node and moves only the shadow row, which is what
the split predicts: everything except the shadow is per-node bookkeeping and
does not scale with record size.

The record itself is 56 B. So the replica is roughly **one quarter data and
three quarters index**: two `nodeKey`-keyed maps at 48 B of useful payload each
(a 24 B `nodeKey` and a 24 B `reflect.Value`) but ~73 B once Go's map overhead is
included, plus a cached branch whose `towerBranchNode` is 104 B — 32 B of that
being the two boxed pointers the compiled comparator reads.

That ~1.5× map factor is not specific to the Tower. The indexed profile above
measures a `map[int]*T` primary lookup at 23 B against 16 B of useful payload —
1.44×, against the Tower maps' 1.52×. Per-node cost in nestory is therefore
governed less by record size than by **how many maps a node appears in**: a
primary key alone is ~23 B, a unique string index adds ~43 B, and enabling the
Tower adds 146 B of maps plus the 104 B branch entry.

Two of those are avoidable in principle rather than by nature. `live` is a
defensive copy of `committedOwnership.index.nodes`, held only so the rebuild can
release `committedOwnership.RLock()` early; the branch cache is retained for the
replica's life even if that root is never written again.

Read together with the timing table, the trade is: a scalar write on a
100,000-child branch drops from 97 ms to 3.5 ms, and the project's live heap
grows by roughly a third. That is a good trade for a write-heavy graph and a bad
one for a large graph that is written rarely — the replica is retained whether
or not it is used again, until any participating table changes.

Run it with:

```sh
NESTORY_MEMORY_PROFILE=1 go test -run '^TestTowerMemoryProfile$' -count=1 -v
```

## Full scan + filter — time per scan (lower = better)

| engine | n=100 | n=1,000 | n=10,000 | rows scanned/s @10k | B/op @10k |
|---|---:|---:|---:|---:|---:|
| **nestory** | 474 ns | 5.08 µs | **50.2 µs** | **199 M** | 15.7 kB |
| go-memdb | 811 ns | 6.04 µs | 60.4 µs | 166 M | **312 B** |
| SQLite | 27.1 µs | 85.1 µs | 744 µs | 13.4 M | 15.5 kB |
| MongoDB† | 203 µs | 524 µs | 3.80 ms | 2.63 M | 547 kB |
| Badger | 1.22 ms | 11.6 ms | 116 ms | 86.3 k | 75.9 MB |
| BuntDB | 1.18 ms | 13.6 ms | 120 ms | 83.3 k | 74.1 MB |
| bbolt | 1.34 ms | 14.4 ms | 145 ms | 69.1 k | 73.1 MB |

**nestory wins again** — contiguous `[]T` scan, no per-row deserialization.
~1.2× faster than go-memdb, ~15× SQLite, ~2,900× bbolt. (bbolt is so slow here
because every value is gob-decoded on read — inherent to a KV store of blobs.)

This is the one workload where MongoDB beats the embedded KV stores, by ~30×.
That is not a storage-engine result: Mongo filters server side and ships only
the matching rows, while the KV harness pulls every value across and gob-decodes
it in the client. It measures the harness's encoding choice, not the engines.

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

6. **MongoDB is in a different category and its ratios mean less.** Every other
   engine here runs inside the benchmark process; Mongo answers over a loopback
   socket. Its ~127 µs point read is roughly a client/server round trip plus
   BSON encode/decode, so the comparison mostly measures *embedded versus
   networked*, not one storage engine against another. It also ran on Docker's
   overlay filesystem rather than the host filesystem the others used, so its
   journal fsync is not the same operation. Read it as "what an out-of-process
   database costs you", and nothing finer.

## Verdict

For its niche — an in-RAM, pointer-native graph with WAL durability — nestory is
the fastest measured engine on flat reads, scans, durable point writes and
durable bulk inserts in this harness. ID-targeted structural graph mutation now
avoids both global validation and global rewiring.

Detached branch copying is no longer the only option for a large ownership root:
`UpdateWithin` keeps a persistent shadow of the graph and publishes a diff, so a
scalar root write costs a comparison per node instead of a copy per node, at a
flat 26 allocations regardless of branch size. At 100,000 owned children that is
3.5 ms against 97 ms, and 1.8 ms if the write is declared through
`Tracked().UpdateWithin`.

## Reproduce

```sh
# nestory (from repo root — needs the in-package harness):
go test -run='^$' -bench='BatchInsertFlush|ViewByID|SafeGetByIDOnly|UnsafeGetByID|PointWrite|Filter' -benchmem -benchtime=300ms

# ownership-branch write (fixed iteration count — the largest branch is slow):
go test -run='^$' -bench='TowerVersusBranchClone|TrackedRootWrite' -benchmem -benchtime=200x

# embedded competitors (from ./bench):
go test ./compare/ -run='^$' -bench=. -benchmem -benchtime=300ms

# MongoDB — skipped unless a server is reachable (from ./bench):
docker run -d --name nestory-bench-mongo -p 127.0.0.1:27018:27017 mongo:8
NESTORY_MONGO_URI=mongodb://127.0.0.1:27018 \
  go test ./compare/ -run='^$' -bench=Mongo -benchmem -benchtime=300ms
docker rm -f nestory-bench-mongo
```

Not yet measured here (need extra infra): cgo SQLite (`mattn/go-sqlite3`, needs a
C toolchain) and server RDBMS (Postgres/MySQL via Docker).
