# nestory — benchmarks & comparison

Same-machine, same-session comparison of nestory against three pure-Go stores,
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
- Competitors: `modernc.org/sqlite` (pure-Go SQLite, `synchronous=FULL`),
  `go.etcd.io/bbolt` (B+tree, fsync per commit), `hashicorp/go-memdb`
  (in-memory MVCC, **no durability**).

## Workloads (matched across engines)

| Workload | nestory | SQLite / bbolt | go-memdb |
|---|---|---|---|
| **Bulk insert** | queue n + 1 `Flush` (1 fsync) | n inserts in 1 txn (1 fsync) | n inserts in 1 txn (no fsync) |
| **Point read** | `FindOneBy("Id")` (index) | `SELECT … WHERE id=?` / `Get` | `First("id")` |
| **Scan** | `Filter` (Age==42) | `SELECT … WHERE age=?` / cursor | iterate + filter |

## Durable bulk insert — time per batch (lower = better)

| n | nestory | SQLite | bbolt | go-memdb* |
|---:|---:|---:|---:|---:|
| 100 | 304 µs | 284 µs | 269 µs | 74 µs |
| 1,000 | 3.51 ms | 1.78 ms | 2.75 ms | 0.78 ms |
| 10,000 | 28.6 ms | 16.6 ms | 36.3 ms | 10.1 ms |

\* go-memdb is **not durable** — no fsync — so its write number isn't comparable
to the others; it's the in-memory floor.

At n=10k that's ~350k rows/s (nestory), ~600k (SQLite), ~280k (bbolt), ~990k
(go-memdb, no durability). **nestory's durable bulk insert sits right between
SQLite and bbolt** — same order of magnitude as both.

## Point read by id — ns/op (lower = better)

| engine | n=100 | n=1,000 | n=10,000 | allocs/op |
|---|---:|---:|---:|---:|
| **nestory** | 24.5 | 24.6 | **24.6** | **0** |
| go-memdb | 222 | 267 | 240 | 6 |
| SQLite | 10,058 | 10,228 | 10,276 | 24 |
| bbolt | 13,087 | 12,101 | 13,001 | 186 |

**nestory wins decisively, and flat:** ~25 ns / **0 allocs** — a raw map hit +
pointer, no transaction, no row decode. ~10× faster than go-memdb, ~420× faster
than SQLite, ~530× faster than bbolt.

## Full scan + filter — time per scan (lower = better)

| engine | n=100 | n=1,000 | n=10,000 |
|---|---:|---:|---:|
| **nestory** | 394 ns | 4.3 µs | **41 µs** |
| go-memdb | 769 ns | 5.7 µs | 57 µs |
| SQLite | 25 µs | 80 µs | 614 µs |
| bbolt | 1.4 ms | 11.4 ms | 110 ms |

**nestory wins again** — contiguous `[]T` scan, no per-row deserialization.
~1.4× faster than go-memdb, ~15× SQLite, ~2,700× bbolt. (bbolt is so slow here
because every value is gob-decoded on read — inherent to a KV store of blobs.)

## How to read this — the honest caveats

1. **Reads are nestory's home turf, and it dominates.** Point reads and scans
   are pure-RAM pointer/map/slice work with no transaction and no
   (de)serialization. It beats even go-memdb and is 2–3 orders faster than the
   disk-backed engines. This is the real edge.

2. **The insert win is an illusion for the wrong workload.** The table measures
   *one* durable commit of a whole batch. nestory's `Flush` rewrites the
   **entire file**, so the cost is O(dataset), not O(change). For *many small
   durable commits* (commit-per-row), nestory does N full-file rewrites — and
   loses badly to SQLite/bbolt, which write incrementally. nestory's sweet spot
   is "build/load a dataset, snapshot it," not "high-frequency small writes."

3. **go-memdb's writes aren't durable** — exclude them from the durability
   comparison.

4. **bbolt could be tuned** — a different value encoding would cut its scan cost;
   the gob-per-value here is a reasonable but not optimal choice.

5. **Concurrency.** SQLite/bbolt/go-memdb are concurrency-safe (MVCC / locking).
   nestory is **not yet goroutine-safe** — part of why its reads are so cheap is
   that there's no synchronization. Apples-to-oranges until that lands (see
   [../TODO.md](../TODO.md)).

## Verdict

For its niche — an in-RAM, read-heavy graph that fits in memory and is snapshotted
rather than continuously committed — **nestory is the fastest of the four on
reads by a wide margin, and competitive on durable bulk writes.** It loses, by
design, on high-frequency small durable writes (whole-file rewrite) and isn't
yet safe for concurrent use. That matches the positioning: a pointer-native
read model / projection store, not a general-purpose transactional database.

## Reproduce

```sh
# nestory (from repo root — needs the in-package harness):
go test -run='^$' -bench='BatchInsertFlush|FindOneByID|Filter' -benchmem -benchtime=300ms

# competitors (from ./bench):
go test ./compare/ -run='^$' -bench=. -benchmem -benchtime=300ms
```

Not yet measured here (need extra infra): cgo SQLite (`mattn/go-sqlite3`, needs a
C toolchain) and server RDBMS (Postgres/MySQL via Docker).
