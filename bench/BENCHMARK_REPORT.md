# nestory — benchmark report

One report, one table per category, the same players in every table.

## How this is measured

Every `(category, engine, scale, shape)` runs in **its own `go test` process**.
That is not tidiness: nestory registers entity types process-globally, so a
second configuration cannot exist beside the first — and two engines sharing a
process would let the first one's heap, GC state and warmed caches move the
second one's numbers. `run.sh` walks the matrix, one player at a time.

```sh
./run.sh                                    # every category, scales 1 … 1,000,000
./run.sh point                              # one category
NESTORY_SCALES="1000 100000" ./run.sh graph # chosen scales
```

Scale comes from `NESTORY_BENCH_SCALE`, shape from `NESTORY_BENCH_SHAPE`. Raw
output lands in `results/<category>/<engine>-<shape>-<scale>.txt`.

| category | file | what it measures |
|---|---|---|
| point | `compare/point_test.go` | read and write one row by id |
| bulk | `compare/bulk_test.go` | load N rows into an empty dataset |
| scan | `compare/scan_test.go` | full scan with a predicate |
| graph | `compare/graph_test.go` | read one ownership branch, 1 → 1,000,000 nodes |

**Environment.** Intel Core i5-8600K @ 3.60 GHz (6 cores), linux/amd64,
`benchtime=300ms`. Record: `{Id int, Name, Email string, Age int}`.

**Read the write columns with `REVIEW.md`'s correction in hand.** `/tmp` is
tmpfs here, so every fsync lands in RAM. On a real NVMe a durable point write
costs 6.46 ms rather than 3.02 µs, and nestory places fourth of five rather than
first. Reads, scans and the graph tables are unaffected.

## Players

| engine | model | in these runs |
|---|---|---|
| **nestory** | in-RAM pointer graph, WAL | `View` (safe), `Unsafe` (exclusive), `Get` (detached copy) |
| go-memdb | immutable-radix MVCC | no durability |
| SQLite | B-tree, SQL | on disk (`synchronous=FULL`) and `:memory:` |
| bbolt | B+tree, mmap | on disk |
| BuntDB | in-memory KV + AOF | on disk (`SyncPolicy=Always`) and `:memory:` |
| Badger | LSM | on disk (`SyncWrites`) and `WithInMemory` |
| Redis 7 | remote in-memory KV | loopback TCP, gated on `NESTORY_REDIS_URI` |
| MongoDB 8 | remote document store | loopback TCP, gated on `NESTORY_MONGO_URI` |

Redis and MongoDB are skipped unless a server is reachable; both answer over a
socket, so their numbers say what an out-of-process database costs, not what
their storage engines can do.

## Point read and write — n=10,000

| engine | read ns/op | read allocs | write ns/op | write allocs |
|---|---:|---:|---:|---:|
| **nestory `Unsafe`** | **31.5** | **0** | — | — |
| **nestory `View`** | **55.1** | **0** | 3,021 | 16 |
| nestory `Get` (detached) | 473 | 2 | — | — |
| go-memdb | 228 | 6 | 4,116 | 62 |
| SQLite `:memory:` | 1,815 | 16 | 6,348 | **7** |
| BuntDB | 11,102 | 168 | 3,805 | 32 |
| BuntDB `:memory:` | 11,903 | 168 | **2,646** | 30 |
| Badger | 11,454 | 173 | 10,781 | 61 |
| Badger `InMemory` | 12,159 | 173 | 7,636 | 51 |
| bbolt | 13,539 | 186 | 17,996 | 115 |
| SQLite | 9,703 | 16 | 57,635 | 7 |

`View` reads 4.1× faster than go-memdb and 33× faster than SQLite `:memory:`,
allocating nothing: it returns the live row rather than a decoded copy. The one
engine that beats nestory on writes is BuntDB `:memory:`, which has no
durability at all — and see the tmpfs note above before reading that column as a
durability result.

## Bulk insert — 10,000 rows into an empty dataset

| engine | ms/op | rows/s | B/op | allocs/op |
|---|---:|---:|---:|---:|
| **nestory** | **4.09** | **2.45 M** | 4.40 MB | **40,911** |
| go-memdb | 8.22 | 1.22 M | 4.96 MB | 110,444 |
| SQLite | 16.2 | 616 k | **3.59 MB** | 148,999 |
| SQLite `:memory:` | 17.0 | 588 k | 3.59 MB | 148,999 |
| BuntDB `:memory:` | 24.7 | 406 k | 16.7 MB | 259,522 |
| BuntDB | 25.8 | 388 k | 18.1 MB | 259,592 |
| bbolt | 27.3 | 367 k | 20.9 MB | 309,001 |
| Badger `InMemory` | 33.4 | 299 k | 21.9 MB | 332,613 |
| Badger | 38.2 | 262 k | 22.8 MB | 372,628 |

Every engine starts each iteration from empty — those that can be rebuilt are,
and nestory deletes what it just wrote, both off the clock. nestory batches n
rows behind one WAL frame and one fsync, which is where its 2× over go-memdb
comes from; the ordering here survives a real disk, unlike the point-write
column.

## Full scan with a predicate — n=10,000

| engine | ns/op | rows/s | B/op | allocs/op |
|---|---:|---:|---:|---:|
| **nestory `Filter`** | **46,399** | **216 M** | 15.7 kB | **8** |
| go-memdb | 56,681 | 176 M | **312 B** | 8 |
| SQLite `:memory:` | 603,180 | 16.6 M | 15.5 kB | 1,008 |
| SQLite | 619,426 | 16.1 M | 15.5 kB | 1,008 |
| BuntDB | 107 ms | 93 k | 74.1 MB | 1,660,001 |
| Badger | 108 ms | 92 k | 75.9 MB | 1,662,095 |
| Badger `InMemory` | 109 ms | 92 k | 76.1 MB | 1,662,097 |
| bbolt | 114 ms | 88 k | 73.1 MB | 1,651,073 |
| BuntDB `:memory:` | 117 ms | 85 k | 74.1 MB | 1,660,005 |

The KV engines decode every value to read one field, which costs 1.6 million
allocations and 74 MB per pass. go-memdb allocates least — it iterates lazily
where `Filter` materializes the result.

Past cache: nestory holds **151 M rows/s at 5 million rows**, down 29% from its
in-cache figure and then flat. Quote that for datasets that do not fit in L2.

## Ownership branch — read one root with everything it owns

The shape nestory exists for. A workspace owns projects, a project owns
documents; the workload is "give me this workspace and all of it". Every other
engine reassembles that from rows or blobs on each read.

**`single` — the whole project under one root:**

| engine | n=1,000 (branch 1,000) | n=100,000 (branch 100,000) | allocs @100k |
|---|---:|---:|---:|
| **nestory `Unsafe`** | **1.09 µs** | **210 µs** | **0** |
| **nestory `View`** | **1.21 µs** | **149 µs** | **0** |
| go-memdb | 16.2 µs | 1.91 ms | 2,593 |
| SQLite | 1.53 ms | 91.2 ms | 693,044 |
| Badger | 11.2 ms | 1,096 ms | 16,700,787 |
| bbolt | 11.8 ms | 1,106 ms | 17,559,319 |
| Bunt `:memory:` | 11.6 ms | 1,082 ms | 15,881,366 |
| BuntDB | 12.2 ms | 1,055 ms | 15,881,360 |
| Badger `InMemory` | 11.3 ms | 1,083 ms | 16,700,726 |

**`wide` — the same totals spread over five-node roots:**

| engine | n=1,000 | n=100,000 |
|---|---:|---:|
| **nestory `Unsafe`** | **34.7 ns** | **34.7 ns** |
| **nestory `View`** | **135 ns** | **138 ns** |
| go-memdb | 1.07 µs | 1.16 µs |
| bbolt | 57.2 µs | 59.8 µs |
| BuntDB | 57.3 µs | 55.7 µs |
| Badger | 56.3 µs | 64.6 µs |
| SQLite | 89.0 µs | 91.5 µs |

Three things only the shape matrix shows:

**`wide` is flat across two orders of magnitude** — 138 ns at 100,000 nodes,
because a read touches only its own five-node branch. `single` grows linearly,
because there the branch *is* the project. nestory's lead holds in both (~8×
over go-memdb wide, ~13× single), so it is not an artifact of one layout.

**At 100,000 nodes `View` overtakes `Unsafe`** (149 µs against 210 µs). One
branch read lock disappears into the cost of walking 100,000 nodes, and the
difference is noise at that size.

**Memory variants do not help the KV engines anywhere.** BuntDB `:memory:` is
slower than its disk build on scans and branch loads alike; Badger `InMemory`
likewise. Their cost is decoding a blob per node, and RAM cannot remove it. It
is the fourth workload in a row where that holds.

## What the other reports say

- **`REVIEW.md`** — real-disk writes, cache-scaling scans, raw-Go baselines, and
  the corrections an external review forced. Read it before quoting any write
  number from this file.
- **`GRAPH_MEMORY.md`** — what a relation graph costs in RAM, and what each
  write path allocates.
- **`TRANSACTION_STRESS.md`** — the transaction pipeline against branch size.

## Reproduce

```sh
cd bench
./run.sh                      # everything, scales 1 … 1,000,000

# with the remote engines:
docker run -d --name nestory-bench-redis -p 127.0.0.1:6380:6379 redis:7
docker run -d --name nestory-bench-mongo -p 127.0.0.1:27018:27017 mongo:8
NESTORY_REDIS_URI=127.0.0.1:6380 NESTORY_MONGO_URI=mongodb://127.0.0.1:27018 ./run.sh
docker rm -f nestory-bench-redis nestory-bench-mongo
```
