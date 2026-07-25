# nestory versus in-memory databases

Same-machine, same-session comparison of nestory against five engines running
**purely in memory**: no file, no fsync, state lives and dies with the process.
This is the companion to `COMPARISON.md`, which compares against the same
engines in their durable configurations — here the competitors give up
durability, while nestory keeps its WAL, so every ratio in this file is tilted
*against* nestory by design.

> **Methodology.** One session on the machine below, `benchtime=300ms`,
> `-benchmem`. The record is identical everywhere: `{Id int, Name, Email
> string, Age int}`. nestory's numbers come from `../bench_test.go` at commit
> `4717f03`; the competitors from `./compare/inmemory_test.go`. go-memdb was
> re-run in the same session even though it has no separate memory mode — it
> only has one.
>
> **The tmpfs caveat.** `/tmp` on this machine is tmpfs, and every benchmark
> tempdir lives there — so nestory's "durable" WAL fsync lands in RAM, not on a
> disk platter. This makes the in-memory comparison *more* level (everyone
> ultimately writes to RAM), but it means nestory's write numbers here should
> not be quoted as disk-durable throughput. The same caveat applies to every
> durable number in `COMPARISON.md`.

## Environment

- CPU: Intel Core i5-8600K @ 3.60 GHz (6 cores), linux/amd64.
- Embedded: `modernc.org/sqlite` (`:memory:`, single connection),
  `tidwall/buntdb` (`:memory:`), `dgraph-io/badger/v4` (`WithInMemory(true)`),
  `hashicorp/go-memdb`.
- Server: Redis 7 in Docker, loopback TCP, `appendonly` off (its default) — the
  canonical in-memory database, included the way MongoDB is included in
  `COMPARISON.md`, and with the same caveat: its ratios mostly measure
  *embedded versus networked*.

| engine | model | persistence in this run |
|---|---|---|
| **nestory** | in-RAM pointer graph | **WAL append + fsync per write (only durable entrant)** |
| go-memdb | immutable-radix MVCC | none |
| SQLite `:memory:` | B-tree, SQL | none |
| BuntDB `:memory:` | in-memory KV, gob values | none |
| Badger `InMemory` | LSM, gob values | none |
| Redis 7 | remote in-memory KV, gob values | none |

## Bulk insert — time per batch (lower = better)

| engine | n=100 | n=1,000 | n=10,000 | rows/s @10k | B/op @10k | allocs/op @10k |
|---|---:|---:|---:|---:|---:|---:|
| **nestory** | **87 µs** | **0.48 ms** | **4.29 ms** | **2.33 M** | 4.88 MB | **11,169** |
| go-memdb | 73 µs | 0.76 ms | 8.31 ms | 1.20 M | 4.96 MB | 110,445 |
| SQLite mem | 182 µs | 1.68 ms | 16.6 ms | 604 k | **3.59 MB** | 148,999 |
| Badger mem | 270 µs | 2.33 ms | 24.7 ms | 405 k | 17.6 MB | 329,705 |
| BuntDB mem | 228 µs | 2.41 ms | 24.7 ms | 405 k | 17.1 MB | 259,778 |
| Redis | 372 µs | 2.62 ms | 27.5 ms | 364 k | 17.4 MB | 289,458 |

nestory batches n rows behind **one** WAL frame and fsync, and still inserts
1.9× faster than go-memdb's non-durable MVCC batch, with 10× fewer allocations.
Dropping the fsync did not move the competitors much: comparing against their
durable numbers in `COMPARISON.md`, SQLite gained ~20% and BuntDB/Badger ~10%,
which says their bulk-insert cost was never the disk — it is per-row
transaction and encoding machinery.

## Point read by id — ns/op (lower = better)

| engine / API | n=100 | n=1,000 | n=10,000 | reads/s @10k | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| **nestory `Unsafe.Get`** | 31 | 32 | **32** | **31.4 M** | **0** | **0** |
| **nestory `View`** | 53 | 53 | **53** | **18.8 M** | **0** | **0** |
| **nestory detached `Get`** | 195 | 201 | **189** | 5.29 M | 96 | 2 |
| go-memdb | 214 | 281 | 239 | 4.19 M | 208 | 6 |
| SQLite mem | 2,367 | 2,608 | 2,560 | 391 k | 712 | 27 |
| Badger mem | 11,258 | 11,433 | 11,332 | 88.2 k | 8,144 | 173 |
| BuntDB mem | 12,164 | 12,094 | 11,696 | 85.5 k | 7,480 | 168 |
| Redis | 53,871 | 53,893 | 53,374 | 18.7 k | 7,624 | 172 |

The ordering is the whole story of what each engine *is*. nestory returns the
live pointer: no lookup beyond one map hop, nothing to decode. go-memdb walks a
radix tree. SQLite executes a query plan even from RAM — taking away its file
cut the read from 11.0 µs to 2.6 µs, and the remaining 2.6 µs is the SQL
machinery itself. Badger and BuntDB pay gob decoding per read (~170
allocations), which RAM cannot help with. Redis pays a loopback round trip: at
53 µs it is slower *in memory* than SQLite is *on disk*.

## Point write — µs/op (lower = better)

| engine | n=100 | n=1,000 | n=10,000 | writes/s @10k | B/op @10k | allocs/op @10k |
|---|---:|---:|---:|---:|---:|---:|
| BuntDB mem | 2.31 | 2.44 | **2.36** | 424 k | 1,968 | 30 |
| **nestory (fsynced WAL)** | 2.79 | 2.79 | **2.80** | 357 k | 752 | 14 |
| go-memdb | 3.07 | 4.13 | 4.32 | 232 k | 9,082 | 62 |
| SQLite mem | 6.27 | 6.47 | 6.49 | 154 k | **176** | **7** |
| Badger mem | 7.46 | 7.59 | 7.47 | 134 k | 3,432 | 51 |
| Redis | 41.6 | 42.5 | 42.7 | 23.4 k | 1,712 | 31 |

**This is the one workload where an in-memory competitor beats nestory, and the
margin is 16%** — BuntDB doing a bare in-memory set against nestory writing and
fsyncing a WAL frame plus taking its safe-API locks. Buy durability back for
BuntDB (`SyncPolicy=Always`, `COMPARISON.md`) and it costs 4.3 µs; nestory's
2.80 µs already includes it. Everyone else is behind nestory even with no
persistence at all.

## Full scan + filter — time per scan (lower = better)

| engine | n=100 | n=1,000 | n=10,000 | rows scanned/s @10k | B/op @10k |
|---|---:|---:|---:|---:|---:|
| **nestory `Filter`** | 445 ns | 5.04 µs | **46.8 µs** | **214 M** | 15.7 kB |
| go-memdb | 795 ns | 5.70 µs | 57.2 µs | 175 M | **312 B** |
| SQLite mem | 14.5 µs | 68.9 µs | 609 µs | 16.4 M | 15.5 kB |
| Badger mem | 1.10 ms | 11.0 ms | 109 ms | 91.7 k | 76.1 MB |
| BuntDB mem | 1.21 ms | 11.9 ms | 115 ms | 87.3 k | 74.1 MB |
| Redis | 1.37 ms | 16.8 ms | 134 ms | 74.6 k | 76.1 MB |

nestory scans a contiguous `[]T` with a predicate — the CPU prefetcher's
favourite shape. The KV engines gob-decode every value to look at one field, so
their scan is ~2,300× slower and allocates 76 MB per pass; that is the price of
storing blobs, not of being on disk. Redis additionally ships every value over
the socket to filter client-side (`SCAN`+`MGET`), which is the honest
translation of this workload and also exactly why you would not use Redis this
way.

## How to read this — the honest caveats

1. **nestory is the only durable entrant.** Every competitor here was stripped
   of persistence; nestory kept its WAL and fsync. It wins three of four
   workloads anyway, and loses the fourth by 16% to an engine that dropped
   durability to get there.
2. **tmpfs blunts the durability edge.** The fsync lands in RAM on this
   machine, so nestory's write cost here is a lower bound on what a real disk
   would show — as is every durable number in `COMPARISON.md`.
3. **gob is a harness choice.** Badger, BuntDB and Redis store gob blobs; a
   leaner codec would cut their read and scan costs, though not the per-read
   allocation shape.
4. **Redis is measured through a socket.** Its numbers say what an
   out-of-process in-memory store costs, not what its storage engine can do.
   Same category error as MongoDB in `COMPARISON.md` — deliberately included,
   clearly labeled.
5. **These are indicative, not publication-grade** — one machine, one run,
   short benchtime.

## Verdict

Taking durability away from the competitors barely changed the ranking from
`COMPARISON.md`, which is the finding: **the competitors' costs were never
about the disk.** SQLite's read cost is SQL execution, the KV stores' cost is
value encoding, Redis's cost is the network hop. nestory's model — live
pointers, no serialization boundary, contiguous storage — is faster in RAM than
engines built for RAM: 7.5× go-memdb on reads, 1.9× on batch insert, 1.2× on
scans, and 357 k durable writes/s against engines doing non-durable ones.

## Reproduce

```sh
# nestory (from repo root):
go test -run='^$' -bench='BatchInsertFlush|ViewByID|SafeGetByIDOnly|UnsafeGetByID|PointWrite|Filter' \
  -benchmem -benchtime=300ms

# in-memory competitors (from ./bench):
go test ./compare/ -run='^$' -bench='SQLiteMem|BuntMem|BadgerMem|Memdb' -benchmem -benchtime=300ms

# Redis — skipped unless a server is reachable (from ./bench):
docker run -d --name nestory-bench-redis -p 127.0.0.1:6380:6379 redis:7
NESTORY_REDIS_URI=127.0.0.1:6380 go test ./compare/ -run='^$' -bench=Redis -benchmem -benchtime=300ms
docker rm -f nestory-bench-redis
```
