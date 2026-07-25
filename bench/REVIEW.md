# What the earlier benchmarks got wrong

An external review raised four objections to `COMPARISON.md` and `INMEMORY.md`.
All four were correct. This file reports what happened when each was measured
instead of argued, and it contradicts headline claims in both of those files —
the corrections are noted there, but the numbers live here.

Machine as before: Intel i5-8600K, linux/amd64. "Real disk" is ext4 on LVM on a
Samsung 980 PRO NVMe (`/home`), against the previous runs' tmpfs `/tmp`.

## 1. The tmpfs caveat was load-bearing, not a footnote

An `fsync` of 64 bytes costs **6.5 ms** on the real filesystem and effectively
zero on tmpfs. Every durable write number ever published for this project was
measured on the second one.

| point write, n=1,000 | tmpfs | real disk | factor |
|---|---:|---:|---:|
| **nestory** | 2.80 µs | **6.46 ms** | 2,300× |
| BuntDB | 4.30 µs | 6.29 ms | 1,460× |
| Badger | 11.0 µs | 3.71 ms | 337× |
| bbolt | 19.6 µs | 4.15 ms | 212× |
| SQLite | 50.7 µs | 19.6 ms | 386× |

**The ranking inverts.** On tmpfs nestory led every durable engine; on a real
device it is fourth of five:

| durable point write, real disk | µs/op | writes/s |
|---|---:|---:|
| Badger | 3,712 | 269 |
| bbolt | 4,151 | 241 |
| BuntDB | 6,292 | 159 |
| **nestory** | **6,457** | **155** |
| SQLite | 19,591 | 51 |

The engines that overtake nestory do so by not paying a device flush per write —
Badger's value log and bbolt's page-level commit amortize where nestory's
per-write WAL frame does not. **"357 k durable writes/s" describes a filesystem
that loses your data on power failure.** The honest figure for an unbatched
durable write on this machine is ~155/s, and it is a property of the device.

What survives is batching:

| bulk insert, 10,000 rows, real disk | ms/op | rows/s |
|---|---:|---:|
| **nestory** | **27.6** | **363 k** |
| SQLite | 58.3 | 172 k |
| bbolt | 77.3 | 129 k |

One fsync amortized over 10,000 rows keeps nestory 2.1× ahead of SQLite and
2.8× ahead of bbolt, and the ratio to its own tmpfs number is only 6.4×. So the
correct claim is not "nestory writes fast" but **"nestory batches well, and is
unremarkable at unbatched durable writes."**

## 2. The advantage on graphs is real, and the safe API gives it away

The flat record hid the shape nestory exists for. Same ownership graph in three
engines — 200 workspaces × 5 projects × 10 documents — reading one workspace
with everything it owns (56 nodes):

| engine / API | ns/op | allocs/op | vs nestory raw |
|---|---:|---:|---:|
| **nestory, pointer traversal** | **64.7** | **0** | 1× |
| **nestory, safe `View`** | **177** | **0** | 2.7× |
| go-memdb | 2,241 | 52 | 35× |
| SQLite | 202,049 | 570 | 3,120× |

`View` cost **4,967 ns** when the review landed — 2.2× *slower* than go-memdb on
the workload nestory exists for. Two rounds of profiling fixed it:

1. **`committerFor(key.typ.Name())` per node, twice.** A reflect metadata parse
   plus a string map lookup, run once per lock and once per unlock. Branch keys
   are sorted by type name, so one lookup per contiguous run replaces one per
   node. 4,967 → 2,828 ns.
2. **One read lock per row.** 112 atomic read-modify-writes scattered across 56
   cache lines, which the profile then showed as 46% of what remained. Ownership
   branches now carry a single `RWMutex` at their top-level owner: a reader takes
   that and nothing else, and writers take the branch locks of the roots they
   touch — in the same total order they already use for rows, so the two classes
   cannot form a cycle. 2,828 → **177 ns**.

The first draft of this file asserted the locks were the problem without
profiling. They were, eventually — but only after the type lookups, which were
larger, were removed. Both numbers came from measurement, in that order.

Two findings, pulling opposite ways:

**The data model is worth more than any flat benchmark showed.** 64.7 ns for 56
nodes is **1.15 ns per node** — the branch is already assembled, so reading it
is pointer arithmetic. go-memdb has to do an index lookup per level, SQLite has
to run three queries and rebuild objects from rows. Against SQLite this is
3,120×, far beyond the ~200× the flat point-read table suggested. The reviewer
asked whether the advantage grows with relations: it grows by an order of
magnitude.

**And the safe API now costs 2.7× the raw traversal, not 77×.** `View` at 177 ns
against 64.7 ns of pointer walking is one uncontended read lock plus the walk —
**12.7× faster than go-memdb** and 1,140× faster than SQLite, with zero
allocations against their 52 and 570.

The review's implicit question was whether nestory's advantage on graphs is real
or an artifact of the flat record. It is real and it is large — but it took
replacing the read-lock protocol to collect it, and until the review forced the
measurement, the safe path was losing to go-memdb on nestory's home ground.

## 3. Scan throughput past the cache — no cliff

The published 214 M rows/s came from 10,000 rows (560 kB, L2-resident). Scaling
until the data leaves every cache level:

| rows | nestory `Filter` | ns/row | raw Go slice loop | ns/row |
|---:|---:|---:|---:|---:|
| 10,000 | 213 M/s | 4.68 | 1,162 M/s | 0.86 |
| 100,000 | 165 M/s | 6.08 | 865 M/s | 1.16 |
| 1,000,000 | 153 M/s | 6.53 | 501 M/s | 2.00 |
| 5,000,000 | 151 M/s | 6.60 | 490 M/s | 2.04 |

nestory loses **29%** leaving cache and then flattens — no `TRANSACTION_STRESS`-style
non-linearity. The raw slice loses **58%** over the same range, so the gap
between them *narrows* with size (5.4× → 3.2×): both converge on memory
bandwidth, and the contiguous layout is what lets nestory converge at all. The
scepticism was warranted; the structure held.

Quote **151 M rows/s** for scans that do not fit in cache, not 214 M.

## 4. What it costs to be a database

Raw Go with no durability, locking, versioning or validation:

| operation | raw Go | nestory | overhead |
|---|---:|---:|---:|
| point read by id | 7.0 ns (map) | 31.8 ns (`Unsafe.Get`) | 4.5× / +25 ns |
| scan, 5 M rows | 2.04 ns/row | 6.60 ns/row | 3.2× |
| point write, in-process | 7.1 ns (slice) | — | — |
| point write, durable | — | 6.46 ms (real disk) | ~10⁶× |

Reads and scans sit within a small constant of raw Go: the engine costs about
25 ns per read and ~4.5 ns per scanned row, and everything else in the
cross-engine tables is other engines' serialization and query machinery, not
nestory's cleverness. Durability is the entire cost structure of writes, and it
is the device's, not the engine's.

## Corrections this forces

- **`INMEMORY.md`** claims nestory "wins three of four workloads" while being
  the only durable entrant. True on tmpfs; on a real device it is fourth of five
  on point writes. The bulk-insert, read and scan conclusions stand.
- **`COMPARISON.md`**'s durable point-write table ranks engines by an effect
  that disappears on real hardware. Its `View` row (56 ns for a relation-free
  type) was never wrong, but it hid that the same call on a real ownership
  branch cost 4,967 ns until this round.
- Any external quotation of **357 k writes/s** should be **155 writes/s
  unbatched, 363 k rows/s batched, on this NVMe**.
- Nothing in the memory reports is affected — those measured allocation and
  retention, which tmpfs does not touch.

## Reproduce

```sh
# Real-disk writes (TMPDIR must be on a real filesystem):
mkdir -p ~/.nestory-bench-disk
TMPDIR=~/.nestory-bench-disk go test -run='^$' -bench='PointWrite|BatchInsertFlush' -benchtime=30x
cd bench && TMPDIR=~/.nestory-bench-disk go test ./compare/ -run='^$' \
  -bench='SQLite_PointWrite|Bolt_PointWrite|Bunt_PointWrite|Badger_PointWrite' -benchtime=20x

# Ownership graph across engines:
go test -run='^$' -bench=GraphNestoryLoadBranch -benchmem -benchtime=300ms
cd bench && go test ./compare/ -run='^$' -bench='GraphSQLite|GraphMemdb' -benchmem -benchtime=300ms

# Cache scaling and raw-Go baselines:
go test -run='^$' -bench='FilterAtScale|BaselineScan|BaselineMapGet' -benchmem -benchtime=20x
```
