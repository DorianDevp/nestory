# Graph memory profile — what a relation costs

Measured 2026-07-25 with `TestGraphMemoryProfile`, one isolated subprocess per
(schema, size). Every sample is taken after `debug.FreeOSMemory()`, so it is
retained live heap, not recently-allocated garbage. Intel i5-8600K, linux/amd64.

```sh
NESTORY_MEMORY_PROFILE=1 go test -run '^TestGraphMemoryProfile$' -count=1 -v
```

## The two schemas

The control is one type, five fields, no relations:

```go
type memFlatRow struct {
    Id    int `key:"primary"`
    Name  string
    Kind  string
    Seq   int
    Score int
}
```

The subject is five types with two relations each, two levels of nesting under
two different ownership paths, plus a three-row lookup table that every document
borrows and every label optionally references:

```
memWorkspace ──own──> memProject ──own──> memDocument ──borrow──┐
     └────────own──> memLabel ─────────────option──────────────>┤
                                                          memTier (3 rows)
```

One unit is 1 workspace + 2 projects + 6 documents + 1 label = 10 nodes. The
shape is held constant at every size, so a size column compares like with like.
`memTier` also carries two `inverse` views, which is how the profile prices a
three-row table that 60,000 documents point at.

## Table 1 — retained heap, no relations

| nodes | live heap | B/node | after first write | B/node |
|---:|---:|---:|---:|---:|
| 10 | 0.3 MiB | — | 0.3 MiB | — |
| 100 | 0.3 MiB | — | 0.3 MiB | — |
| 1,000 | 0.4 MiB | 126 | 0.7 MiB | 440 |
| 10,000 | 2.0 MiB | 180 | 4.1 MiB | 401 |
| 100,000 | 17.2 MiB | **177** | 34.2 MiB | **356** |

The second pair of columns is the surprise: **one** write doubles the heap of a
table that has no relations at all, and every later write adds nothing. A heap
profile taken immediately after that single write attributes 18.5 MiB of the
35.8 MiB total to `buildCommittedRelationIndex` and `addGraphNode`, reached
through `UpdateWithin → Tx.Get → getInTransaction → relationApplyPending`.

A relation-free table therefore pays for a project-wide relation index the first
time it is written, and keeps it.

## Table 2 — retained heap, five related types

| nodes | roots | graph alone | B/node | + Tower replica | replica B/node | total B/node |
|---:|---:|---:|---:|---:|---:|---:|
| 13 | 1 | 0.4 MiB | — | 0.4 MiB | — | — |
| 103 | 10 | 0.6 MiB | — | 0.7 MiB | — | — |
| 1,003 | 100 | 2.9 MiB | 2,718 | 3.5 MiB | 627 | 3,345 |
| 10,003 | 1,000 | 22.3 MiB | 2,306 | 27.2 MiB | 514 | 2,820 |
| 100,003 | 10,000 | 202.8 MiB | **2,123** | 243.3 MiB | **425** | **2,548** |

Both columns converge from above, so the per-node cost is a genuine asymptote
rather than a fixed overhead being amortized.

Against the flat control measured the same way, **a related node costs 12× a
flat one** (2,123 B against 177 B) before the Tower is involved at all. The
Tower's 425 B/node — the number the previous report focused on — is the smaller
half of the problem by a factor of five.

## Table 3 — where the 202.8 MiB actually goes

Heap profile at 100,003 nodes, `inuse_space`, top allocation sites:

| site | MB | share of database |
|---|---:|---:|
| `incrementIncoming` | 58.42 | **28.2%** |
| `recordResolvedRelation` | 34.56 | 16.7% |
| `indexRelationNodeTargets` | 24.90 | 12.0% |
| `indexRelationFields` | 18.76 | 9.1% |
| `attachNodeOwner` | 12.58 | 6.1% |
| `addChild` | 10.08 | 4.9% |
| `addGraphNode` | 8.10 | 3.9% |
| **`storeCommittedOwnership` (cumulative)** | **121.42** | **58.6%** |
| `chunkStore.Append` (documents) | 9.21 | 4.4% |

**The relation index is roughly five times the size of the entities it indexes.**
Fifty-nine percent of the database is bookkeeping about the graph rather than the
graph.

## Table 4 — allocation per write, by API

Churn is `TotalAlloc` per iteration with the collector running normally. The
branch under test is 10 nodes at every size, so a flat row means the cost does
not depend on total graph size.

| write path | 13 | 103 | 1,003 | 10,003 | 100,003 |
|---|---:|---:|---:|---:|---:|
| `UpdateWithin` | 2,782 | 2,782 | 2,782 | 2,785 | 2,784 |
| `Tracked().UpdateWithin` | 2,798 | 2,800 | 2,802 | 2,798 | 2,798 |
| `Get` + `Update` | 8,660 | 8,663 | 8,660 | 8,662 | 8,662 |
| `Transaction` | 8,679 | 8,675 | 8,679 | 8,676 | 8,679 |
| rotating between roots | — | 3,485 | 5,051 | 5,081 | 5,053 |
| **mixing `UpdateWithin` with `Get`+`Update`** | 22,080 | 81,496 | 1,006,870 | 8,361,880 | **74,647,456** |
| **insert a row, then write a root** | 135,551 | 637,408 | 7,092,685 | 66,579,172 | **617,477,639** |

The first five rows are flat. The last two scale linearly with the whole project
and are three to five orders of magnitude larger.

### Why the last two rows explode

A Tower write ends in `captureEpochs()`, which absorbs the epoch bumps its own
commit produced, so the replica survives it. Nothing else does this. `applyWrite`
— the path behind `Get`+`Update` and `Transaction` — calls `db.markChanged()`
and nothing puts the epoch back, and so do `Unsafe` create/delete and `Flush`.

Any of those retires the replica. The next `UpdateWithin` finds `replica.Load()`
stale and runs `rebuild()`, which copies the **entire committed index**, not the
branch it was asked about:

```go
liveNodes := make(map[nodeKey]reflect.Value, len(index.nodes))
for key, value := range index.nodes {   // every node in the project
    liveNodes[key] = value
}
```

Alternating the two write APIs therefore rebuilds a shadow of the whole project
on every pair. Inserting a row does the same and costs eight times more, for a
second and independent reason.

### Why an insert costs eight times a mixed pair

`Unsafe().Flush()` does not *fall back* to a full rebuild — it has no other path.
The incremental machinery (`buildRelationCreateIndexDelta`, `publishAndRewire`)
exists only in the transaction engine, gated on `indexDelta != nil` at
`engine.go:464`. `flushRelations` instead runs `collectRelationNodes(true, nil)`
over every node of every type, rebuilds the model, and ends at
`storeCommittedOwnership(model, deleted)` — the unconditional full index build.

Splitting the insert from the write that follows it separates the two costs:

| nodes | insert via `Unsafe`+`Flush` | insert via `Transaction` | replica rebuild after the insert |
|---:|---:|---:|---:|
| 1,000 | 6,093,622 | 54,512 | 979,674 |
| 10,000 | 58,143,970 | 439,008 | 8,354,184 |
| 100,000 | **542,836,848** | **3,677,360** | **74,639,754** |

Two findings, not one:

1. **The same insert is 148× cheaper through a transaction** — 3.7 MB against
   543 MB at 100,000 nodes — purely because the transaction path has a delta and
   `Flush` does not. This is available today: staging creates through
   `Transaction`/`tx.Create` rather than `Unsafe().Create`+`Flush` removes almost
   all of it without any change to the library.
2. **The 74.6 MB that remains is entirely the Tower rebuild**, and it matches the
   mixed-API churn of 74,647,456 B to within 0.01% — the same cost reached by a
   different trigger. Giving `Flush` a delta would fix the first row and leave
   this one untouched.

## Table 5 — peak heap during one operation

Measured with the collector switched off across a single operation, so the
`HeapAlloc` delta is exactly that operation's high-water mark — the number an
out-of-memory kill would see.

| | 1,003 nodes | 10,003 nodes | 100,003 nodes |
|---|---:|---:|---:|
| database, live | 3.5 MiB | 27.2 MiB | 243.3 MiB |
| peak — mixing write APIs | 1.0 MiB | 8.0 MiB | 71.2 MiB |
| peak — insert then write | 6.8 MiB | 63.5 MiB | **589.3 MiB** |
| peak as a share of the database | 194% | 233% | **242%** |

A single insert followed by a single update transiently needs **2.4× the entire
database**, and the ratio is still climbing at 100,000 nodes.

## The weakest point

**Problem.** The committed relation index costs about five times what the data it
indexes costs, which sets both the resident floor and the rebuild price.

**Cause.** Two of its fields are maps of maps:

```go
type committedRelationIndex struct {
    incoming map[nodeKey]map[relationHolderField]int
    children map[nodeKey]map[nodeKey]struct{}
    // …
}
```

`incrementIncoming` allocates one Go hash table **per referenced node**:

```go
func (index *committedRelationIndex) incrementIncoming(target nodeKey, field relationHolderField) {
    if index.incoming[target] == nil {
        index.incoming[target] = make(map[relationHolderField]int)   // one map per node
    }

    index.incoming[target][field]++
}
```

It is worth being precise about what is *not* the cause. `nodeKey` is 24 B and
`relationHolderField` is 32 B, and the `reflect.Type` inside each is a two-word
interface pointing at a descriptor Go allocates once per type for the whole
program — not per node. The keys are cheap. The shape is not.

A direct measurement of the two shapes holding identical information, 100,000
targets with one incoming reference each:

| representation | B/node |
|---|---:|
| `map[nodeKey]map[relationHolderField]int` | **452** |
| `map[struct{target, holder nodeKey; field int}]int` | **94** |

The map-of-maps costs **4.8×** the flat map for the same data, because a Go map
allocates a whole group of slots on first insert regardless of how few entries it
will hold. The real index measures 584 B/node rather than 452 because this
fixture averages about 2.5 edges per node; the gap between the two shapes is the
part that is structural. `children` has the same shape for ownership.

**Effect.** Three consequences, in increasing order of severity:

1. The resident graph is 12× its flat equivalent.
2. Everything that rebuilds the index pays its size. That is why one insert
   costs 617 MB at 100,000 nodes: 543 MB because `Flush` rebuilds the index in
   full, and 75 MB because the Tower then rebuilds its replica over it.
3. Because the index is per-project rather than per-table, and is built
   unconditionally, a project with **no relations anywhere** pays for it too —
   Table 1's doubling. In that probe only `memFlatRow` was registered and it
   declares no relation at all, yet `addGraphNode` (10.5 MB) and
   `buildCommittedRelationIndex` (7.0 MB) still ran, which is the whole 175
   B/node the table gains on its first write.

The fix direction is already in the file. `nodeKeySet` at `relation_index.go:97`
uses `inline [8]nodeKey` with an overflow slice and only promotes to a map when a
node exceeds eight entries. That is exactly the idiom `incoming` and `children`
need; it was written for a different structure and not applied here.

## What has been fixed so far

Three changes, each measured with the profile above at 100,003 nodes.

**Step 1 — `incoming` reshaped, `children` deleted.** `incoming` went from
`map[nodeKey]map[relationHolderField]int` to `map[nodeKey]incomingCounts`, a
slice of pairs that only promotes to a lookup map above eight entries.
`children` turned out to be **written but never read** — ownership children are
served by `committedOwnership.outgoing` — so the field and its two methods were
removed outright.

**Step 2 — no relation graph for projects that have none.**
`ensureCommittedOwnership` now returns early when `relationParticipants` is
empty. The check is re-evaluated per call rather than cached in `ready`, so a
relation-carrying type registered later still gets its index built; every
consumer of `committedOwnership.index` already handled a nil index.

**Step 3 (partial) — `relationSpec` held by pointer.** `resolvedRelation` and
`incomingOwn` embedded a 96-byte `relationSpec` by value; `model.refs` holds one
`resolvedRelation` per edge in the project. Both now point into the slice
`relationSpecs` caches per type, which is built once and never mutated:
`resolvedRelation` 144 → 56 B, `incomingOwn` 120 → 32 B.

**Step 4 — the replica refreshes instead of rebuilding.** `rebuild()` now first
tries `refresh()`: when the committed node set is unchanged (same keys, same live
pointers) the shadows keep their addresses, so `blocks`, the `nodes` map and the
`live` map are all reused and only field values are re-copied. Anything that adds
or removes a node still falls back to a full rebuild, which is what keeps
`blocks`' never-grow invariant intact — a shadow address is handed to user code
inside an `UpdateWithin` callback and cached in `branches`, so a reallocated
block would leave both pointing at a copy nobody publishes. The caller already
holds `graphMu.Lock`, which excludes every reader, so mutating in place cannot
race a diff in progress.

| at 100,003 nodes | before | after 1+2 | after 3 | after 4 |
|---|---:|---:|---:|---:|
| graph alone | 202.8 MiB | 165.9 MiB | **147.2 MiB** | 147.2 MiB |
| B/node | 2,123 | 1,736 | **1,543** | 1,543 |
| with Tower replica | 243.3 MiB | 206.5 MiB | **187.7 MiB** | 187.7 MiB |
| mixed-API, churn | 74.6 MB | 74.6 MB | 74.6 MB | **42.3 MB** |
| mixed-API, peak | 71.2 MiB | 71.2 MiB | 71.2 MiB | **40.3 MiB** |
| insert-then-write, churn | 617.5 MB | 593.4 MB | **464.5 MB** | 464.5 MB |
| insert-then-write, peak | 589.3 MiB | 594.1 MiB | **444.1 MiB** | 444.2 MiB |

Step 4 leaves `insert-then-write` untouched by design: an insert grows the node
set, so it takes the rebuild fallback. What remains of the mixed-API path is
`cloneSliceFields`, which still allocates a fresh backing array per slice field
per node even when the slice contents did not change — copying into the existing
shadow slice when capacity allows is the obvious next step and is not done.

**Step 5 — `model.refs` sized from the last published model.** A full rebuild
reaches the same edge count the committed model already has, so
`buildRelationModelFromTargets` now presizes from `committedRelationRefCount()`.
That turns roughly eighteen append doublings — each copying and abandoning the
previous array — into one allocation. Flush churn 389.3 → 332.8 MB;
`insert-then-write` peak 444.2 → **389.7 MiB**.

**Steps 6-8 — stop rebuilding what is already known.**
`buildCommittedRelationIndex` filtered `model.nodes` into an identical map and
recomputed the target index from scratch, both from data the model already held;
the node map is now read from the model directly and the target index cloned.
`model.refs` is presized from the last published model instead of growing from
empty. `map[nodeKey][]incomingOwn` became a value record: a node has at most one
owner, so that map allocated a slice per node to hold one element.

| at 100,003 nodes | baseline | now |
|---|---:|---:|
| graph alone | 202.8 MiB | **146.4 MiB** |
| with Tower replica | 243.3 MiB | **186.9 MiB** |
| flush churn | 543 MB | **285 MB** |
| insert-then-write, peak | 589.3 MiB | **351.0 MiB** |
| peak as a share of the base | 242% | **188%** |

Two things this round are worth recording because they cut against the
optimization.

**A shared map was a latent corruption, and the tests passed.** The first version
also shared `model.targets` with the index. `publishAndRewire` assigns into
`index.targets` when a create is published, which would have written into the
model `committedOwnership.graph` still holds. `go test -race` was green with the
bug present; it was found by grepping mutation sites, not by testing. The index
clones the map instead.

**Loose map size hints cost more than they save.** Sizing `fields` from
`len(model.refs)` and `outgoing` from `len(nodes)` cut churn but raised the
resident graph from 146.5 to 168.0 MiB, because both maps are keyed by something
far less numerous than the hint. Only `incoming`, which really does take one
entry per node, keeps its hint.

**Steps 9-10 — size every rebuilt map from the previous rebuild.** Each
collection the flush rebuilds lands within a few entries of the size it had last
time, so `committedSizes` records those counts at publication and the next
rebuild sizes from them. A hintless map of this size pays about fifteen rehashes,
each allocating a larger table and abandoning the old one.

The hints travel as parameters rather than being read from `committedOwnership`:
`buildRelationTargetIndexFromFields` runs under `committedOwnership.Lock` on one
path, and Go's `RWMutex` is not reentrant. The first version fetched them inside
that function and deadlocked the whole suite.

| at 100,003 nodes | baseline | now |
|---|---:|---:|
| graph alone | 202.8 MiB | **146.5 MiB** |
| with Tower replica | 243.3 MiB | **187.1 MiB** |
| flush churn | 543 MB | **241 MB** |
| mixed-API, peak | 71.2 MiB | **40.3 MiB** |
| insert-then-write, peak | 589.3 MiB | **302.0 MiB** |
| peak as a share of the base | 242% | **161%** |

**Steps 11-12 — diff instead of rebuild.** `Flush` now compares the live graph
against the Tower shadow and skips the model and index rebuild entirely when
nothing moved. The shadow is the last committed state, so it *derives* the change
set rather than trusting a declaration — which is what makes it sound under
Unsafe's contract, where a caller may mutate through a pointer it kept since
creation and nothing records that. `refresh` likewise compares before copying,
instead of re-cloning every slice field to reproduce bytes that already matched.

Two things had to be right for the gate to ever open:

- **Identity, not `synced()`.** `Unsafe.Flush` bumps the table epoch before
  reaching the check, so `synced()` is always false by then. What matters is that
  the replica's baseline came from the index still published, which pointer
  identity states exactly.
- **Compare by id, not by pointer.** A relation field holds shadow pointers on
  the shadow side and live pointers on the live side. Comparing them as pointers
  reports a difference on every node with a relation, always — the first version
  did, and silently never took the fast path.

| at 100,003 nodes | baseline | now |
|---|---:|---:|
| graph alone | 202.8 MiB | **146.4 MiB** |
| with Tower replica | 243.3 MiB | **187.0 MiB** |
| **mixed-API, peak** | 71.2 MiB | **0.013 MiB** |
| **mixed-API, churn** | 74,647,456 B | **13,624 B** |
| **flush after in-place mutation, peak** | 210.4 MiB | **0.001 MiB** |
| **flush after in-place mutation, churn** | 220,769,016 B | **1,536 B** |
| insert-then-write, peak | 589.3 MiB | **293.0 MiB** |

**Step 13 — refresh only the nodes that moved.** An untouched node keeps its
wiring and its relation baseline, because refresh reuses shadow addresses: a
pointer into an untouched node is still the right pointer. Rebuilding both for
the whole project allocated a `reflect.MakeSlice` baseline per owner and a map
entry per node on every refresh.

The mutation flush is the shape this was aimed at: 143,000x less churn, and a
peak that no longer scales with the graph at all.

**Steps 14-16 — the flush settles against the shadow.** Three changes close the
create path:

- `graphMovedNodes` classifies every divergence between live and shadow:
  relation fields by id, lookup-key fields by value. A lookup-key change poisons
  the committed target index, so it forces the full rebuild; anything else is a
  change set the delta can take. This also closed a soundness hole in the first
  version of the gate, which compared only relation fields — a mutated match
  field slipped through it.
- The tower refresh absorbs **created** nodes through fresh per-table blocks, so
  no existing shadow address ever moves and a create no longer costs a
  whole-project tower rebuild.
- `flushIncremental` feeds the derived change set plus the queued creates to
  `buildRelationCreateIndexDelta` — the same tested route a transactional create
  takes — and publishes the delta. Deletes, key changes and builder refusals
  still fall through to today's full rebuild, so every rejection lands in the
  old path.

A related correctness bug was found and fixed on the way: the refresh's
moved-node detection skipped relation fields entirely, which left a reordered
own slice stale in the shadow. Regression test included.

| at 100,003 nodes | baseline | now |
|---|---:|---:|
| graph alone | 202.8 MiB | **146.5 MiB** |
| with Tower replica | 243.3 MiB | **187.0 MiB** |
| mixed-API, peak | 71.2 MiB | **0.013 MiB** |
| flush after in-place mutation, peak | 210.4 MiB | **0.002 MiB** |
| **insert-then-write, peak** | 589.3 MiB | **6.3 MiB** |
| **insert-then-write, churn** | 617.5 MB | **6.6 MB** |

### Where this stands, against the stated target

The target was an increment of at most 50% of the live base, aiming at 5%. The
measured worst-case increment is now **6.3 MiB against a 187.0 MiB base — 3.4%.
The target is met, including the stretch goal.** Every write path in the profile
now peaks below 7 MiB regardless of graph size; the only operations that still
pay a full O(project) rebuild are deletes and lookup-key mutations, by design.

A side effect worth knowing: a delta publish nils the retained committed model
(`publishAndRewire` has always done this), so steady-state live heap after
delta-based flushes drops to **137.4 MiB** — the model is rebuilt only when a
full rebuild is next needed.

Of the remaining 6.6 MB per insert, most is `relationApplyPending` building an
`existing` map over the whole table to check for duplicate ids, and the
per-table reindex/save sweep — both proportional to table size, not graph size,
and neither specific to the delta path.

## Failure modes this architecture permits

No critical threshold appears *within* the measured range in the sense of a cliff
— every curve is linear or flat. The danger is that two of the curves are linear
with a very large constant, and nothing in the system bounds them.

1. **Transient OOM kill under a normal workload.** 589 MiB peak on a 243 MiB
   database. A 512 MiB container limit dies; a 1 GiB limit survives 100,000
   nodes and dies at roughly 250,000. Because the spike is transient it will not
   appear in steady-state memory monitoring — it presents as a random `OOMKilled`
   with a healthy-looking memory graph.

2. **Collector saturation that looks like a hang.** 617 MB of churn per
   insert-then-write pair means ten such operations per second allocate ~6 GB/s.
   With a 243 MiB live set and default `GOGC=100`, every single operation blows
   through the next GC target. The process would spend most of its time
   collecting; throughput collapses without any error being returned.

3. **Every table is taxed for a graph, whether or not one exists.** Table 1 shows
   a relation-free type paying 18.5 MiB at 100,000 rows on its first write, in a
   project where *nothing* declares a relation. This is not triggered by adding
   `rel:"own,…"` somewhere — `UpdateWithin` reaches `relationApplyPending →
   buildCommittedRelationIndex` unconditionally, and `addGraphNode` inserts every
   node of every registered table into `index.nodes`. A user who never touches
   relations still pays roughly 2× for using the safe write path at all, with no
   diagnostic pointing at the cause.

4. **Unbounded branch cache.** `branches` is filled lazily per root written and
   never evicted for the replica's life. Rotating roots holds the replica valid
   but grows the cache — visible as the live heap creeping from 27.2 to 27.3 MiB
   over 32 distinct roots at 10,003 nodes. A long-lived replica over a project
   with many roots accumulates `104 B × nodes-in-every-root-ever-written`.

5. **Failure is always death, never an error.** There is no cap on replica size,
   no eviction, and no path that declines a rebuild it cannot afford. Every
   overload above ends as an OOM kill rather than a returned error, which leaves
   an operator no way to shed load or degrade gracefully.

Extrapolating the linear fits: at 1,000,000 nodes the graph is ~2.4 GiB resident
and an insert-then-write pair peaks near 6 GiB. That is an extrapolation, not a
measurement — the profile stops at 100,003 nodes.
