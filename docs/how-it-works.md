# How Nestory works

Nestory combines an in-memory object graph with WAL-backed durability. The live
representation is optimized for direct Go access; the disk representation is a
recoverable, flattened record stream.

## Memory layout

Each entity type has a chunked arena made of fixed 512-row blocks. Existing
slots never move when a new block is added, so pointers held by the relation
graph remain stable across inserts.

Every live row has:

- its stable object slot;
- an integer ID lookup;
- a resource version for optimistic conflict detection;
- a resource lock;
- derived secondary-index entries;
- relation-index entries when the type participates in the graph.

Deletes tombstone slots. Snapshot compaction removes deleted rows from the
durable representation without invalidating unrelated live pointers.

The complete working set is loaded into RAM during registration. Nestory does
not page cold objects in or out.

## Disk layout

Each type persists under:

```text
<DataDir>/<Type>/<chunk>.gob
<DataDir>/<Type>/wal.log
<DataDir>/transactions.wal
```

Chunk files are snapshots of flattened rows. Relation pointers are stored as
matching keys rather than process addresses. Registration loads chunk files in
parallel, replays newer WAL records, and inflates rows. Opening the databases
then rebuilds indexes and rewires canonical pointers.

Scalar fields use a compact direct codec when supported, including raw `[]byte`
payloads. More complex row shapes fall back to schema-aware gob encoding.

## Durability

The normal commit order is WAL before publication:

1. validate resource versions, relations, and indexes;
2. encode the changed rows and tombstones;
3. append and fsync the WAL frame;
4. publish objects and derived indexes in memory.

A transaction confined to one entity type uses that type's WAL. A transaction
that spans entity types is encoded into one frame in the instance-wide
`transactions.wal`; replay therefore sees either the whole frame or none of it.
A torn frame at the end of either WAL is ignored during recovery.

Safe commits do not rewrite snapshot chunks on every update. `Unsafe().Flush`
is currently the explicit checkpoint/compaction boundary: it writes dirty
chunks atomically via replacement files and truncates compacted WAL state.

## Safe transaction pipeline

`Get` and transaction reads clone the requested root plus its complete `own`
subtree. They preserve ordinary Go mutation: no proxy, setter, or wrapper is
needed. The engine retains the original values and resource versions.

DB-level `UpdateWithin` uses a different path for ownership roots with
children. Tower keeps one project-wide shadow graph, partitioned into typed
tables, plus relation-ID baselines and a bounded cache of the ownership
branches that have been written. A callback edits only the shadow. Publication
then:

1. verifies that no commit outside Tower bumped a participating table's epoch;
2. compares the hot branch semantically, using compiled field plans;
3. creates shallow patch bases only for changed nodes and deep-copies only
   changed slice fields;
4. validates versions and indexes, writes the WAL, and writes through the
   stable canonical pointers.

The replica is published through an `atomic.Pointer`, so a callback never holds
anything a rebuild needs. Writes serialize per top-level owner rather than per
project: roots in separate trees commit in parallel, nested roots queue. `View`
keeps reading the previous committed state throughout; only the short
publication window takes the graph lock.

A comparison decides everything. Field values compare through a comparator the
compiler emits per entity type rather than through reflection, and relation
fields compare by **id**, never by pointer. The shadow world and the live world
never share addresses, so a pointer comparison would report every node as
changed. Contiguous runs of scalar fields settle in one memory comparison, which
is sound in the one direction that matters: equal bytes imply equal values.

The first Tower use is deliberately expensive: it clones and wires the project
shadow and records relation identities. After that it is refreshed rather than
rebuilt: only nodes whose values moved are re-copied, and created nodes are
absorbed into fresh per-table blocks so no existing shadow address ever moves.
A commit that replaces or removes a node still takes the full rebuild.

The replica increases retained memory, but warm transactions avoid the much
larger temporary pair of `work` and `original` copies for every node.

`Tracked().UpdateWithin` narrows step 2. The callback declares each node it
writes through `Edit`, and the commit diffs only those plus the root. Setting
`AuditTrackedWrites` re-runs the full branch diff and fails an undeclared write
with `ErrUndeclaredWrite`; it costs what the declaration saves, so it belongs in
tests rather than production.

At commit it:

1. detects which detached resources actually changed;
2. classifies the change as flat, graph-read, or graph-write work;
3. builds a local relation/index delta where possible;
4. locks structural state and resources in deterministic order;
5. verifies versions and final-state invariants;
6. writes the WAL;
7. publishes rows, indexes, ownership changes, and targeted pointer rewires.

A stale version produces `ErrConflict`. No live object is changed before the
durable frame succeeds.

## Relation graph

The committed graph maintains derived indexes for:

- the ordered targets of every relation field;
- the owner of each node;
- children grouped by owner;
- incoming holders grouped by target.

Those indexes make cascade closure, outsider-borrow checks, inverse rewiring,
and ownership-cycle checks local to the changed area in the common case.
Creates, deletes and relation edits publish deltas when their effect can be
settled against the committed graph. A changed lookup key, or any shape the
delta builder declines, takes the full validation and rebuild path. The fast
path only ever refuses work, never guesses at it.

`Unsafe().Flush` uses the same machinery. Its contract is that the caller holds
exclusive access until the flush completes, not that it re-acquires every
pointer, so it cannot know which nodes were touched. A caller may mutate
through a pointer it has held since creation. It therefore does not ask: it
compares the live graph against the Tower shadow, which *is* the last committed
state, and derives the change set. If nothing structural moved, the model and
index are already correct and the flush only persists values.

Validation and publication of a structural change share the global graph write
lock. This closes the window in which a parallel transaction could otherwise
add a borrow after a delete was validated. Read-only graph access takes the
read side of that lock.

Flat, relationless scalar writes bypass the graph lock entirely. They use row
and per-type structural locks, so independent flat datasets do not serialize on
relation maintenance.

## Read paths

- `Get` creates a mutable detached ownership branch.
- `View` exposes the stable live ownership branch under a single read lock held
  at the branch's top-level owner. Writers take the branch locks of every root
  they touch, in the same total order they use for rows, so the two classes
  cannot form a cycle; readers take no row locks at all. Locking each row
  instead meant one atomic pair per node, scattered across as many cache lines,
  which dominated the cost of reading a graph.
- `ViewMany` locks selected rows in ID order.
- `ViewRange` resolves an ordered secondary-index prefix and locks those rows;
  `ViewRangeAfter` starts after an exclusive cursor for incremental consumers.
- `Unsafe.Get` exposes the live pointer without locks or copies.

The benchmark report measures these paths separately because they provide
different guarantees. See [bench/BENCHMARK_REPORT.md](../bench/BENCHMARK_REPORT.md).

## Current limitations

Nestory is alpha software. Current constraints include:

- The complete dataset must fit in RAM, and a relation graph costs several times
  the size of the records in it. The indexes over the graph outweigh the graph.
- Startup is O(data): there is no lazy loading, so a process is not ready until
  the whole set is materialized and its pointers wired. This is a one-time cost
  that any long-lived service amortizes, and a per-request cost that a
  scale-to-zero deployment never does.
- An unbatched durable write costs one device flush. Batching amortizes it over
  the whole batch; a workload of single writes is bounded by the disk, not by
  Nestory.
- Primary keys are `int`; string IDs require a unique secondary key.
- `DataDir`, type registries, and the transaction engine are process-global.
  Multiple independent Nestory stores in one process are not yet supported.
- There is no public close/lifecycle API yet.
- Schema and disk-format migrations are not implemented. Renaming or changing
  persisted fields may make existing data unreadable.
- The on-disk format has no compatibility guarantee between alpha versions.
- `Filter` and unindexed `FindOneBy` perform full scans.
- `View` cannot enforce read-only pointers in Go's type system.
- `Unsafe` requires external exclusion and cannot roll back rejected live
  mutations.
- A relation inside an embedded value is not wired; embed leaf data only.
- Snapshot compaction is tied to the unsafe `Flush` boundary rather than a
  background checkpoint manager.

These are product constraints, not hidden behavior. Design data ownership and
recovery around them until the corresponding APIs stabilize.
