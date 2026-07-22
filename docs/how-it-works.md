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
Creates and relation edits publish deltas when their keys can be resolved
against the committed graph. Deletes, changed match keys, or an unsupported
delta shape can use a full validation/rebuild path for correctness.

Validation and publication of a structural change share the global graph write
lock. This closes the window in which a parallel transaction could otherwise
add a borrow after a delete was validated. Read-only graph access takes the
read side of that lock.

Flat, relationless scalar writes bypass the graph lock entirely. They use row
and per-type structural locks, so independent flat datasets do not serialize on
relation maintenance.

## Read paths

- `Get` creates a mutable detached ownership branch.
- `View` read-locks and exposes the stable live ownership branch.
- `ViewMany` locks selected rows in ID order.
- `ViewRange` resolves an ordered secondary-index prefix and locks those rows.
- `Unsafe.Get` exposes the live pointer without locks or copies.

The benchmark report measures these paths separately because they provide
different guarantees. See [bench/COMPARISON.md](../bench/COMPARISON.md).

## Current limitations

Nestory is alpha software. Current constraints include:

- The complete dataset must fit in RAM.
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
