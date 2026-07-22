# Indexes and batch operations

Nestory always indexes `Id`. Additional indexes are declared on struct fields,
validated during `Register`, rebuilt during `Open`, and updated atomically with
safe commits.

## Single-field unique indexes

```go
type Message struct {
	Id        int
	MessageID string `key:"unique"`
}
```

`key:"unique"` creates an index named after the field (`MessageID` here).
Duplicates fail with `nestory.ErrUniqueViolation` before the commit is
published.

A single-field unique index also accelerates `FindOneBy`:

```go
message, err := messages.FindOneBy("MessageID", externalID)
```

`FindOneBy` still works for a field without an index, but performs a full scan.

## Ordered composite indexes

Fields in a composite index use the same name and consecutive positions
starting at 1:

```go
type Message struct {
	Id        int
	MessageID string `key:"unique"`
	SessionID string `index:"session_seq,1,unique"`
	Seq       int    `index:"session_seq,2,unique"`
	Payload   []byte
}
```

The example creates an ordered index named `session_seq` on
`(SessionID, Seq)`. Because every participating tag includes `unique`, the
complete tuple is unique. Omitting `unique` from all fields creates a non-unique
ordered index.

All fields of one composite index must agree about uniqueness. Positions must
be consecutive, with no duplicates or gaps. Supported field kinds are:

- `bool`
- signed and unsigned integers
- `string`

Pointers, slices, floats, structs, and interfaces cannot currently be indexed.

## Ordered range views

`ViewRange` accepts a typed leading prefix. An index on `(SessionID, Seq)` can
return one session in sequence order without scanning other sessions:

```go
err := messages.ViewRange(
	"session_seq",
	[]any{sessionID},
	func(rows []*Message) error {
		for _, row := range rows {
			consume(row.Payload)
		}
		return nil
	},
)
```

An empty prefix returns the whole index in index order. A full prefix selects
one tuple or all rows sharing that tuple for a non-unique index. Prefix values
must have exactly the declared Go types.

Rows are stable live pointers, read-locked for the callback. They are read-only
by contract and must not be retained for synchronized use afterward.

### Delta views with an exclusive cursor

`ViewRangeAfter` keeps the same prefix but returns only rows whose next indexed
field is strictly greater than a cursor:

```go
err := messages.ViewRangeAfter(
	"session_seq",
	[]any{sessionID},
	lastSeenSeq,
	func(rows []*Message) error {
		appendToDecodedContext(rows)
		return nil
	},
)
```

For `(SessionID, Seq)`, this uses binary search and locks only rows
with `Seq > lastSeenSeq`. It is intended for generation/high-water protocols
that retain a validated local snapshot and request an ordered delta. It avoids
turning an append-only history into a full transfer on every read.

The prefix must leave at least one index field for the cursor. The cursor must
have exactly the Go type of that next field. If more index fields follow it,
every row with the same next-field value is excluded; use a unique monotonic
cursor such as `Seq` when no row may be skipped.

## Batch reads by ID

```go
err := messages.ViewMany(ids, func(rows []*Message) error {
	return consumeBatch(rows)
})
```

`ViewMany` deduplicates IDs and returns rows in ascending ID order. If any ID is
missing, it returns `nestory.ErrNotFound` and does not invoke the callback with a
partial set.

This is preferable to thousands of detached `Get` calls when the workload only
needs read-only access to flat hot records.

## Batch delete

```go
err := messages.DeleteMany(ids)
```

Duplicate IDs are harmless. All surviving rows are deleted in one logical
transaction and one durable WAL frame, or none are.

To evict an indexed partition:

```go
err := messages.DeleteByIndex("session_seq", []any{sessionID})
```

The prefix is resolved once. Rows added after that snapshot belong to a later
state and are not part of the deletion. Relation rules still apply: deleting
owners cascades, and an outsider `borrow` can veto the whole batch.

## Persistence and unsafe mutations

Secondary indexes are derived in-memory structures. Durable entity rows and WAL
frames are the source of truth; indexes are rebuilt on `Open` and then
maintained with each safe commit.

Large safe transactions update ordered indexes in batches. Removed rows are
filtered in one pass; additions are sorted and merged with the surviving index.
This keeps bulk hydration and eviction from repeatedly shifting the same index
slice. A pure-create batch prepares the ordered slices before taking the short
publication lock, so existing point lookups do not wait for sorting or merging.

Live changes made through `Unsafe` bypass incremental index maintenance.
`Unsafe().Flush` validates uniqueness and rebuilds affected index state before
persisting. A validation error does not roll back the live mutation.
