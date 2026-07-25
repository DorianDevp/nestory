# Unsafe live pointers

The safe API copies mutable branches or holds read locks. `Unsafe` exists for a
different case: one owner has exclusive access, needs the stable live pointer,
and wants to amortize persistence over many ordinary struct mutations.

```go
unsafeUsers := users.Unsafe()

user, err := unsafeUsers.Get(userID)
if err != nil {
	return err
}

user.Name = "Ada"
user.Profile.Theme = "dark"

return unsafeUsers.Flush()
```

`Get` returns the exact pointer stored in Nestory's arena and marks its chunk
dirty. The mutation is visible in memory immediately. There is no snapshot,
change tracking, isolation, retry, or rollback.

## API

```go
unsafe := db.Unsafe()

entity, err := unsafe.Get(id) // live pointer; marks its chunk dirty
all := unsafe.All()           // all live pointers; marks all chunks dirty
unsafe.Create(entity)         // stages create until Flush
err = unsafe.Delete(id)       // stages delete until Flush
err = unsafe.Flush()          // validate, apply lifecycle effects, persist
```

`All` allocates a slice of live pointers. The objects themselves are not copied.

## The contract

The caller must guarantee exclusive access from the first unsafe operation
until `Flush` completes. Do not run safe readers, safe writers, or another
unsafe session concurrently. The API deliberately performs no synchronization
on your behalf.

`Flush` is the boundary at which Nestory:

1. validates the complete relation graph and secondary-index constraints;
2. computes ownership cascades and optional-reference cleanup;
3. applies staged creates and deletes;
4. rewires canonical relation pointers;
5. writes dirty chunk snapshots and compacts WAL state.

The database contract is still enforced. Unsafe means unchecked mutation until
the boundary, not permission to persist an invalid schema.

Step 1 is where the contract cuts both ways. Because you may mutate through a
pointer held since creation, `Flush` cannot be told what changed and does not
try to track it. Instead it compares the live graph against the last committed
state and derives the change set, then publishes a delta when the result is
something the committed indexes can settle. A changed lookup key, or anything
the delta declines, falls back to revalidating and rebuilding the whole graph.
That fallback is correct, not cheap: it is O(project), so a workload that keeps
triggering it will feel it.

## Validation failures

A rejected `Flush` leaves the invalid live mutation in memory. Fix the graph
through the same exclusive pointers and flush again, or discard the process and
recover the last committed state from disk. Do not expose the database to other
goroutines between a failed flush and repair.

Nestory keeps the last committed ownership index separately from the live
pointers. If unsafe code clears or corrupts an ownership field and then deletes
the owner, `Flush` still follows the committed ownership tree and deletes the
children. It does not silently orphan them.

## When to use it

Use `Unsafe` for controlled phases such as:

- loading or transforming a private in-memory working set;
- applying a large batch before one persistence boundary;
- a single-threaded game or simulation loop;
- an application-owned hot cache with explicit quiescence.

Prefer `Transaction`, `UpdateWithin`, or `View` when concurrent access, rollback,
or a narrow consistency boundary matters. Unsafe access is a performance tool,
not the default programming model.
