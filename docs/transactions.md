# Transactions and data access

Nestory exposes several access modes because copying an object graph, holding a
read lock, and exposing a live pointer have different safety and performance
properties.

## API at a glance

| API | Data returned | Write behavior | Best use |
|---|---|---|---|
| `Get` + `Update` | Detached `own` branch | Explicit merge | Edit outside a callback |
| `Transaction` | Detached branches in one context | Automatic commit on nil return | Several related operations |
| `UpdateWithin` | Persistent Tower shadow or detached branch | Automatic commit | Short intent-only update |
| `Tracked().UpdateWithin` | Same shadow, writes declared through `Edit` | Automatic commit | Large branch, few known writes |
| `View` | Live branch under one branch read lock | No writes allowed | Zero-copy aggregate read |
| `ViewMany` / `ViewRange` | Read-locked live rows | No writes allowed | Flat batch read |
| `Unsafe` | Unlocked live pointers | Explicit `Flush` | Exclusive hot loop |

## Detached branches: `Get` and `Update`

```go
user, err := users.Get(id)
if err != nil {
	return err
}

user.Name = "Ada"
user.Profile.Theme = "dark"
return users.Update(user)
```

The branch is a transaction-local copy of the root and its complete ownership
subtree. Think of `Get` as creating a branch and `Update` as merging it. A
pointer returned by `Get` is not the stable pointer held by the live store.

`Update` compares the final branch with its starting snapshot, validates its
relations and indexes, writes the WAL, and publishes the change. Concurrent
changes to the same resources return `nestory.ErrConflict`.

## Callback transactions

`Transaction` creates one context. Every object loaded inside it can be edited
directly; there is deliberately no `tx.Update`.

```go
err := users.Transaction(func(tx *nestory.Tx[User]) error {
	user, err := tx.Get(userID)
	if err != nil {
		return err
	}

	user.Name = "Ada"
	user.Profile.Theme = "dark"

	if err := tx.Create(&anotherUser); err != nil {
		return err
	}

	return tx.Delete(oldUserID)
})
```

Returning a non-nil error discards the complete working set. Returning nil
attempts one commit containing all changed branches, creates, and deletes.

Repeated `tx.Get` calls for the same node return the same transaction-local
pointer. Operation order inside the callback does not define relation validity:
Nestory validates the final graph, so a temporary double owner during a
reparent is acceptable if the final state has exactly one owner.

## Cross-type transactions

Use `Join` to bind another typed DB to the existing context:

```go
err := users.Transaction(func(tx *nestory.Tx[User]) error {
	logs := logDB.Join(tx)

	if err := logs.Delete(logID); err != nil {
		return err
	}

	return tx.Delete(userID)
})
```

The changes are committed as one logical transaction. When more than one type
is touched, recovery uses a single instance-wide transaction WAL frame, so a
crash cannot replay only half of the type set.

Do not start a nested `Transaction`; join the existing context instead.

## Retrying updates

`UpdateWithin` is the short form for one aggregate:

```go
err := users.UpdateWithin(userID, func(user *User) error {
	user.Name = "Grace"
	return nil
})
```

The DB-level method retries after `ErrConflict`. Its callback may therefore run
more than once. Keep it free of external side effects such as sending messages,
charging a card, or appending to an unrelated file. Record intent in the object
graph and perform external effects after the call succeeds.

For an ownership root with children, the DB-level method uses Tower: one
project-wide coordinator holding separate shadow tables wired into a complete
shadow graph. The callback mutates that persistent graph, so repeated updates
do not clone the ownership branch. After the callback, a semantic field diff
selects changed rows and fields; only those rows are materialized for WAL,
index validation, and publication. Relation changes use the normal graph
validator and pointer-canonicalization path.

Tower serializes writable callbacks per **ownership branch**, not per project.
Two roots that share no top-level owner have disjoint branches, so their
callbacks run side by side. Nested roots, meaning a node and one of its own
ancestors, share a top-level owner and queue behind each other. It does not hold the
canonical graph lock while the callback runs, so `View` continues to read the
previous committed state.

Each participating table carries an epoch. A commit outside Tower bumps the
epoch of the table it touched, which retires the shadow; the next Tower update
refreshes it before retrying the callback. Refreshing copies only the nodes
whose values actually moved and absorbs newly created ones into fresh storage
blocks, so a shadow that is still mostly accurate is not rebuilt from scratch.
Writing to a table outside the relation graph never retires it.

The warm diff is still O(branch size). Ordinary `*T` writes have no setter or
proxy through which Nestory could record the changed address, so finding an
arbitrary changed descendant cannot be guaranteed in O(1). Tower removes the
full branch clone, per-row transaction registration, and unchanged-row
publication; it does not claim constant-time automatic change discovery.

## Declaring writes: `Tracked`

When a callback knows which nodes it touches, it can say so and skip the branch
diff entirely:

```go
err := workspaces.Tracked().UpdateWithin(id, func(w *nestory.Writes, ws *Workspace) error {
	nestory.Edit(w, ws.Projects[0]).Name = "renamed"
	return nil
})
```

`Edit` returns the node it was given, so it reads as a wrapper around the write
rather than a separate bookkeeping call. It rejects a node outside the branch
being updated. The commit then diffs only the declared nodes plus the root,
which turns an O(branch) comparison into O(declared). That is worth it from
roughly a hundred owned children upward, and no faster below that.

An undeclared write is a silent lost update, so there is an opt-in check:

```go
nestory.AuditTrackedWrites = true // re-runs the full branch diff and returns
                                  // ErrUndeclaredWrite if anything else moved
```

Enable it in tests. It costs exactly what declaring the write saves, so leaving
it on in production defeats the purpose.

Inside an existing transaction, `tx.UpdateWithin` only loads and mutates the
object in that context; it neither commits independently nor adds another retry
loop.

## Read-only views

`View` exposes the stable live root and read-locks its ownership subtree for the
duration of the callback:

```go
err := users.View(userID, func(user *User) error {
	return encode(user)
})
```

Go cannot express `const *T`. Mutating a view pointer is a caller error, and the
pointer must not be retained for synchronized use after the callback. `borrow`
and `option` targets outside the owned subtree remain navigable but do not share
the same per-resource consistency window.

`ViewMany` locks deduplicated IDs in ascending order. `ViewRange` resolves an
ordered index prefix and locks the returned rows. Both use the same callback-
only, read-only pointer contract. See [Indexes and batches](indexes-and-batches.md).

## Conflict and publication model

Safe commits use optimistic concurrency control:

1. Work happens on detached structs outside live resource locks.
2. Nestory calculates the changed resources and relation/index delta.
3. It locks structural state and resources in deterministic order.
4. Resource versions are checked. A mismatch returns `ErrConflict`.
5. Index and relation invariants are validated against the commit's final state.
6. The durable WAL frame is fsynced.
7. The in-memory state, indexes, and relation pointers are published.

Pure scalar updates on flat, relationless types use a shorter path and do not
take the global relation-graph lock. Structural graph commits serialize graph
publication to keep validation and publication in one consistency window.
