# Transactions and data access

Nestory exposes several access modes because copying an object graph, holding a
read lock, and exposing a live pointer have different safety and performance
properties.

## API at a glance

| API | Data returned | Write behavior | Best use |
|---|---|---|---|
| `Get` + `Update` | Detached `own` branch | Explicit merge | Edit outside a callback |
| `Transaction` | Detached branches in one context | Automatic commit on nil return | Several related operations |
| `UpdateWithin` | Detached branch in a retrying context | Automatic commit | Short intent-only update |
| `View` | Read-locked live branch | No writes allowed | Zero-copy aggregate read |
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
