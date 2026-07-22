# Relations and lifecycle

Nestory stores one object graph. Relations are real Go pointers in memory and
matching keys on disk. The relation grammar describes lifecycle, while the Go
field type describes cardinality.

```go
type User struct {
	Id    int
	Posts []*Post `rel:"own,User"`
}

type Post struct {
	Id   int
	User *User `rel:"ownedby,Id"`
}
```

Register every related type before opening any of them. Schema rules are
checked during `Register`; instance rules and delete effects are checked against
the final state of every safe commit and unsafe `Flush`.

## Why relations are lifetime contracts

An ORM sits between two independently designed representations: the object
model and the database schema. Mapping values is only the obvious part of its
job. It must also translate what each representation means. Which side stores
the association? Is the foreign key nullable? Does deleting one row cascade,
set null, remove an orphan, or fail? Which constraints exist only in the
database?

A fully explicit ORM mapping answers those questions with separate options for
nullability, cascade behavior, owning side, foreign keys, and constraints. The
result is accurate but verbose. A shorter mapping has the opposite problem: it
leaves part of the contract in conventions or in a schema that must be
inspected separately. With real pointers this gap is especially dangerous,
because an object can still point at a resource whose database lifetime was
described somewhere else.

Nestory does not translate an object model into an external database model. It
is both the object representation and the database, so it can choose one
coherent contract instead of reproducing a collection of SQL and ORM switches.
The Go field already states cardinality. A relation tag therefore answers the
questions that the field type cannot answer:

1. May this reference disappear while the holder remains alive?
2. Who controls the lifetime of the referenced value?

This is why Nestory relations describe **how values may exist over time**, not
how a join should be configured.

## The mental model

The vocabulary is inspired by Rust's explicit ownership and borrowing model.
Nestory does not implement Rust semantics or a compile-time borrow checker; Go
cannot prove these rules statically. It adopts the useful part of that model as
a runtime database contract:

- `own` says that the target belongs to the holder's lifetime. The holder may
  replace it, and deleting the holder deletes what it owns.
- `borrow` says that the holder needs somebody else's value to stay alive. A
  surviving borrower therefore prevents that value from being deleted.
- `option` says that the reference makes no lifetime promise. The target may
  disappear, in which case the reference becomes nil or drops from the slice.
- `ownedby` names the same ownership edge from the owned value when backward
  navigation is useful.
- `inverse` is only a computed navigation view. It derives from `borrow` or
  `option` and introduces no new lifetime rule.

The role combines facts that would otherwise be spread across nullable,
cascade, orphan-removal, and association settings. In particular, Nestory's
`own` is stronger than an ORM's technical “owning side”: it is authority over a
resource's lifecycle.

For a to-one field, `own`, `ownedby`, and `borrow` are required contracts. The
pointer cannot be nil while its holder survives. `option` is the explicit
nullable contract. The same lifecycle vocabulary extends to collections, where
an empty slice remains valid.

Nestory validates the model at the boundaries where it can know the complete
truth: structural rules at `Register`, and value lifetimes against the final
state of a commit or unsafe `Flush`.

## The ownership forest is a consequence

If a value has at most one owner and ownership cannot cycle, all `own` and
`ownedby` edges necessarily form a forest. This is not a separate project layer
and users do not build a “forest graph” beside a “reference graph.” It is the
mathematical shape produced by the ownership contract.

That shape gives cascade deletion a precise meaning: deleting an owner deletes
its complete ownership subtree, with no shared owned node, double delete,
or orphan sweep. References that do not own values may freely cross ownership
boundaries, be shared, point in either direction, and form cycles. Their role
still determines the outcome: `borrow` vetoes deletion while its holder
survives, and `option` yields to deletion.

## Roles

Cardinality comes from the field shape: `*T` is to-one and `[]*T` is to-many.
The tag is `rel:"role,field"`.

| Role | Meaning |
|---|---|
| `own` | The holder owns the target. A surviving to-one `own` is mandatory. Deleting the holder cascades to the target or list. |
| `ownedby` | The same ownership edge viewed from the child. It is a mandatory `*T`; the child cannot survive its owner. |
| `borrow` | The holder does not own the target, but vetoes deletion while the holder survives. A to-one borrow is mandatory. |
| `option` | The target may disappear. A surviving pointer becomes nil, and a slice drops deleted targets. |
| `inverse` | A computed `[]*T` view of holders pointing here through `borrow` or `option`. It is not authoritative storage. |

There is no neutral pointer to a registered entity type. Every relation pointer
must state what happens when its target dies.

## `own` and `ownedby` are one edge

These tags are the two ends of one ownership edge, not two independent
relations. The complement is only declared when the other side needs a field.

One-sided to-one ownership:

```go
type User struct {
	Id      int
	Profile *Profile `rel:"own,Id"`
}
```

Two-sided to-one ownership:

```go
type User struct {
	Id      int
	Profile *Profile `rel:"own,Id"`
}

type Profile struct {
	Id   int
	User *User `rel:"ownedby,Id"`
}
```

To-many ownership names the child's back-reference field:

```go
type User struct {
	Id    int
	Posts []*Post `rel:"own,User"`
}

type Post struct {
	Id   int
	User *User `rel:"ownedby,Id"`
}
```

When both ends are declared, both are mandatory and must agree in the final
state. Moving a post from one user to another means updating both users' slices
and `post.User` in the same transaction. The engine canonicalizes agreeing
pointers; it does not invent a missing side.

For a to-many `own`, `field` names a field on the child. It may be a raw integer
key or the child's `ownedby` pointer. For a to-one relation, `field` names the
target field to match, normally `Id`.

## Delete behavior

The final-state rules are exact:

| Relation | Delete target | Delete holder |
|---|---|---|
| to-one `own` | Forbidden by itself. Swap the target or delete the owner. | Cascades to the target. |
| to-many `own` | Allowed; the owner's slice shrinks. | Cascades to every child. |
| `ownedby` | Deleting the owner cascades to the child. | The reverse owner field shrinks; a to-one owner cannot remain without a replacement. |
| `borrow` | Blocked while the holder survives. | Target is unaffected. |
| `option` | Pointer becomes nil or slice element is removed. | Target is unaffected. |
| `inverse` | Computed from the forward edge. | Behavior follows the holder's `borrow` or `option`. |

Deleting an owner always includes every descendant in the delete set. Unsafe
code cannot evade this by clearing one side before `Flush`; the committed
ownership index remains the basis of the cascade.

A surviving outsider that borrows any node in the doomed subtree vetoes the
whole delete. A borrower inside the same doomed subtree does not veto, because
it will not survive. Optional references from outsiders are cleared.

## Complete field spelling space

The following table is the schema contract covered by `relations_test.go`.

| # | Spelling | Verdict | Failure condition |
|---:|---|---|---|
| 1 | `P *T`, no tag | error | A pointer to a registered type has no lifecycle role. |
| 2 | `P []*T`, no tag | error | A pointer slice is neither authoritative `own` nor computed `inverse`. |
| 3 | `FK int` | legal | A raw scalar key has no lifecycle semantics on its own. Zero is allowed. |
| 4 | `P *T rel:"own,Id"` | legal | Register rejects self-type or missing target field. Commit rejects nil on a survivor, isolated target delete, a second owner, or an ownership cycle. |
| 5 | `P []*T rel:"own,F"` | legal, including self-type | Register rejects missing `F`. Commit rejects duplicate membership, a second owner, or adopting an ancestor. |
| 6 | `P *T rel:"ownedby,Id"` | legal | Register rejects a second `ownedby`, self-type, or mandatory type cycle. Commit rejects nil or disagreement with the owner side. |
| 7 | `P []*T rel:"ownedby,..."` | error | It would describe many owners for one node. |
| 8 | `P *T rel:"borrow,Id"` | legal | Commit rejects nil on a survivor. Self-borrow is legal. |
| 9 | `P []*T rel:"borrow,Id"` | legal | Empty is allowed; each surviving element vetoes its target's deletion. |
| 10 | `P *T rel:"option,Id"` | legal | Nullable; target deletion sets it to nil. |
| 11 | `P []*T rel:"option,Id"` | legal | Deleted targets are removed from the slice. |
| 12 | `P *T rel:"inverse,F"` | error | A computed mirror is always a collection. |
| 13 | `P []*T rel:"inverse,F"` | conditional | `F` must exist on the element type, carry `borrow` or `option`, and target the inverse holder's type. |
| 14 | `rel:"..."` on a non-pointer field | error | Embedded values and scalar keys are not relation fields. |

`inverse` never mirrors `own`: the named complement of `own` is `ownedby`.

## Final-state commit semantics

For every commit Nestory computes:

- `D`: explicitly deleted nodes plus their transitive ownership descendants;
- `S`: every node that survives.

It then validates the final state:

- no survivor may `borrow` a node in `D`;
- survivor `option` references into `D` become nil or lose the element;
- survivor to-one `own`, `ownedby`, and `borrow` fields are non-nil;
- each node has at most one owner;
- ownership is acyclic;
- both declared ends of an ownership edge agree.

Because validation sees the final state, operation order inside a transaction
does not matter. Reparenting may temporarily expose two owners in the callback;
deleting a borrower and target in either order is valid if neither survives.

## Invariants

Schema invariants are checked once at registration:

- **I1 — one role per field.** A field declares one relation role.
- **I2 — valid shape.** `ownedby` is `*T`; `inverse` is `[]*T` and names a
  compatible `borrow` or `option`; other roles allow pointer or pointer slice.
- **I3 — satisfiable schema.** Mandatory ownership declarations must admit a
  finite instance graph. A type may have at most one `ownedby`; mandatory
  `ownedby` cycles and to-one `own` cycles are rejected.

Instance invariants are checked at commit:

- **I4 — at most one owner.** The two declared sides of the same edge count as
  one. Duplicate appearance in an `own` slice still counts as two edges and is
  rejected.
- **I5 — acyclic ownership.** Self-referential types can form trees through
  `own []*T`, but a concrete node cannot own itself or an ancestor.
- **I6 — pair consistency.** A declared `ownedby` pointer equals the owner
  derived from the reverse ownership index.

Whether a node may be unowned depends on the child type. A type declaring
`ownedby` must always have an owner. A type without `ownedby` may be a root or an
unowned row.

### Satisfiability examples

| Schema | Verdict | Reason |
|---|---|---|
| `A{B *B own}` and `B{A *A own}` | error | Every owner demands another fresh child; no finite instance exists. |
| `Node{Next *Node own}` | error | Mandatory self descent never terminates. |
| `Node{Parent *Node ownedby}` | error | Every node demands a parent, so no root can exist. |
| two `ownedby` fields in one type | error | Every instance would demand two owners. |
| `Node{Children []*Node own}` | legal | An empty slice terminates the tree. |
| `A{Buddy *A borrow}` | legal | A self-borrow or mutually pinning pair is satisfiable. |
| any `option` layout | legal | Optional references demand no target. |

## Composition rules

Ownership alone must remain a forest. Non-owning references may otherwise
compose freely.

| Combination | Result |
|---|---|
| two owning edges to one target | Rejected by I4, even if both come from the same owner. |
| mutual ownership or any ownership cycle | Rejected by I5. Use a shared owner or embedding for co-death. |
| `own` plus its reverse `ownedby` | One legal two-sided edge, checked by I6. |
| owner borrows its child | Protected member: child cannot be deleted alone, but still dies with the owner. |
| owner optionally points to its child | Distinguished member: child may be deleted; the option is then nil. |
| child borrows its owner | Legal but normally inert while the child remains in that owner's cascade. |
| mutual borrow | Atomic co-death group: neither dies alone, both may die in one transaction. |
| borrow cycle of any length | Legal; every survivor still vetoes deletion of its target. |
| option cycle | Legal; deletions clear survivor references. |
| borrow or option self-loop | Legal and inert when holder and target are the same deleted node. |
| outsider borrows an owned descendant | Deleting either the descendant or any owner above it is vetoed. |
| outsider optionally references a descendant | Cascade succeeds and clears the outsider's reference. |

Incoming-reference counts treat a self-loop as one edge. Ordered pointer slices
retain their declared order during incremental rewiring; inverse collections do
not gain duplicate entries from a rewire.

## Common patterns

### Top-down trees

A self `ownedby` parent pointer is unsatisfiable because it requires even the
root to have a parent. Model the authoritative tree downward and keep upward
navigation as a scalar key:

```go
type Tree struct {
	Id    int
	Roots []*Node `rel:"own,TreeId"`
}

type Node struct {
	Id       int
	TreeId   int
	ParentId int
	Children []*Node `rel:"own,ParentId"`
}
```

A node is owned either by `Tree.Roots` or a parent's `Children`, never both.
Deleting a node prunes its subtree and shrinks the authoritative slice.

### Protected and distinguished children

```go
type Playlist struct {
	Id      int
	Entries []*Entry `rel:"own,Playlist"`
	Current *Entry   `rel:"borrow,Id"`
	LastHit *Entry   `rel:"option,Id"`
}

type Entry struct {
	Id       int
	Playlist *Playlist `rel:"ownedby,Id"`
}
```

`Current` prevents accidental deletion of that entry until repointed.
`LastHit` allows deletion and becomes nil. Deleting the playlist deletes all
entries because its own borrow dies inside the same cascade.

### Borrow with inverse

```go
type Loan struct {
	Id   int
	Book *Book `rel:"borrow,Id"`
}

type Book struct {
	Id    int
	Loans []*Loan `rel:"inverse,Book"`
}
```

`Book.Loans` is rebuilt from `Loan.Book`. A book cannot be deleted while a loan
survives.

### Optional audit reference

```go
type Log struct {
	Id   int
	User *User `rel:"option,Id"`
}

type User struct {
	Id   int
	Logs []*Log `rel:"inverse,User"`
}
```

Deleting a user clears `Log.User`; the log survives and drops out of the user's
computed inverse view.

### Many-to-many junction

Use a junction entity with two borrows when the link is independently managed:

```go
type PlaylistEntry struct {
	Id       int
	Playlist *Playlist `rel:"borrow,Id"`
	Song     *Song     `rel:"borrow,Id"`
}
```

If the link belongs to the playlist but only references the song:

```go
type PlaylistEntry struct {
	Id       int
	Playlist *Playlist `rel:"ownedby,Id"`
	Song     *Song     `rel:"borrow,Id"`
}
```

One entity cannot have two owners, so a SQL-style junction that cascades from
both sides is deliberately not representable. Delete such independent links
explicitly in the same transaction.

## Embed or relate?

Embed a value when it has no identity of its own:

```go
type Account struct {
	Balance int
}

type User struct {
	Id      int
	Account Account
}
```

Embedding provides exclusive lifetime, one stored row, and direct field access
without a separate ID, store, relation match, or cascade rule. Use a relation
only when the nested object must be addressed independently, referenced from
elsewhere, or queried by its own identity.

Relations inside embedded values are not currently wired. Embedded values
should therefore be leaf data.
