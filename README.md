# nestory

Persistent in-memory object graph for Go. The whole dataset lives in RAM as
real structs; relations are real `*T` pointers, rewired from disk on `Open`.
No queries, no joins — you walk the graph.

```go
type User struct {
    Id      int      `key:"primary"`
    Name    string
    Profile *Profile `rel:"own,Id"`
    Posts   []*Post  `rel:"own,User"`
}

// register every type in the graph before Open
nestory.Register[Settings]()
nestory.Register[Profile]()
nestory.Register[Post]()
nestory.Register[User]()

db := nestory.Open[User]()
u, _ := db.FindOneBy("Id", 1)

u.Profile.Settings.Theme // already wired, no second lookup
u.Posts[0].Title         // slice already filled
```

Writes go through an optimistic transaction:

```go
// snapshot -> mutate a detached copy -> commit
db.UpdateWithin(1, func(u *User) { u.Name = "bob" })
```

First commit wins; a concurrent one gets `ErrConflict` and a refreshed snapshot.

For exclusive, latency-sensitive work, `UnsafeGet` exposes the stable pointer in
the live store without a snapshot or transaction bookkeeping:

```go
u, _ := db.UnsafeGet(1)
u.Name = "bob" // visible immediately
err := db.Flush()
```

`UnsafeGet` marks the containing chunk dirty, so `Flush` persists direct
mutations and validates the final relation graph. It deliberately provides no
isolation, locking or rollback: the caller must guarantee exclusive access, and
a rejected `Flush` leaves the invalid live mutation in memory until it is fixed.
Nestory keeps the last committed ownership index separately from those live
pointers. Consequently, deleting an owner at `Flush` still cascades through its
committed subtree even if unsafe mutations have already damaged the in-memory
description of an ownership edge.

## How it works

- Entities live in a chunked arena (fixed 512-row blocks, never moved), so a
  `*T` you hold never dangles when the dataset grows. That's what makes the
  pointer graph safe. This might look like append-only DS, but snapshot + hard delete
  solves the problem
- Each type persists as a directory of per-chunk gob snapshots
  (`<Type>/<n>.gob`). Only changed chunks are rewritten, in parallel.
- Transactional commits are durable via a per-type write-ahead log: append one
  fsynced frame, then mutate memory. `Open` replays the log over the snapshots;
  `Flush` compacts the chunks and truncates the log.

## Architecture: the ownership forest

The dataset is one object graph — any entity can reach any other through a real
`*T`. The organizing idea is that this graph **factors into two layers**:

- An **ownership forest** — the `own`/`ownedby` edges. Every entity has at most
  one owner and there are no cycles, so these edges form a forest of trees. This
  is the lifecycle backbone: it answers "what dies when this dies." Being a forest
  is precisely what lets a cascade delete be a plain walk down a subtree — no
  shared node, no double-free, no bookkeeping.
- A **free reference graph** laid over it — the `borrow`/`option`/`inverse` edges.
  They own nothing, so they're unconstrained: shared, any direction, cyclic — a
  borrow cycle is just a group of nodes deletable only together. They navigate
  the data; they don't own it.

Pinning lifecycle to a strict forest is also why there's no orphan sweep: an
entity's lifetime is structural — its place in the forest — not something you
discover by counting or chasing references. A reference that outlives its target
is resolved by its role: `option` goes nil, `borrow` blocks the delete.

## Ownership & cascade

A relation has two independent axes. **Cardinality** comes from the Go type —
`*T` is to-one, `[]*T` is to-many — so you never tag it. **Lifecycle** — who owns
whom, who must outlive whom, and what a delete does — is what the tag declares,
written `rel:"role,field"`. The role names are Rust-inspired *lifetime
descriptors* (not Rust semantics — there's no compile-time borrow checker here).

The five roles are not five peers. Three of them — `own`, `borrow`, `option` — sit
on one axis: **how much power you hold over the target's lifetime**, full / veto /
none. `ownedby` is `own`'s *named complement* — the same edge seen from the owned
side. `inverse` is a *computed* mirror for the two bonds that have no named
complement (`borrow`, `option`); it is never stored and never authoritative.

- `own`     — full power: I create and destroy the target (to-one or to-many).
- `ownedby` — `own` from the owned side: I die no later than my owner.
- `borrow`  — veto: I don't own it, but it may not be deleted while I hold it.
- `option`    — no power: it may vanish; my reference just goes nil.
- `inverse` — computed `[]*T` view of everyone pointing at me via `borrow`/`option`.

A `*T` field always declares its own bond — there is no neutral pointer, because
a pointer must say what happens when its target dies. A `[]*T` is either an
authoritative `own` (I own this list) or a computed `inverse` (filled from the
holders' FKs on load).

### The own ↔ ownedby symbiosis

`own` and `ownedby` are the two ends of **one** owning edge, not two edges. The
tag is required on one side; the complement appears only when the other side
needs a field of its own. Every edge shape has exactly one spelling:

```go
// to-one, one-sided — the owner holds the only field:
type User struct {
    Id      int      `key:"primary"`
    Profile *Profile `rel:"own,Id"`
}

// to-one, two-sided — the SAME edge from the other end, no inverse involved:
type Profile struct {
    Id   int   `key:"primary"`
    User *User `rel:"ownedby,Id"`
}

// to-many — own []*T owns the LIST, so the only possible complement is ownedby.
// The second arg names the child's back-reference field: a raw int FK, or the
// child's ownedby pointer when the edge is two-sided.
type Post struct {
    Id   int   `key:"primary"`
    User *User `rel:"ownedby,Id"`
}
type User struct {
    Id    int     `key:"primary"`
    Posts []*Post `rel:"own,User"`
}
```

When both ends are tagged, consistency is checked **at commit** (invariant I6):
the `ownedby` pointer must equal the owner from the reverse index. A transaction
that pushed `p` into `u1.Posts` but left `p.User == u2` is contradictory. Both
declared fields are mandatory in the final state: the engine canonicalizes their
pointers but never invents a missing side. Moving a two-sided edge therefore
requires updating both fields before commit.

Declaring `ownedby` is a commitment: every instance of the type must then have
an owner, always — nil is an error, and there is no "root" special case (see the
tree pattern below for what to do instead). A type may declare at most one
`ownedby` field: two non-nullable owners would violate ≤1-owner on every single
instance, so the schema is rejected at `Register` (I3).

FK storage follows the child: when an `ownedby` field exists the FK flattens
there (like an RDBMS) and the owner's pointer/slice is rehydrated from the
reverse index; a lone to-one `own` flattens the FK on the owner — there is no
child field to put it on.

### Tag signatures — every one, with its side effects

`field` means two things depending on cardinality. On a to-one bond
(`own`/`ownedby`/`borrow`/`option`) it names the **target field to match**
(usually `Id`). On a `[]*T` (`own` to-many, `inverse`) it names the **field on
the element type** that points back.

| Tag | Field | Delete target → | Delete holder → | Nullable |
|-----|-------|-----------------|-----------------|----------|
| `rel:"own,Id"` (to-one) | `*T` | forbidden alone — delete me or swap | deletes the target | no (nil = error at commit) |
| `rel:"own,F"` (to-many) | `[]*T` | child delete allowed; the slice shrinks | deletes every child | — (slice) |
| `rel:"ownedby,Id"` | `*T` only | deletes me | owner's `own []*T` shrinks; under a to-one `own` forbidden alone — delete the owner or swap | no |
| `rel:"borrow,Id"` | `*T` / `[]*T` | **blocked** while I hold it (checked at commit) | target unaffected | no |
| `rel:"option,Id"` | `*T` / `[]*T` | my pointer → nil (slice: element dropped) | target unaffected | yes |
| `rel:"inverse,F"` | `[]*T` only | element deleted → drops out of the view | per `F`'s bond: `borrow` elements block my delete, `option` elements go nil | — (computed) |

Notes that bite:

- **`[]*T` + `ownedby` is an error.** `ownedby` means "the target owns me"; a
  slice would mean many owners of one node, which breaks ≤1-owner. `ownedby` is
  always to-one.
- **`inverse` is `[]*T` only, and `F` must carry `borrow` or `option`.** An
  `inverse` naming an `own`/`ownedby` field is a schema error — the mirror of
  `own` already has a name: `ownedby`. `F`'s own target type must be the type
  declaring the `inverse` (`User.Logs rel:"inverse,User"` requires `Log.User`
  to be a `*User`). And a `*T` can never be a mirror: a raw `*Parent` with no
  role is a schema error, because a pointer must declare what happens when its
  target dies.
- **`own` + `ownedby` on the same edge is not a duplicate.** It's the two-sided
  spelling of one edge, kept consistent at commit (I6). What *is* an error is a
  second owner (I4) or mutual ownership (I5) — almost everything else composes;
  see Compositions below.
- **Cascade and veto interact both ways.** Deleting an owner is **blocked** if
  any survivor borrows anything in the doomed subtree — the error names the
  pinned node and its borrower. Conversely, borrows held *inside* the doomed
  subtree die with it and veto nothing; that second half is why deleting a
  playlist together with its borrowing entries just works.

The complete spelling space — every variant a field can take, and when it
errors:

| # | Spelling | Verdict | When it errors |
|---|----------|---------|----------------|
| 1 | `P *T`, no tag | **error** | Register: a pointer to a registered type must declare its bond — there is no neutral pointer. |
| 2 | `P []*T`, no tag | **error** | Register: a slice is either an authoritative `own` or a computed `inverse`. |
| 3 | `Fk int` (raw FK) | legal | Never. Zero allowed; carries no semantics of its own — meaning comes from an `own,Fk` on the parent side, if any. |
| 4 | `P *T rel:"own,Id"` | legal | Commit: nil on a survivor; deleting the target without a swap or without deleting me; target gaining a second owning edge (I4). Register: self-type (I3); `Id` missing on the target. |
| 5 | `P []*T rel:"own,F"` | legal (self-type too) | Register: `F` missing on the element type. Commit: duplicate element (I4); element already owned elsewhere (I4); adopting your own ancestor (I5). |
| 6 | `P *T rel:"ownedby,Id"` | legal | Commit: nil (ownership is mandatory — no root exception); disagreeing with the reverse index when both sides were mutated (I6). Register: a second `ownedby` in the type, self-type, type cycle (I3). |
| 7 | `P []*T rel:"ownedby,…"` | **error** | Register (I2): would mean many owners of one node. |
| 8 | `P *T rel:"borrow,Id"` | legal | Commit: nil on a survivor. Self-type is satisfiable (mutual pairs, self-borrow). |
| 9 | `P []*T rel:"borrow,Id"` | legal | Never at field level; an empty slice is fine. |
| 10 | `P *T rel:"option,Id"` | legal | Never. The one role that cannot block a commit. |
| 11 | `P []*T rel:"option,Id"` | legal | Never; elements drop when their target dies. |
| 12 | `P *T rel:"inverse,F"` | **error** | Register (I2): a mirror is a collection; a backward `*T` must declare its own bond. |
| 13 | `P []*T rel:"inverse,F"` | conditional | Register: `F` missing; `F` carries `own`/`ownedby` (the mirror of own is named `ownedby`); `F` has no role; `F` targets a type other than mine. |
| 14 | `rel:"…"` on a non-pointer field | **error** | Register: roles go on `*T`/`[]*T` only; an embedded value is embedding, not a relation. |

### The lifetime model: static order, dynamic veto

Ownership is the only *static* lifetime claim. The `own`/`ownedby` edges form a
forest (I4, I5), and the forest **is** the order: a node provably dies no later
than every one of its ancestors, and a cascade is nothing but a walk down that
order. Both spellings state the same edge — `ownedby` is `own` read from below.

`borrow` makes no static claim. It is an *operational veto, scoped to the edge*:
the target cannot be deleted while a **surviving** holder points at it. The veto
ends when the holder repoints, when the slice element is removed, or when the
holder itself dies — in particular, a holder that dies in the same commit vetoes
nothing. `option` claims nothing in either direction; `inverse` is a computed view.

Because the veto is operational, borrow edges may form cycles, and a cycle is
not a contradiction — it is a *co-death group*, nodes deletable only together
(or after repointing). Two entries that mutually borrow each other are an
**atomic pair**: think a debit and the credit it balances, removable only in
one transaction. Soundness never depends on borrow topology: the cascade
closure walks only ownership edges (terminates by I5), and the veto/setnull
checks compare pointwise against the transaction's final state.

### Invariants

The commit semantics come first; every check is defined against them. A commit
computes `D` = the explicitly deleted nodes plus everything they transitively
own (the *cascade closure* — it walks only ownership edges), `S` = everything
else (the *survivors*), and then validates the **final state**: no survivor may
borrow a node in `D`, survivors' `option` pointers into `D` go nil, and every
survivor's `own`/`ownedby`/`borrow` is non-nil. Deferring everything to commit
means operation order *inside* a transaction never matters: moving a subtree
briefly shows two owners and is fine; deleting a playlist together with the
entries that borrow it passes in any order, because at commit no live borrower
remains.

Three invariants are schema properties, checked once at `Register`:

- **I1 — one role per field.** A field declares exactly one role.
- **I2 — shape.** `ownedby` is `*T` only. `inverse` is `[]*T` only, and its `F`
  must name a `borrow`/`option` field on the element type whose target is the
  declaring type. `own`/`borrow`/`option` go either way.
- **I3 — satisfiability.** A schema that can never have instances is rejected
  outright. Declaring `ownedby` makes ownership *mandatory*, so: at most one
  `ownedby` field per type (two would demand two owners on every instance,
  against I4), no cycle of `ownedby` edges in the type graph — including the
  self-loop `Node{ Parent *Node rel:"ownedby,Id" }`, where every node demands a
  parent and no root can ever exist — and no cycle of to-one `own` edges, where
  every owner demands a fresh owned node (fresh by I4) and the descent never
  terminates.

I3 in full — what `Register` rejects as unsatisfiable, and what it lets through:

| Schema | Verdict | Why |
|--------|---------|-----|
| `A{ B *B own }` + `B{ A *A own }` | **error** | Every owner demands a fresh child (fresh by I4) → infinite descent; no instance can ever exist. |
| `Node{ Next *Node rel:"own,Id" }` | **error** | The same, as a self-loop. |
| `Node{ Parent *Node rel:"ownedby,Id" }` | **error** | Every node demands a parent → no root can exist. |
| an `ownedby` cycle across several types | **error** | The same, by a longer path. |
| two `ownedby` fields in one type | **error** | Two mandatory owners on every instance — unsatisfiable under I4. |
| `Node{ Children []*Node own }` | legal | The empty slice stops the regress — the canonical tree. |
| `A{ Buddy *A rel:"borrow,Id" }` | legal | Satisfiable: mutually pinning pairs, or a self-borrow. |
| mixed own/borrow type cycles | legal | Satisfying instance layouts exist, so the commit decides, not the schema. |
| any `option` layout | legal | Nullable demands nothing. |

Three are instance properties, checked at the end of every transaction:

- **I4 — ≤1 owner.** Each node is the target of at most one owning edge (`own`
  from the owner and `ownedby` from the owned are ONE edge, not two), counting
  duplicates — the same child appearing twice in one `own []*T` is an error.
  Checked via the reverse-index edge count, so ownership can be handed over
  mid-transaction.
- **I5 — ownership is acyclic, per instance.** Self-referential *types* are
  legal — `Node{ Children []*Node rel:"own,ParentId" }` is a tree, the canonical
  case. What is forbidden is an instance cycle: adopting `r` under `a` while `r`
  is an ancestor of `a`. The check is one walk up the owner chain from the
  adopter — a single chain, because I4 — and meeting the adoptee means a cycle;
  O(tree depth). With I4 this makes the ownership graph a **forest**: cascade is
  a plain subtree walk and the closure always terminates.
- **I6 — pair consistency.** An `ownedby` pointer must equal the owner recorded
  by the reverse index. Both declared fields must be non-nil and agree in the
  final state; the engine canonicalizes pointers but never guesses a missing
  complement.

Whether a node may live unowned is the **child's declaration, not the owner's**:
a type with an `ownedby` field must always have an owner (I3's mandatoriness);
a type without one may sit outside any owning edge — a `Node` with
`ParentId == 0` is just an unowned row, not an error and not a special case.

One practical note on `option`: ids are never reused (the auto-id counter is
seeded from disk), so a nil'd option pointer can never silently rebind to a
stranger that inherited the id.

### Compositions — combining roles

Mechanisms compose; the engine bans almost nothing statically. The full pair
matrix — every way two edges can meet between nodes A and B. `own→` reads
"A owns B", `bor←` reads "B borrows A":

| # | Pair | Verdict | Description |
|---|------|---------|-------------|
| 1 | `own→` + `own→` | **error I4** (commit) | A second owning edge onto the same target — even from the same owner (B twice in one slice, or in two fields of A). |
| 2 | `own→` + `own←` | **error I5** (commit) | Mutual ownership: the owner walk never terminates, the forest loses its root. Spell co-death with a shared owner or embedding. |
| 3 | `own→` + `ownedby←` | legal | Not a pair — one edge spelled from both ends; I6 keeps the two ends equal. |
| 4 | `own→` + `bor→` | legal | **Protected member**: the child can't be deleted by accident while the owner lives, yet dies with the owner — the veto dies with its holder. `Playlist.Current`. |
| 5 | `own→` + `bor←` | legal, inert | The owned pins its owner: every cascade reaching A collects B, so B never survives to veto. Wakes if B is reparented out. "My owner" is spelled `ownedby`. |
| 6 | `own→` + `option→` | legal | **Distinguished member**: the owner singles out its own child; deleting the child shrinks the slice and nils the pointer. `Playlist.LastHit`. |
| 7 | `own→` + `option←` | legal, inert | As #5: the setnull is never observable while B sits in A's subtree. |
| 8 | `bor→` + `bor→` | legal | A duplicate veto, idempotent. |
| 9 | `bor→` + `bor←` | legal | **Atomic pair**: neither dies alone (the survivor vetoes), both die in one transaction (both in `D`, vetoes vacuous). A debit and its credit. |
| 10 | `bor→` + `option→` | legal | The option is dominated: while the borrow holds, the target can't die, so the nil never happens; wakes after the borrow repoints. |
| 11 | `bor→` + `option←` | legal | Both fire cleanly: deleting A nils B's option; deleting B is vetoed by A. |
| 12 | `option→` + `option→` | legal | Two views, nothing more. |
| 13 | `option→` + `option←` | legal | Mutual acquaintance; the free layer is legally cyclic. |
| 14 | `own` self-loop | **error I5** (commit) | A cycle of length one — the walk meets the adoptee immediately. |
| 15 | `borrow`/`option` self-loop | legal, inert | The holder is in `D` whenever the target is, so veto/setnull are vacuous. A self-borrow even has a use: the bottom-up tree root, below. |

The distinguished and the protected member, on one playlist:

```go
type Entry struct {
    Id       int       `key:"primary"`
    Playlist *Playlist `rel:"ownedby,Id"`
}

type Playlist struct {
    Id      int      `key:"primary"`
    Entries []*Entry `rel:"own,Playlist"`
    Current *Entry   `rel:"borrow,Id"` // protected: no accidental delete
    LastHit *Entry   `rel:"option,Id"`   // distinguished: may vanish, then nils
}
```

Deleting the entry `Current` points at is vetoed — repoint `Current` first.
Deleting one that only `LastHit` points at shrinks `Entries` and nils `LastHit`.
Deleting the playlist collects everything, vetoes included. And note what the
schema now *states*: a non-nil `Current` means a playlist can never go below
one entry — that business rule lives in the type, not in application code.

Beyond one pair:

| Configuration | Verdict | Description |
|---------------|---------|-------------|
| `own` chain A→B→C | legal | A tree; `delete(B)` collects C and shrinks A's slice. |
| `own` cycle of any length | **error I5** (commit) | Caught by one walk up from the adopter, O(depth). |
| `borrow` cycle of any length | legal | A co-death group: none dies alone, all die together in one transaction. |
| mixed own/borrow cycle | legal | As long as ownership alone stays acyclic; the borrow fragments may be inert (pair #5). |
| outsider borrows a child: `A own B`, `C bor B` | legal | `delete(B)` is vetoed by C — and so is `delete(A)`: the cascade can't take B while C survives. A borrow pins every ancestor of its target; the error names the pinned node and the borrower. |
| outsider option at a child: `C option B` | legal | The cascade through A nils C's pointer. Never blocks. |
| child borrows an outsider: `A own B`, `B bor C` | legal | `delete(C)` is blocked while A's subtree lives; `delete(A)` releases the veto automatically. |
| borrow/option at a grandparent and beyond | legal, inert | Same mechanics as pair #5, by a longer path; deliberately not caught pairwise — wakes after reparenting. |

The whole error surface, in one paragraph: at `Register`, only I1 (two roles on
one field), I2 (shapes and `inverse` well-formedness) and I3 (satisfiability)
can reject. At commit, only: a nil `own`/`ownedby`/`borrow` on a survivor, the
borrow veto (a survivor borrowing a node in `D`, directly or through a deep
cascade), deleting the target of a to-one `own` without a swap, I4, I5 and I6.
Nothing else ever errors — `option` and `inverse` are structurally incapable of
blocking a commit.

### Patterns

**Trees are spelled top-down — the structure knows, the node doesn't.** A
parent *pointer* would be `ownedby`, and `ownedby` is mandatory, so
`Node{ Parent *Node rel:"ownedby,Id" }` is rejected at `Register` (I3): every
node would demand a parent and no root could ever exist. This is deliberate —
the Angular take: a component doesn't know its route, the router does. Give the
forest an owner type and navigate downward:

```go
type Tree struct {
    Id    int     `key:"primary"`
    Roots []*Node `rel:"own,TreeId"`
}

type Node struct {
    Id       int     `key:"primary"`
    TreeId   int     // raw FK — set on top-level nodes only
    ParentId int     // raw FK — set on nested nodes only
    Children []*Node `rel:"own,ParentId"`
}
```

Each node has exactly one owner — the `Tree` or its parent node; I4 counts
edges, not fields. `Delete(node)` means the same thing at every level: prune
that subtree, shrink the slice above. No special root case, no surprise root
error at runtime. Need the parent? Look it up by `ParentId` — upward navigation
is a query, not a stored pointer.

**Co-death is a shared owner, never mutual `own`** (I5 forbids the cycle):

```go
type Invoice struct {
    Id     int      `key:"primary"`
    Header *Header  `rel:"own,Id"`
    Total  *Summary `rel:"own,Id"` // Header and Summary die together — with the Invoice
}
```

**Pinning without owning is `borrow`, optionally with an `inverse` view:**

```go
type Loan struct {
    Id   int   `key:"primary"`
    Book *Book `rel:"borrow,Id"`
}
type Book struct {
    Id    int     `key:"primary"`
    Loans []*Loan `rel:"inverse,Book"` // computed; Book can't die while non-empty
}
```

**A self-sufficient holder is `option` + `inverse` — the canonical log:**

```go
type Log struct {
    Id   int   `key:"primary"`
    User *User `rel:"option,Id"` // user vanishes → pointer nils, the log lives on
}
type User struct {
    Id   int    `key:"primary"`
    Logs []*Log `rel:"inverse,User"` // navigation only, no lifecycle
}
```

**m2m is a junction, and the grammar gives exactly two spellings.** Symmetric —
both sides outlive the link, links are deleted explicitly:

```go
type PlaylistEntry struct {
    Id       int       `key:"primary"`
    Playlist *Playlist `rel:"borrow,Id"`
    Song     *Song     `rel:"borrow,Id"`
}
```

Or the link belongs to one side — dies with the playlist, pins the song:

```go
type PlaylistEntry struct {
    Id       int       `key:"primary"`
    Playlist *Playlist `rel:"ownedby,Id"`
    Song     *Song     `rel:"borrow,Id"`
}
```

SQL's double-CASCADE — the link auto-dying with *either* side — is deliberately
unspellable: it would need two owners, and I4 forbids that. The cost is deleting
links explicitly; the payoff is a forest that never has shared subtrees.

The relation grammar is enforced at `Register`, persisted as foreign keys and
rehydrated into stable pointers on `Open`. Lifecycle invariants are checked at
commit, including cascades, borrow vetoes and option set-null.

## Embed or relate?

Embedding is the degenerate `own`: a leaf the owner holds exclusively and that
needs no identity of its own.

Not every nested struct is a relation. A relation costs you a separate type, its
own store, an `Id`, and a flattened FK that gets rehydrated on every `Open`. You
pay that only when the inner thing needs an **identity of its own** — when it's
referenced from more than one place, or addressed on its own (`FindOneBy`, its
own `Id`). When it has no identity of its own, **embed it by value**:

```go
type User struct {
    Id      int     `key:"primary"`
    Account Account            // a value, not a relation
}

u.Account.Balance // plain field access — nothing to wire, nothing to look up
```

For something the User exclusively owns, the embedded form is strictly better
than `Account *Account` + a relation:

- **Lifecycle is free.** The Account lives inside the User's row, so it's created,
  copied, persisted and deleted with its owner — no dangling FK, no cascade rule.
- **Exclusive by construction.** A value can't be shared, so "belongs to one
  owner" holds without you enforcing anything.
- **One lookup.** It's already in the struct: no FK match on load, no store, no
  `Register[Account]()`.

The deciding question is identity, not how you read the data. Wanting to scan
every account and compare two of its fields is *not* a reason to relate — you
just iterate the users and read `u.Account`. Reach for `rel` only when the inner
type must be addressed or referenced on its own.

(A relation *inside* an embedded value isn't wired on load yet — see TODO — so
for now embed only leaf data.)

## Status

Alpha. API will change. Don't store anything you can't lose. Enjoy the db in 
your weekend project

**Solid:**
- Zero dependencies — stdlib `encoding/gob` only.
- RAM-speed reads — O(1) `Id` lookups, contiguous scans.
  badger on point read and scan by a wide margin (`bench/compare`).
- Per-chunk durable writes; durable single-row write ~9µs (WAL).
- OCC transaction path (`Get` / `Update` / `UpdateWithin`) is goroutine-safe,
  race-tested.

**Not there yet:**
- Not fully ACID. Per-chunk `Flush` isn't cross-chunk atomic; the WAL is
  per-type so a cross-type transaction isn't atomic either. Isolation only
  covers the transaction path — `FindOneBy` / `Filter` / `Flush` still assume a
  single goroutine.
- Dataset must fit in RAM (target < 100 MB).
- Non-`Id` queries are full scans.
- Field rename breaks the gob files until schema migration lands.
