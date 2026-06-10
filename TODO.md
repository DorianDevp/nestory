# TODO

Stuff I still want to do, roughly most-important first. Data safety before nice-to-haves.

## Done

- [x] Atomic save (tmp + fsync + rename), now per chunk.
- [x] Chunked arena so `*T` addresses stay stable as the dataset grows — relations never dangle.
- [x] Auto-id counter seeded from disk on Open (no id reuse after reload).
- [x] `FindOneBy("Id", …)` hits the index, O(1).
- [x] Tombstone deletes (no shifting, pointers stay valid).
- [x] Per-chunk save in parallel — only dirty chunks get rewritten.
- [x] Optimistic transactions: Get / Update / UpdateWithin, per-row version + mutex,
      first writer wins, ErrConflict + snapshot refresh. Race-tested.
- [x] Write-ahead log per type — a commit is durable without a full Flush.
- [x] Idempotent Open (one DB per type, shared by the engine).
- [x] Benchmarks against SQLite, bbolt, go-memdb, buntdb, badger.

## In progress

- [ ] Deleting a parent that something points to → next Open panics on the dangling FK.
      Need a policy: cascade delete, null the FK, or skip with a warning.
- [ ] Hydrate relations on embedded structs. fillRelation only walks the top type's fields,
      so a `relto`/`mapby` inside an embedded value (e.g. User{ Account Account } where
      Account has its own FK) is never wired. Two sketches:
        a) a dependency node keyed by field path — (account, balance, 3) — so the walker
           knows where the relational field lives inside the nesting, or
        b) hoist relational fields to a flat list at registration and walk the stores from
           there to fill them.
      Decide the embed-vs-relate rule alongside this: embed only leaves (no own relations,
      no own identity); the trigger for a real relation is **identity** — being referenced,
      or addressed by its own key — not merely being read in bulk.
- [ ] Relation role tags. Replace bare `relto`/`mapby` with one `rel:"role,field"` tag
      naming the lifecycle role at the definition site (Rust-inspired lifetime descriptors,
      NOT Rust semantics). Four bond descriptors on the reference holder + one computed mirror:
        - `own`     — full power; deleting me deletes the target (to-one or to-many).
        - `ownedby` — own's NAMED COMPLEMENT: the same edge from the owned side; deleting
                      the owner deletes me. TO-ONE ONLY — `[]*T ownedby` is an error.
                      Declaring it makes ownership MANDATORY for the type (nil = error,
                      NO root special case); at most one ownedby field per type (I3).
        - `borrow`  — veto; target may not be deleted while I hold it (restrict, commit-time).
        - `weak`    — no power; deleting the target nils my pointer (setnull). Nullable.
        - `inverse` — computed `[]*T` view of who points at me; never stored, filled like
                      mapby today. TO-MANY ONLY; second arg must name a `borrow`/`weak` field
                      on the element type whose target type is the declaring type. inverse of
                      own/ownedby is a schema error — the mirror of own IS ownedby.
      A `*T` always declares its own bond (no neutral pointer: it must say what happens when
      its target dies); a `[]*T` is authoritative `own` or computed `inverse`. own↔ownedby
      symbiosis: two ends of ONE edge, tag required on one side, complement optional when the
      other side needs a field; consistency between the ends is invariant I6. `field` =
      target-field-to-match on a to-one bond, child back-reference field on a to-many
      `own`/`inverse` (a raw int FK, or the child's ownedby field when two-sided).
      Nullability falls out of role (own/ownedby/borrow non-nullable, weak nullable) — no
      `,nullable` modifier, that idea is dropped. FK storage: on the ownedby side when
      present; flattened on the owner for a lone to-one own (today's relto shape).
- [ ] Rules for the ownership system — invariants I1–I6 + commit mechanics.
      Commit semantics first, every check is defined against them: D = explicit deletes ∪
      ownership closure (walks own edges only); S = survivors; validate the FINAL state.
      Operation order inside a tx never matters (subtree moves, playlist + its borrowing
      entries deleted in any order).
      Schema-time (Register):
        - I1 — one role per field.
        - I2 — shape: ownedby *T only; inverse []*T only, naming a borrow/weak field on the
               element type that points back at the declaring type; own/borrow/weak either.
        - I3 — satisfiability: a schema that can never have instances is rejected. ownedby =
               MANDATORY ownership → at most one ownedby field per type; no ownedby cycle in
               the type graph (incl. self-loop Node{ Parent *Node ownedby } — every node
               demands a parent, no root can exist); no to-one own cycle (every owner demands
               a fresh child by I4 → infinite descent).
      Commit-time (instance):
        - I4 — ≤1 owning edge per node, duplicates counted (same child twice in one own
               slice = error). Reverse-index edge count; ownership can move mid-tx.
        - I5 — ownership acyclic per instance (self-referential types fine; forbidden is an
               instance cycle). One walk up the single owner chain from the adopter, O(depth).
               With I4 → forest; cascade closure terminates.
        - I6 — pair consistency: ownedby pointer ≡ reverse-index owner. One side mutated →
               engine fills the complement; both mutated and disagreeing → error.
      Mechanics (not invariants): cascade closure; borrow veto = no SURVIVOR borrows a
      deleted node — error must name the pinned node and its borrower; a borrower dying in
      the same commit vetoes nothing (deep borrows pin all ancestors of their target; borrows
      inside the doomed subtree are auto-released). weak setnull (ids never reused → no ABA).
      Non-nil own/ownedby/borrow on survivors.
      DROPPED, deliberately (old global ≼-acyclicity and pair exclusivity): mechanisms
      compose instead of being pattern-banned. own+weak on own child = distinguished member
      (Playlist.LastHit); own+borrow on own child = protected member (Playlist.Current can't
      be deleted by accident, still dies with the playlist); mutual borrow = atomic pair
      (deletable only in one tx — debit/credit). Backward borrow/weak at one's own(er) is
      legal but inert while inside the owner's subtree (wakes after reparenting); "my owner"
      is spelled ownedby. Lint inert edges later, maybe.
      Patterns the grammar gives:
        - trees top-down: container type owns roots, own,ParentId slices, raw int FKs; NO
          parent pointers (self-ownedby is unsatisfiable, I3); upward navigation is a query.
          Uniform delete at every level — no special root rules.
        - co-death: never mutual own; a shared owner owns both (Invoice owns Header + Summary).
        - pin without owning: borrow, optionally with an inverse view on the target.
        - self-sufficient holder: weak + inverse view; canonical example a Log.
        - m2m: symmetric junction = two borrows (links deleted explicitly, by design);
          link-belongs-to-one-side = ownedby + borrow. SQL double-CASCADE (link auto-dies
          with either side) is unspellable — two owners, I4 forbids it.
        - unowned nodes are legal for types WITHOUT ownedby (mandatoriness is the child's
          declaration); a Node with ParentId unset is just an unowned row.
      DECIDED:
        - ownership cycles forbidden at instance level; strict forest. Co-death → embedding
          (no identity) or a shared owner / aggregate root. Mutual own = error; mutual
          borrow = legal atomic pair.
        - ownedby is mandatory ownership; NO nil-as-root special case, ever.
        - direct delete of an owned node is allowed: prune its subtree and shrink the owner's
          `own []*T` — EXCEPT under a non-nullable to-one `own`, which can't be left empty:
          delete the owner or swap in a replacement first.
        - target-delete behavior is the role, not a flag: `borrow` = restrict, `weak` = setnull.
        - ALL instance checks deferred to commit: I4–I6, non-nil own/ownedby/borrow, veto,
          setnull.

## Data safety — do these first

- [ ] The non-transactional paths (Flush, FindOneBy, Filter, PatchById, the persist/delete
      queues) don't take `mu`. Either lock them or write down that they're single-goroutine.
- [ ] Flush + a running transaction is a data race — compaction reads `item` without the
      per-row lock. For now it's "don't run them at once"; make that real or coordinate the locks.
- [ ] Per-chunk save isn't atomic across chunks. A crash mid-Flush can leave some chunks new,
      some old. Need a generation marker, or stage everything then rename.
- [ ] The WAL is per type, so a transaction touching two types writes two logs, fsynced
      separately — half of it can survive a crash. Need one shared log or a 2-phase marker.
      (Blocks cascade.)
- [ ] Nobody fsyncs the directory after rename/create, so a crash can lose the rename even
      though the bytes are on disk. Open the dir and Sync it.

## Should fix

- [ ] Stop panicking in the library. fillRelation, schema building, Register/Open, and Flush
      all kill the process on ordinary errors. Return errors instead.
- [ ] `merge` skips zero values, so you can't patch a field back to 0 / "" / false.
- [ ] `FindOneBy` on a slice/map field panics (== on non-comparable). Guard it.
- [ ] A couple of returned errors are capitalized and end in `\n`. Lowercase, no newline.

## Transactions — still open

- [ ] Snapshot is a shallow copy, so only scalar fields are safe to mutate. Relations need a
      cascade-bounded deep copy (the graph is cyclic), with each node tracked by (type, id).
- [ ] apply rewrites the whole row + bumps version; a field diff would only touch what changed.
- [ ] Updating an indexed field doesn't update the index yet.
- [ ] Two separate Gets can't share one transaction (no explicit Begin), and a read-only Get
      never gets cleaned up if Update never comes (timeout? explicit close?).

## Performance

- [ ] Empty Flush still scans the whole store to build the insert-vs-patch set. Skip it when
      the queue is empty.
- [ ] Every commit does a NormalizeToSchema + gob encode under the WAL lock. Cache the row type,
      reuse buffers, maybe batch fsyncs under load.
- [ ] Deleted slots stay in memory until the next Open. A Compact() for long-running processes.

## Later

- [ ] Secondary indexes (only Id is indexed today).
- [ ] Schema migration (add/remove/rename fields without losing the files).
- [ ] Event-sourcing bits: versioned snapshots, Apply(event, handler), AutoSnapshot.

## Tests I'm missing

- [ ] Delete + reload (tombstone, index/resById eviction).
- [ ] PatchById partial updates, including the zero-value limit.
- [ ] WAL torn-tail recovery (truncated last frame dropped, earlier ones replayed).
- [ ] Crash between chunk renames, once cross-chunk atomicity lands.
