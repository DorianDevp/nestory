# Contributing

Nestory is an alpha database project. Small changes can alter durability,
concurrency, or lifecycle guarantees, so contributions should make the intended
contract explicit and verify it at the public API boundary.

## Local setup

Requirements:

- Go 1.24
- `gofumpt`
- `goimports`
- `golangci-lint`

The Makefile resolves the tools from `$(go env GOPATH)/bin`.

## Before committing

Run the repository's formatter and linter pipeline:

```sh
make check
```

`make check` runs both formatters before `golangci-lint`. Also run the tests,
including the race detector for concurrency-sensitive changes:

```sh
go test ./...
go test -race ./...
```

For a focused package while iterating:

```sh
go test ./... -run TestName
go test ./... -bench BenchmarkName -benchmem
```

Do not commit generated benchmark output, temporary data directories, or local
sandbox projects.

## Test expectations

- Reproduce a bug with a failing regression test before fixing it when
  practical.
- Exercise behavior through public APIs unless the test specifically targets a
  codec or index primitive.
- Relation changes must cover schema validation, final-state invariants,
  persistence/reopen, cascade effects, pointer rewiring, and relevant races.
- Durability changes must test clean replay and a torn final frame.
- Index changes must test create, update, delete, uniqueness, reopen, and unsafe
  flush validation.
- Concurrency fixes should include a deterministic coordination point when
  possible; a race detector run alone is not a proof of the intended ordering.

Tests must use `t.TempDir()` or `b.TempDir()` for storage and restore global
state in cleanup.

## Benchmarks

Do not optimize from one end-to-end number. Isolate the suspected cost, measure
allocations, and include scaling points that distinguish a constant-factor
improvement from a complexity change.

The cross-engine harness and caveats are documented in
[bench/COMPARISON.md](../bench/COMPARISON.md). When updating published numbers,
record the machine, Go version, command, durability settings, and record shape.
Compare APIs with equivalent safety and durability guarantees.

## Code style

- Prefer direct, shallow control flow over deeply nested reflection code.
- Keep performance abstractions small and named after the invariant they
  protect.
- Preserve normal Go struct ergonomics; do not introduce proxies or setters to
  intercept field mutation.
- Validate schemas at registration and instances at the transaction boundary.
- Keep WAL-before-publication and deterministic lock ordering visible in code.
- Document any path that intentionally falls back from an incremental delta to
  a full graph rebuild.

## Commits

Keep commits short, focused, and written in the imperative mood, for example:

```text
docs: split guides from readme
fix: preserve inverse order on rewire
perf: bypass graph lock for flat creates
```

Do not add `Co-authored-by` trailers. Avoid mixing unrelated formatting,
refactors, behavior changes, and benchmark updates in one commit.

## Documentation

README is the project landing page. Put detailed behavior in `/docs`:

- user flow in `getting-started.md` or `transactions.md`;
- relation semantics in `relations.md`;
- internal guarantees in `how-it-works.md`;
- measured performance in `bench/COMPARISON.md`.

When an API or invariant changes, update its guide in the same commit as the
implementation.
