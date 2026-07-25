# Nestory documentation

Start with the guide that matches the question you are trying to answer:

| Guide | Covers |
|---|---|
| [Getting started](getting-started.md) | Installation, entity definitions, registration, CRUD, and the first relation |
| [Transactions and data access](transactions.md) | `Get`, `Update`, `Transaction`, `UpdateWithin`, callback views, conflicts, and cross-type work |
| [Relations](relations.md) | The ownership forest, all relation tags, cascades, vetoes, invariants, and common patterns |
| [Indexes and batches](indexes-and-batches.md) | Unique and composite indexes, `ViewRange`, `ViewMany`, and bulk delete |
| [Unsafe API](unsafe.md) | Live pointers, exclusive access, validation, and `Flush` |
| [How it works](how-it-works.md) | Memory layout, WAL and snapshots, commit publication, locking, recovery, and limitations |
| [Contributing](contributing.md) | Local checks, test expectations, benchmarks, and commit style |

Performance numbers and their caveats live in
[the benchmark report](../bench/BENCHMARK_REPORT.md). The source code remains the
authority while the project is in alpha; if behavior and documentation differ,
please open an issue or send a focused fix.
