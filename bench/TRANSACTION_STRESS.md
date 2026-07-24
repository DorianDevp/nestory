# Transaction branch stress report

Measured on 2026-07-24 using:

- Go `go1.26.5-X:nodwarf5`;
- Linux/amd64;
- Intel Core i5-8600K, 6 hardware threads;
- temporary test filesystem for WAL and snapshots.

This diagnostic measures how the transaction pipeline scales when one ownership
root has between 10 and 1,000,000 direct children.

## Detached-branch baseline workload

Every measured transaction:

1. calls `Get(root)` and clones the complete ownership branch;
2. changes only the scalar `root.Name`;
3. calls `Update(root)`;
4. performs change detection, WAL append, publication and cleanup.

The relation topology does not change and only the root row is written to the
WAL. The test therefore stresses branch handling rather than structural graph
validation or large durable frames.

The stage observer was enabled only for this diagnostic and removed after the
measurement. Each size had a cold transaction followed by steady-state
transactions. Steady-state iteration counts decreased with branch size:

| children | iterations |
|---:|---:|
| 10 | 1,000 |
| 100 | 500 |
| 1,000 | 100 |
| 10,000 | 20 |
| 100,000 | 3 |
| 1,000,000 | 1 |

## Complete steady-state scale

| children | `Get` | commit | complete transaction | GC cycles during series |
|---:|---:|---:|---:|---:|
| 10 | 8.77 us | 7.19 us | **16.09 us** | 2/1,000 |
| 100 | 67.22 us | 25.47 us | **92.85 us** | 10/500 |
| 1,000 | 700.77 us | 212.67 us | **913.65 us** | 32/100 |
| 10,000 | 7.32 ms | 1.96 ms | **9.28 ms** | 9/20 |
| 100,000 | 90.26 ms | 20.64 ms | **110.90 ms** | 1/3 |
| 1,000,000 | 1.430 s | 292.23 ms | **1.722 s** | **0/1** |

The transition from 100,000 to 1,000,000 children is 15.5 times slower for a
10-times larger branch. The million-child transaction did not run a GC cycle,
so that super-linear degradation came from the transaction pipeline, memory
locality, maps and pointer chasing rather than the collector.

## Cold ownership-cache transaction

The first access computes and sorts the ownership branch. Subsequent accesses
reuse the cached key list.

| children | build branch key list | `Get` | commit | complete transaction |
|---:|---:|---:|---:|---:|
| 10 | 6.67 us | 30.82 us | 19.50 us | **50.54 us** |
| 100 | 44.91 us | 115.87 us | 38.64 us | **154.69 us** |
| 1,000 | 654.47 us | 1.50 ms | 228.30 us | **1.73 ms** |
| 10,000 | 8.59 ms | 16.37 ms | 1.87 ms | **18.23 ms** |
| 100,000 | 108.02 ms | 198.26 ms | 20.86 ms | **219.12 ms** |
| 1,000,000 | **1.260 s** | **2.770 s** | 277.54 ms | **3.048 s** |

## Steady-state `Get` stages

| stage | 10 | 100 | 1,000 | 10,000 | 100,000 | 1,000,000 |
|---|---:|---:|---:|---:|---:|---:|
| prepare graph | 22 ns | 24 ns | 41 ns | 72 ns | 116 ns | 217 ns |
| read cached branch | 49 ns | 54 ns | 72 ns | 155 ns | 207 ns | 247 ns |
| lock resources | 669 ns | 5.95 us | 58.25 us | 617.14 us | 7.43 ms | **132.39 ms** |
| clone `work` and `original` | 3.13 us | 24.60 us | 246.26 us | 2.35 ms | 25.53 ms | **322.29 ms** |
| rewire copies | 1.85 us | 15.00 us | 165.42 us | 1.71 ms | 21.97 ms | **459.04 ms** |
| register transaction resources | 1.73 us | 14.85 us | 170.28 us | 1.96 ms | 28.02 ms | **381.89 ms** |
| unlock resources | 675 ns | 5.95 us | 58.34 us | 679.35 us | 7.29 ms | **134.02 ms** |
| **complete `Get`** | **8.77 us** | **67.22 us** | **700.77 us** | **7.32 ms** | **90.26 ms** | **1.430 s** |

For one million children, the largest `Get` components were:

- reflection-based rewiring: 459 ms;
- one `engine.record` call per resource: 382 ms;
- two entity copies per resource: 322 ms;
- locking and unlocking one million resources: 266 ms combined.

## Steady-state commit stages

| stage | 10 | 100 | 1,000 | 10,000 | 100,000 | 1,000,000 |
|---|---:|---:|---:|---:|---:|---:|
| copy transaction state | 137 ns | 1.01 us | 11.36 us | 74.95 us | 709.76 us | **25.12 ms** |
| detect changed entities | 1.68 us | 13.94 us | 141.20 us | 1.31 ms | 12.67 ms | **135.84 ms** |
| classify graph access | 1.36 us | 3.38 us | 20.33 us | 187.06 us | 1.85 ms | **18.75 ms** |
| acquire graph lock | 24 ns | 28 ns | 48 ns | 99 ns | 136 ns | 159 ns |
| acquire changed-row lock | 42 ns | 62 ns | 112 ns | 299 ns | 642 ns | 526 ns |
| validate version | 29 ns | 32 ns | 48 ns | 87 ns | 264 ns | 153 ns |
| complete WAL | 2.64 us | 3.10 us | 5.16 us | 10.50 us | 12.34 us | **13.12 us** |
| publish live `*T` | 52 ns | 65 ns | 128 ns | 323 ns | 344 ns | 585 ns |
| evict transaction resources | 416 ns | 2.95 us | 32.68 us | 365.79 us | 5.40 ms | **112.50 ms** |
| **complete commit** | **7.19 us** | **25.47 us** | **212.67 us** | **1.96 ms** | **20.64 ms** | **292.23 ms** |

Although only `root.Name` changes, commit still:

- compares all one million detached resources;
- compares the million-element `Children` relation while classifying graph
  access;
- deletes one million snapshot bindings from the transaction engine.

Publishing the changed root into its stable live `*T` took 585 ns.

## WAL stages

| stage | 10 | 100 | 1,000 | 10,000 | 100,000 | 1,000,000 |
|---|---:|---:|---:|---:|---:|---:|
| encode changed row | 681 ns | 784 ns | 1.64 us | 3.01 us | 3.55 us | 3.23 us |
| acquire WAL lock | 25 ns | 31 ns | 33 ns | 79 ns | 126 ns | 143 ns |
| encode frame | 60 ns | 73 ns | 159 ns | 271 ns | 403 ns | 511 ns |
| write | 920 ns | 1.20 us | 1.98 us | 5.05 us | 5.41 us | 4.61 us |
| fsync | 522 ns | 547 ns | 642 ns | 1.09 us | 1.13 us | 1.20 us |
| **complete WAL** | **2.64 us** | **3.10 us** | **5.16 us** | **10.50 us** | **12.34 us** | **13.12 us** |

WAL cost remained nearly constant because the transaction changed only one
row. The measured `fsync` latency is not representative of a production
filesystem: the test used a temporary filesystem where `fsync` was
unrealistically cheap. Durable latency must be measured again with `DataDir`
placed on the intended storage device.

## Setup and memory

Setup creates the complete graph and calls `Unsafe().Flush`. A forced GC then
stabilizes the retained heap before the transaction measurement.

| children | create DB and flush | stabilizing GC | live heap after setup | heap after transaction series | RSS after transaction series |
|---:|---:|---:|---:|---:|---:|
| 10 | 435 us | 415 us | 0.3 MiB | 1.3 MiB | 15.8 MiB |
| 100 | 516 us | 406 us | 0.4 MiB | 0.8 MiB | 15.4 MiB |
| 1,000 | 3.29 ms | 606 us | 2.0 MiB | 2.8 MiB | 15.3 MiB |
| 10,000 | 37.86 ms | 1.71 ms | 15.5 MiB | 29.1 MiB | 50.8 MiB |
| 100,000 | 420.33 ms | 9.32 ms | 136.6 MiB | 287.0 MiB | 299.2 MiB |
| 1,000,000 | **5.663 s** | **244.79 ms** | **1.772 GiB** | **2.720 GiB** | **2.660 GiB** |

The million-child transaction temporarily required about 950 MiB above the
settled database heap.

## Million-child summary

```text
1.722 s
├── Get: 1.430 s
│   ├── rewiring:             459 ms
│   ├── engine.record:        382 ms
│   ├── two entity copies:    322 ms
│   ├── lock one million:     132 ms
│   └── unlock one million:   134 ms
└── Commit: 292 ms
    ├── change detection:     136 ms
    ├── engine eviction:      112 ms
    ├── transaction snapshot:  25 ms
    ├── graph classification:  19 ms
    └── WAL:                   13 us
```

The limiting behavior in this baseline was not GC or WAL. Changing one scalar
on the root required cloning, registering, rewiring, locking and later
comparing the complete ownership subtree.

## Tower result

The same scalar-root workload was rerun after adding the project-wide Tower
shadow, cached semantic field plans, a cached hot branch, changed-field patch
materialization, and batch transaction registration. Each steady-state result
below is the mean of three transactions. `cold` includes the first Tower
replica build and its first commit.

Command:

```sh
NESTORY_STRESS=1 go test -run '^$' \
  -bench '^BenchmarkTowerScalarRootWriteStress$' \
  -benchtime=3x -benchmem -count=1
```

| children | Tower cold | Tower steady | previous steady | steady reduction | steady allocations |
|---:|---:|---:|---:|---:|---:|
| 10 | 33.15 us | 9.07 us | 16.09 us | 43.6% | 1,845 B / 36 |
| 100 | 130.44 us | 15.07 us | 92.85 us | 83.8% | 1,845 B / 36 |
| 1,000 | 1.27 ms | 87.92 us | 913.65 us | 90.4% | 1,845 B / 36 |
| 10,000 | 14.01 ms | 801.91 us | 9.28 ms | 91.4% | 1,845 B / 36 |
| 100,000 | 184.43 ms | 8.63 ms | 110.90 ms | 92.2% | 1,845 B / 36 |
| 1,000,000 | **2.509 s** | **150.39 ms** | **1.722 s** | **91.3%** | **1,845 B / 36** |

The steady million-child transaction no longer allocates or clones in
proportion to the branch size. It still performs a linear semantic scan:
arbitrary writes through ordinary Go pointers cannot be intercepted by a
setter or proxy. The remaining 150 ms is therefore primarily change discovery,
not WAL publication or GC.

The cold path remains O(project size) and intentionally pays for the persistent
shadow nodes, shadow relation wiring, relation-ID baselines, and hot-branch
descriptor cache. This trades retained replica memory and one serialized Tower
writer for low-allocation warm writes. Canonical zero-copy `View` readers do
not wait for the user callback; they only contend with the short publication
window.
