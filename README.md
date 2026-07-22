# nestory

Nestory is a persistent in-memory object graph for Go. Your structs are the
database: the complete dataset lives in RAM, relations are ordinary `*T`
pointers, and committed changes are recovered from a write-ahead log.

> [!WARNING]
> Nestory is alpha software. Its API and on-disk format may change. Use it for
> experiments and replaceable data, not as the only copy of production data.

```go
type Profile struct {
	Id    int
	Theme string
}

func (profile Profile) GetId() int { return profile.Id }

type User struct {
	Id      int
	Name    string
	Profile *Profile `rel:"own,Id"`
}

func (user User) GetId() int { return user.Id }

nestory.DataDir = "./data"

if err := nestory.Register[Profile](); err != nil {
	log.Fatal(err)
}
if err := nestory.Register[User](); err != nil {
	log.Fatal(err)
}

nestory.Open[Profile]()
users := nestory.Open[User]()

user := &User{Name: "Ada", Profile: &Profile{Theme: "dark"}}
if err := users.Create(user); err != nil {
	log.Fatal(err)
}

err := users.View(user.Id, func(user *User) error {
	fmt.Println(user.Profile.Theme) // already wired; no query or join
	return nil
})
```

## Why Nestory?

- Normal Go structs and pointers, without generated models, proxies, or
  setters.
- Stable pointers in a chunked arena; inserts do not move existing objects.
- Detached branches and optimistic transactions for safe edits.
- Zero-copy callback reads with `View`, `ViewMany`, and `ViewRange`.
- Durable unique and ordered composite indexes.
- A strict ownership forest with cascade delete, borrow vetoes, optional
  references, and computed inverse views.
- WAL-backed commits, including one crash-atomic frame for transactions that
  touch multiple entity types.
- An explicitly unsafe live-pointer API for exclusive, allocation-sensitive
  workloads.

Nestory deliberately keeps the whole dataset in memory. It is a good fit for
small, hot object graphs and embedded services; it is not a replacement for a
large disk-first database.

## Install

```sh
go get github.com/DorianDevp/nestory
```

Nestory currently requires Go 1.24.

## Documentation

- [Documentation index](docs/README.md)
- [Getting started](docs/getting-started.md)
- [Transactions and data access](docs/transactions.md)
- [Relations and lifecycle rules](docs/relations.md)
- [Indexes and batch operations](docs/indexes-and-batches.md)
- [Unsafe live pointers](docs/unsafe.md)
- [How it works](docs/how-it-works.md)
- [Contributing](docs/contributing.md)
- [Benchmarks and methodology](bench/COMPARISON.md)

## Status

The core read, transactional, relation, indexing, and recovery paths are
race-tested, but the project is still an alpha. Important current limitations
include an in-memory working set, integer primary keys, process-global
registration, no schema migration system, and no stable on-disk compatibility
promise. See [How it works](docs/how-it-works.md#current-limitations) for the
full list.

## License

Nestory is licensed under the GNU General Public License v2.0. See
[LICENSE](LICENSE).
