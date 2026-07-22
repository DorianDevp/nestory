# Getting started

## Install

```sh
go get github.com/DorianDevp/nestory
```

Nestory currently requires Go 1.24 and has no runtime dependencies outside the
standard library.

## Define an entity

Every stored type implements `nestory.Entity`: it has an integer ID exposed by
`GetId`. A zero ID passed to `Create` is assigned by Nestory.

```go
type Note struct {
	Id    int
	Title string
	Body  []byte
}

func (note Note) GetId() int { return note.Id }
```

`Id` is the primary key by convention. IDs are not reused after restart.

## Select the data directory

Set `DataDir` before registering any type:

```go
nestory.DataDir = "./data"
```

The current alpha uses process-global registration, so configure and open one
Nestory dataset per process. A scoped store API is planned.

## Register, then open

`Register[T]` validates the schema and loads snapshots plus WAL records.
`Open[T]` creates the typed database and wires relations. Register every type
in a relation graph before opening any of them.

```go
if err := nestory.Register[Note](); err != nil {
	log.Fatal(err)
}

notes := nestory.Open[Note]()
```

For related types:

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

if err := nestory.Register[Profile](); err != nil {
	log.Fatal(err)
}
if err := nestory.Register[User](); err != nil {
	log.Fatal(err)
}

nestory.Open[Profile]()
users := nestory.Open[User]()
```

## Create

`Create` commits the entity and every new entity in its `own` subtree. IDs are
written back to the supplied objects.

```go
user := &User{
	Name:    "Ada",
	Profile: &Profile{Theme: "dark"},
}

if err := users.Create(user); err != nil {
	log.Fatal(err)
}

fmt.Println(user.Id, user.Profile.Id)
```

For several creates or a mixture of operations, use one transaction instead of
paying for a durable commit per call.

## Read and update

`Get` returns a detached copy of the entity and its complete ownership subtree.
Edit ordinary fields, then merge the branch with `Update`:

```go
user, err := users.Get(userID)
if err != nil {
	return err
}

user.Name = "Grace"
user.Profile.Theme = "light"

if err := users.Update(user); err != nil {
	return err
}
```

For a read-only hot path, avoid the copy with `View`:

```go
err := users.View(userID, func(user *User) error {
	render(user.Name, user.Profile.Theme)
	return nil
})
```

The pointer passed to `View` is read-only by contract and must not be retained
for synchronized use after the callback.

## Delete

```go
if err := users.Delete(userID); err != nil {
	return err
}
```

Deleting an owner also deletes its complete `own` subtree. A surviving
`borrow` reference into that subtree vetoes the commit; surviving `option`
references are cleared. See [Relations](relations.md) for the full contract.

## Next steps

- Choose the right editing model in [Transactions and data access](transactions.md).
- Add query paths using [Indexes and batches](indexes-and-batches.md).
- Read the complete [relation grammar](relations.md) before modeling a graph.
- Understand durability and constraints in [How it works](how-it-works.md).
