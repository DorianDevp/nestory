package nestory

import (
	"testing"
)

type o2oSettings struct {
	Id    int `key:"primary"`
	Theme string
}

func (s o2oSettings) GetId() int { return s.Id }

type o2oProfile struct {
	Id       int `key:"primary"`
	Nickname string
	Settings *o2oSettings `relto:"Id"`
}

func (p o2oProfile) GetId() int { return p.Id }

type o2oUser struct {
	Id      int `key:"primary"`
	Name    string
	Profile *o2oProfile `relto:"Id"`
}

func (u o2oUser) GetId() int { return u.Id }

type o2mPost struct {
	Id     int `key:"primary"`
	UserId int // FK consumed by o2mUser.Posts mapby
	Title  string
}

func (p o2mPost) GetId() int { return p.Id }

type o2mUser struct {
	Id    int `key:"primary"`
	Name  string
	Posts []*o2mPost `mapby:"UserId"`
}

func (u o2mUser) GetId() int { return u.Id }

type m2oUser struct {
	Id   int `key:"primary"`
	Name string
}

func (u m2oUser) GetId() int { return u.Id }

type m2oComment struct {
	Id     int `key:"primary"`
	Body   string
	Author *m2oUser `relto:"Id"`
}

func (c m2oComment) GetId() int { return c.Id }

// A child cannot carry BOTH a plain int FK (for the parent's mapby) AND a relto
// back-pointer to the same parent: both flatten to the same schema column (e.g.
// "UserId") and reflect.StructOf panics on duplicate fields. So the junction holds
// plain int FKs only, with no back-pointers.

type m2mUserTag struct {
	Id     int `key:"primary"`
	UserId int
	TagId  int
}

func (ut m2mUserTag) GetId() int { return ut.Id }

type m2mTag struct {
	Id       int `key:"primary"`
	Label    string
	UserTags []*m2mUserTag `mapby:"TagId"` // side B
}

func (t m2mTag) GetId() int { return t.Id }

type m2mUser struct {
	Id       int `key:"primary"`
	Name     string
	UserTags []*m2mUserTag `mapby:"UserId"` // side A
}

func (u m2mUser) GetId() int { return u.Id }

func resetRegistries() {
	storeRegistry = make(map[string]any)
	baseRegistry = make(map[string]any)
}

// runIsolated gives a subtest an isolated world: its own DataDir and empty registries,
// both restored on teardown. The body is responsible for registering the types it
// needs (typically twice: once to save, once after resetRegistries to reload).
func runIsolated(t *testing.T, name string, fn func(t *testing.T)) {
	t.Run(name, func(t *testing.T) {
		originalDir := DataDir
		DataDir = t.TempDir()

		resetRegistries()

		t.Cleanup(func() {
			DataDir = originalDir
			resetRegistries()
		})

		fn(t)
	})
}

func mustRegister(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
}

func save[T Entity](t *testing.T, db *DB[T], label string, e *T) {
	t.Helper()
	db.AddToPersistQueue(e)
	if err := db.Flush(); err != nil {
		t.Fatalf("save %s: %v", label, err)
	}
}

func del[T Entity](t *testing.T, db *DB[T], label string, e *T) {
	t.Helper()

	db.QueueDelete((*e).GetId())

	if err := db.Flush(); err != nil {
		t.Fatalf("save %s: %v", label, err)
	}
}

func TestRelations(t *testing.T) {
	// General relation behaviour

	runIsolated(t, "Owner throws error on empty relation", func(t *testing.T) {
	})

	runIsolated(t, "Owner cascadely deletes children", func(t *testing.T) {
	})

	runIsolated(t, "Ownedby dies after parent", func(t *testing.T) {
	})

	runIsolated(t, "Borrower prevents from deleting owner", func(t *testing.T) {
	})

	runIsolated(t, "Borrower deletion does not affects owner", func(t *testing.T) {
	})

	runIsolated(t, "Option relation after deleting relation insert null", func(t *testing.T) {
	})

	runIsolated(t, "Inverse insits on having two-way described relation", func(t *testing.T) {
	})

	runIsolated(t, "Inverse properly behaves on ownedby", func(t *testing.T) {
	})

	runIsolated(t, "Inverse properly behaves on borrow", func(t *testing.T) {
	})

	runIsolated(t, "Inverse properly behaves on option", func(t *testing.T) {
	})

	// Rules of relations

	runIsolated(t, "Owned value can only be owned ONCE", func(t *testing.T) {
	})

	runIsolated(t, "Ownedby can only have own or option relations", func(t *testing.T) {
	})
	
}
