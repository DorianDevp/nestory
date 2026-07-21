package nestory

import (
	"errors"
	"reflect"
	"testing"
)

// Schema fixtures. They intentionally stay small: each type isolates one row
// of the spelling table in README.md.
type schemaTarget struct {
	Id int `key:"primary"`
}

func (v schemaTarget) GetId() int { return v.Id }

type schemaOther struct {
	Id int `key:"primary"`
}

func (v schemaOther) GetId() int { return v.Id }

type schemaNoTagPtr struct {
	Id     int `key:"primary"`
	Target *schemaTarget
}

func (v schemaNoTagPtr) GetId() int { return v.Id }

type schemaNoTagSlice struct {
	Id      int `key:"primary"`
	Targets []*schemaTarget
}

func (v schemaNoTagSlice) GetId() int { return v.Id }

type schemaRawFK struct {
	Id       int `key:"primary"`
	TargetID int
}

func (v schemaRawFK) GetId() int { return v.Id }

type schemaOwnPtr struct {
	Id     int           `key:"primary"`
	Target *schemaTarget `rel:"own,Id"`
}

func (v schemaOwnPtr) GetId() int { return v.Id }

type schemaTree struct {
	Id       int `key:"primary"`
	ParentID int
	Children []*schemaTree `rel:"own,ParentID"`
}

func (v schemaTree) GetId() int { return v.Id }

type schemaOwnedBy struct {
	Id    int           `key:"primary"`
	Owner *schemaTarget `rel:"ownedby,Id"`
}

func (v schemaOwnedBy) GetId() int { return v.Id }

type schemaOwnedBySlice struct {
	Id     int             `key:"primary"`
	Owners []*schemaTarget `rel:"ownedby,Id"`
}

func (v schemaOwnedBySlice) GetId() int { return v.Id }

type schemaBorrowPtr struct {
	Id     int           `key:"primary"`
	Target *schemaTarget `rel:"borrow,Id"`
}

func (v schemaBorrowPtr) GetId() int { return v.Id }

type schemaBorrowSlice struct {
	Id      int             `key:"primary"`
	Targets []*schemaTarget `rel:"borrow,Id"`
}

func (v schemaBorrowSlice) GetId() int { return v.Id }

type schemaOptionPtr struct {
	Id     int           `key:"primary"`
	Target *schemaTarget `rel:"option,Id"`
}

func (v schemaOptionPtr) GetId() int { return v.Id }

type schemaOptionSlice struct {
	Id      int             `key:"primary"`
	Targets []*schemaTarget `rel:"option,Id"`
}

func (v schemaOptionSlice) GetId() int { return v.Id }

type schemaInverseHolder struct {
	Id     int                  `key:"primary"`
	Target *schemaInverseTarget `rel:"option,Id"`
}

func (v schemaInverseHolder) GetId() int { return v.Id }

type schemaInverseTarget struct {
	Id      int                    `key:"primary"`
	Holders []*schemaInverseHolder `rel:"inverse,Target"`
}

func (v schemaInverseTarget) GetId() int { return v.Id }

type schemaInversePtr struct {
	Id     int                  `key:"primary"`
	Holder *schemaInverseHolder `rel:"inverse,Target"`
}

func (v schemaInversePtr) GetId() int { return v.Id }

type schemaOwnBackHolder struct {
	Id     int                     `key:"primary"`
	Target *schemaInverseOwnTarget `rel:"own,Id"`
}

func (v schemaOwnBackHolder) GetId() int { return v.Id }

type schemaInverseOwnTarget struct {
	Id      int                    `key:"primary"`
	Holders []*schemaOwnBackHolder `rel:"inverse,Target"`
}

func (v schemaInverseOwnTarget) GetId() int { return v.Id }

type schemaWrongInverseHolder struct {
	Id    int          `key:"primary"`
	Other *schemaOther `rel:"option,Id"`
}

func (v schemaWrongInverseHolder) GetId() int { return v.Id }

type schemaWrongInverseTarget struct {
	Id      int                         `key:"primary"`
	Holders []*schemaWrongInverseHolder `rel:"inverse,Other"`
}

func (v schemaWrongInverseTarget) GetId() int { return v.Id }

type schemaRelOnValue struct {
	Id    int `key:"primary"`
	Count int `rel:"option,Id"`
}

func (v schemaRelOnValue) GetId() int { return v.Id }

type schemaMissingField struct {
	Id     int           `key:"primary"`
	Target *schemaTarget `rel:"option,Missing"`
}

func (v schemaMissingField) GetId() int { return v.Id }

type schemaMalformed struct {
	Id     int           `key:"primary"`
	Target *schemaTarget `rel:"option"`
}

func (v schemaMalformed) GetId() int { return v.Id }

type schemaUnknown struct {
	Id     int           `key:"primary"`
	Target *schemaTarget `rel:"share,Id"`
}

func (v schemaUnknown) GetId() int { return v.Id }

type schemaTwoOwners struct {
	Id     int           `key:"primary"`
	First  *schemaTarget `rel:"ownedby,Id"`
	Second *schemaOther  `rel:"ownedby,Id"`
}

func (v schemaTwoOwners) GetId() int { return v.Id }

type schemaOwnCycleA struct {
	Id int              `key:"primary"`
	B  *schemaOwnCycleB `rel:"own,Id"`
}

func (v schemaOwnCycleA) GetId() int { return v.Id }

type schemaOwnCycleB struct {
	Id int              `key:"primary"`
	A  *schemaOwnCycleA `rel:"own,Id"`
}

func (v schemaOwnCycleB) GetId() int { return v.Id }

type schemaOwnedCycleA struct {
	Id int                `key:"primary"`
	B  *schemaOwnedCycleB `rel:"ownedby,Id"`
}

func (v schemaOwnedCycleA) GetId() int { return v.Id }

type schemaOwnedCycleB struct {
	Id int                `key:"primary"`
	A  *schemaOwnedCycleA `rel:"ownedby,Id"`
}

func (v schemaOwnedCycleB) GetId() int { return v.Id }

type schemaSelfOwn struct {
	Id   int            `key:"primary"`
	Next *schemaSelfOwn `rel:"own,Id"`
}

func (v schemaSelfOwn) GetId() int { return v.Id }

type schemaSelfOwnedBy struct {
	Id     int                `key:"primary"`
	Parent *schemaSelfOwnedBy `rel:"ownedby,Id"`
}

func (v schemaSelfOwnedBy) GetId() int { return v.Id }

type schemaSelfBorrow struct {
	Id   int               `key:"primary"`
	Peer *schemaSelfBorrow `rel:"borrow,Id"`
}

func (v schemaSelfBorrow) GetId() int { return v.Id }

func TestRelationSchemaSpellingSpace(t *testing.T) {
	tests := []struct {
		name    string
		typ     reflect.Type
		wantErr bool
	}{
		{"pointer without role", reflect.TypeFor[schemaNoTagPtr](), true},
		{"slice without role", reflect.TypeFor[schemaNoTagSlice](), true},
		{"raw foreign key", reflect.TypeFor[schemaRawFK](), false},
		{"own pointer", reflect.TypeFor[schemaOwnPtr](), false},
		{"own slice and self tree", reflect.TypeFor[schemaTree](), false},
		{"ownedby pointer", reflect.TypeFor[schemaOwnedBy](), false},
		{"ownedby slice", reflect.TypeFor[schemaOwnedBySlice](), true},
		{"borrow pointer", reflect.TypeFor[schemaBorrowPtr](), false},
		{"borrow slice", reflect.TypeFor[schemaBorrowSlice](), false},
		{"option pointer", reflect.TypeFor[schemaOptionPtr](), false},
		{"option slice", reflect.TypeFor[schemaOptionSlice](), false},
		{"inverse pointer", reflect.TypeFor[schemaInversePtr](), true},
		{"inverse slice", reflect.TypeFor[schemaInverseTarget](), false},
		{"relation on scalar", reflect.TypeFor[schemaRelOnValue](), true},
		{"missing target field", reflect.TypeFor[schemaMissingField](), true},
		{"malformed tag", reflect.TypeFor[schemaMalformed](), true},
		{"unknown role", reflect.TypeFor[schemaUnknown](), true},
		{"inverse of own", reflect.TypeFor[schemaInverseOwnTarget](), true},
		{"inverse targets wrong type", reflect.TypeFor[schemaWrongInverseTarget](), true},
		{"two mandatory owners", reflect.TypeFor[schemaTwoOwners](), true},
		{"to-one own type cycle", reflect.TypeFor[schemaOwnCycleA](), true},
		{"ownedby type cycle", reflect.TypeFor[schemaOwnedCycleA](), true},
		{"self to-one own", reflect.TypeFor[schemaSelfOwn](), true},
		{"self ownedby", reflect.TypeFor[schemaSelfOwnedBy](), true},
		{"self borrow", reflect.TypeFor[schemaSelfBorrow](), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildRelationSchema(tt.typ)
			if tt.wantErr && !errors.Is(err, ErrRelationSchema) {
				t.Fatalf("error = %v, want ErrRelationSchema", err)
			}

			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected schema error: %v", err)
			}
		})
	}
}

// Runtime fixtures cover every lifecycle role and both cardinalities.
type rtAsset struct {
	Id   int `key:"primary"`
	Name string
}

func (v rtAsset) GetId() int { return v.Id }

type rtProfile struct {
	Id     int      `key:"primary"`
	Owner  *rtUser  `rel:"ownedby,Id"`
	Avatar *rtAsset `rel:"borrow,Id"`
}

func (v rtProfile) GetId() int { return v.Id }

type rtPost struct {
	Id       int `key:"primary"`
	Title    string
	User     *rtUser      `rel:"ownedby,Id"`
	Watchers []*rtWatcher `rel:"inverse,Post"`
}

func (v rtPost) GetId() int { return v.Id }

type rtLog struct {
	Id   int     `key:"primary"`
	User *rtUser `rel:"option,Id"`
}

func (v rtLog) GetId() int { return v.Id }

type rtWatcher struct {
	Id   int     `key:"primary"`
	Post *rtPost `rel:"borrow,Id"`
}

func (v rtWatcher) GetId() int { return v.Id }

type rtCollection struct {
	Id       int        `key:"primary"`
	Borrowed []*rtAsset `rel:"borrow,Id"`
	Optional []*rtAsset `rel:"option,Id"`
}

func (v rtCollection) GetId() int { return v.Id }

type rtUser struct {
	Id       int `key:"primary"`
	Name     string
	Profile  *rtProfile `rel:"own,Id"`
	Posts    []*rtPost  `rel:"own,User"`
	Current  *rtPost    `rel:"borrow,Id"`
	Favorite *rtPost    `rel:"option,Id"`
	Logs     []*rtLog   `rel:"inverse,User"`
}

func (v rtUser) GetId() int { return v.Id }

type rtParent struct {
	Id       int        `key:"primary"`
	Children []*rtChild `rel:"own,ParentID"`
}

func (v rtParent) GetId() int { return v.Id }

type rtChild struct {
	Id       int `key:"primary"`
	ParentID int
}

func (v rtChild) GetId() int { return v.Id }

type rtNode struct {
	Id       int `key:"primary"`
	ParentID int
	Children []*rtNode `rel:"own,ParentID"`
}

func (v rtNode) GetId() int { return v.Id }

type rtBorrowA struct {
	Id int        `key:"primary"`
	B  *rtBorrowB `rel:"borrow,Id"`
}

func (v rtBorrowA) GetId() int { return v.Id }

type rtBorrowB struct {
	Id int        `key:"primary"`
	A  *rtBorrowA `rel:"borrow,Id"`
}

func (v rtBorrowB) GetId() int { return v.Id }

func resetRegistries() {
	storeRegistry = make(map[string]any)
	baseRegistry = make(map[string]any)
	engine = newEngine()
}

func isolatedRelations(t *testing.T, fn func(t *testing.T)) {
	t.Helper()
	original := DataDir
	DataDir = t.TempDir()
	resetRegistries()
	t.Cleanup(func() {
		DataDir = original
		resetRegistries()
	})
	fn(t)
}

func mustRegisterRuntime(t *testing.T) {
	t.Helper()
	registrations := []struct {
		name     string
		register func() error
	}{
		{"asset", Register[rtAsset]},
		{"profile", Register[rtProfile]},
		{"post", Register[rtPost]},
		{"log", Register[rtLog]},
		{"watcher", Register[rtWatcher]},
		{"collection", Register[rtCollection]},
		{"user", Register[rtUser]},
	}
	for _, registration := range registrations {
		if err := registration.register(); err != nil {
			t.Fatalf("Register %s: %v", registration.name, err)
		}
	}
}

type runtimeWorld struct {
	assetDB      *DB[rtAsset]
	profileDB    *DB[rtProfile]
	postDB       *DB[rtPost]
	logDB        *DB[rtLog]
	watcherDB    *DB[rtWatcher]
	collectionDB *DB[rtCollection]
	userDB       *DB[rtUser]
	asset        *rtAsset
	user         *rtUser
	profile      *rtProfile
	current      *rtPost
	favorite     *rtPost
	log          *rtLog
	watcher      *rtWatcher
	collection   *rtCollection
}

func seedRuntime(t *testing.T) runtimeWorld {
	t.Helper()
	mustRegisterRuntime(t)
	w := runtimeWorld{
		assetDB: Open[rtAsset](), profileDB: Open[rtProfile](), postDB: Open[rtPost](),
		logDB: Open[rtLog](), watcherDB: Open[rtWatcher](), collectionDB: Open[rtCollection](),
		userDB: Open[rtUser](),
	}
	w.asset = &rtAsset{Name: "avatar"}
	w.user = &rtUser{Name: "Ada"}
	w.profile = &rtProfile{Owner: w.user, Avatar: w.asset}
	w.current = &rtPost{Title: "current", User: w.user}
	w.favorite = &rtPost{Title: "favorite", User: w.user}
	w.log = &rtLog{User: w.user}
	w.watcher = &rtWatcher{Post: w.current}
	w.collection = &rtCollection{Borrowed: []*rtAsset{w.asset}, Optional: []*rtAsset{w.asset}}
	w.user.Profile = w.profile
	w.user.Posts = []*rtPost{w.current, w.favorite}
	w.user.Current = w.current
	w.user.Favorite = w.favorite

	w.assetDB.AddToPersistQueue(w.asset)
	w.userDB.AddToPersistQueue(w.user)
	w.profileDB.AddToPersistQueue(w.profile)
	w.postDB.AddToPersistQueue(w.current)
	w.postDB.AddToPersistQueue(w.favorite)
	w.logDB.AddToPersistQueue(w.log)
	w.watcherDB.AddToPersistQueue(w.watcher)
	w.collectionDB.AddToPersistQueue(w.collection)
	if err := w.userDB.Flush(); err != nil {
		t.Fatalf("seed Flush: %v", err)
	}

	return w
}

func TestRelationsPersistAndRehydrateEveryRole(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		seedRuntime(t)
		resetRegistries()
		mustRegisterRuntime(t)
		assetDB, profileDB := Open[rtAsset](), Open[rtProfile]()
		postDB, logDB := Open[rtPost](), Open[rtLog]()
		watcherDB, collectionDB := Open[rtWatcher](), Open[rtCollection]()
		userDB := Open[rtUser]()

		user, err := userDB.FindOneBy("Id", 1)
		if err != nil || user == nil {
			t.Fatalf("load user: user=%v err=%v", user, err)
		}

		if user.Profile == nil || user.Profile.Owner != user {
			t.Fatalf("own/ownedby complement was not rehydrated")
		}

		if len(user.Posts) != 2 || user.Current == nil || user.Favorite == nil {
			t.Fatalf("own/borrow/option fields were not rehydrated: %#v", user)
		}

		if len(user.Logs) != 1 || user.Logs[0].User != user {
			t.Fatalf("option inverse was not rebuilt")
		}

		var current *rtPost
		for _, candidate := range postDB.AllEntities() {
			if candidate.Title == "current" {
				current = candidate
			}
		}

		if current == nil || len(current.Watchers) != 1 || current.Watchers[0].Post != current {
			watcher, _ := watcherDB.FindOneBy("Id", 1)
			watcherCount := -1
			if current != nil {
				watcherCount = len(current.Watchers)
			}

			t.Fatalf("borrow inverse was not rebuilt: posts=%#v current=%p watchers=%d watcher=%#v", postDB.AllEntities(), current, watcherCount, watcher)
		}

		profile, _ := profileDB.FindOneBy("Id", 1)
		asset, _ := assetDB.FindOneBy("Id", 1)
		if profile.Avatar != asset {
			t.Fatalf("borrow pointer is not canonical")
		}

		collection, _ := collectionDB.FindOneBy("Id", 1)
		if len(collection.Borrowed) != 1 || len(collection.Optional) != 1 || collection.Borrowed[0] != asset || collection.Optional[0] != asset {
			t.Fatalf("borrow/option slices were not rehydrated")
		}

		if logDB.Len() != 1 || watcherDB.Len() != 1 {
			t.Fatalf("unowned holders disappeared")
		}
	})
}

func TestRelationDeleteSemantics(t *testing.T) {
	t.Run("to-one owned target cannot die alone", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			w := seedRuntime(t)
			if err := w.profileDB.QueueDelete(w.profile.Id); err != nil {
				t.Fatal(err)
			}

			err := w.profileDB.Flush()
			if !errors.Is(err, ErrDeleteRestricted) {
				t.Fatalf("error=%v, want ErrDeleteRestricted", err)
			}
		})
	})

	t.Run("borrow blocks target deletion", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			w := seedRuntime(t)
			if err := w.postDB.QueueDelete(w.current.Id); err != nil {
				t.Fatal(err)
			}

			err := w.postDB.Flush()
			if !errors.Is(err, ErrDeleteRestricted) {
				t.Fatalf("error=%v, want ErrDeleteRestricted", err)
			}
		})
	})

	t.Run("borrower deletion leaves target alive", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			w := seedRuntime(t)
			if err := w.watcherDB.QueueDelete(w.watcher.Id); err != nil {
				t.Fatal(err)
			}

			if err := w.watcherDB.Flush(); err != nil {
				t.Fatal(err)
			}

			if w.postDB.Len() != 2 || w.watcherDB.Len() != 0 {
				t.Fatalf("target or borrower counts are wrong")
			}

			if len(w.current.Watchers) != 0 {
				t.Fatalf("borrow inverse retained deleted holder")
			}
		})
	})

	t.Run("option nils and own slice shrinks", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			w := seedRuntime(t)
			if err := w.postDB.QueueDelete(w.favorite.Id); err != nil {
				t.Fatal(err)
			}

			if err := w.postDB.Flush(); err != nil {
				t.Fatal(err)
			}

			user, _ := w.userDB.FindOneBy("Id", w.user.Id)
			if user.Favorite != nil {
				t.Fatalf("option pointer was not cleared")
			}

			if len(user.Posts) != 1 || user.Posts[0].Id != w.current.Id {
				t.Fatalf("own slice was not shrunk")
			}
		})
	})

	t.Run("deep cascade ignores doomed borrowers and nulls surviving options", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			w := seedRuntime(t)
			if err := w.watcherDB.QueueDelete(w.watcher.Id); err != nil {
				t.Fatal(err)
			}

			if err := w.userDB.QueueDelete(w.user.Id); err != nil {
				t.Fatal(err)
			}

			if err := w.userDB.Flush(); err != nil {
				t.Fatal(err)
			}

			if w.userDB.Len() != 0 || w.profileDB.Len() != 0 || w.postDB.Len() != 0 {
				t.Fatalf("ownership subtree survived cascade")
			}

			if w.assetDB.Len() != 1 || w.logDB.Len() != 1 {
				t.Fatalf("non-owned nodes were cascaded")
			}

			log, _ := w.logDB.FindOneBy("Id", w.log.Id)
			if log.User != nil {
				t.Fatalf("surviving option was not nilled")
			}
		})
	})

	t.Run("borrow and option slices", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			w := seedRuntime(t)
			if err := w.assetDB.QueueDelete(w.asset.Id); err != nil {
				t.Fatal(err)
			}

			if err := w.assetDB.Flush(); !errors.Is(err, ErrDeleteRestricted) {
				t.Fatalf("error=%v, want borrow veto", err)
			}
		})
	})
}

func TestRelationInstanceInvariants(t *testing.T) {
	t.Run("required own cannot be nil", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			if err := Register[schemaTarget](); err != nil {
				t.Fatal(err)
			}

			if err := Register[schemaOwnPtr](); err != nil {
				t.Fatal(err)
			}

			Open[schemaTarget]().AddToPersistQueue(&schemaTarget{})
			ownerDB := Open[schemaOwnPtr]()
			ownerDB.AddToPersistQueue(&schemaOwnPtr{})
			if err := ownerDB.Flush(); !errors.Is(err, ErrRelationInvariant) {
				t.Fatalf("error=%v, want invariant error", err)
			}
		})
	})

	t.Run("required ownedby cannot be nil", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			if err := Register[schemaTarget](); err != nil {
				t.Fatal(err)
			}

			if err := Register[schemaOwnedBy](); err != nil {
				t.Fatal(err)
			}

			childDB := Open[schemaOwnedBy]()
			Open[schemaTarget]()
			childDB.AddToPersistQueue(&schemaOwnedBy{})
			if err := childDB.Flush(); !errors.Is(err, ErrRelationInvariant) {
				t.Fatalf("error=%v, want invariant error", err)
			}
		})
	})

	t.Run("required borrow cannot be nil", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			if err := Register[schemaTarget](); err != nil {
				t.Fatal(err)
			}

			if err := Register[schemaBorrowPtr](); err != nil {
				t.Fatal(err)
			}

			Open[schemaTarget]()
			borrowerDB := Open[schemaBorrowPtr]()
			borrowerDB.AddToPersistQueue(&schemaBorrowPtr{})
			if err := borrowerDB.Flush(); !errors.Is(err, ErrRelationInvariant) {
				t.Fatalf("error=%v, want invariant error", err)
			}
		})
	})

	t.Run("option accepts nil and missing target", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			if err := Register[schemaTarget](); err != nil {
				t.Fatal(err)
			}

			if err := Register[schemaOptionPtr](); err != nil {
				t.Fatal(err)
			}

			Open[schemaTarget]()
			db := Open[schemaOptionPtr]()
			item := &schemaOptionPtr{Target: &schemaTarget{Id: 999}}
			db.AddToPersistQueue(item)
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}

			stored, _ := db.FindOneBy("Id", item.Id)
			if stored.Target != nil {
				t.Fatalf("dangling option was not cleared")
			}
		})
	})

	t.Run("same child cannot have two owners", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			if err := Register[rtParent](); err != nil {
				t.Fatal(err)
			}

			if err := Register[rtChild](); err != nil {
				t.Fatal(err)
			}

			parentDB, childDB := Open[rtParent](), Open[rtChild]()
			child := &rtChild{}
			first := &rtParent{Children: []*rtChild{child}}
			second := &rtParent{Children: []*rtChild{child}}
			childDB.AddToPersistQueue(child)
			parentDB.AddToPersistQueue(first)
			parentDB.AddToPersistQueue(second)
			if err := parentDB.Flush(); !errors.Is(err, ErrRelationInvariant) {
				t.Fatalf("error=%v, want I4 error", err)
			}
		})
	})

	t.Run("ownership instance cycle is rejected", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			if err := Register[rtNode](); err != nil {
				t.Fatal(err)
			}

			db := Open[rtNode]()
			node := &rtNode{}
			db.AddToPersistQueue(node)
			node.Children = []*rtNode{node}
			if err := db.Flush(); !errors.Is(err, ErrRelationInvariant) {
				t.Fatalf("error=%v, want I5 error", err)
			}
		})
	})

	t.Run("borrow cycle is a co-death group", func(t *testing.T) {
		isolatedRelations(t, func(t *testing.T) {
			if err := Register[rtBorrowA](); err != nil {
				t.Fatal(err)
			}

			if err := Register[rtBorrowB](); err != nil {
				t.Fatal(err)
			}

			aDB, bDB := Open[rtBorrowA](), Open[rtBorrowB]()
			a, b := &rtBorrowA{}, &rtBorrowB{}
			a.B, b.A = b, a
			aDB.AddToPersistQueue(a)
			bDB.AddToPersistQueue(b)
			if err := aDB.Flush(); err != nil {
				t.Fatal(err)
			}

			if err := aDB.QueueDelete(a.Id); err != nil {
				t.Fatal(err)
			}

			if err := aDB.Flush(); !errors.Is(err, ErrDeleteRestricted) {
				t.Fatalf("single delete error=%v", err)
			}

			// Failed commits retain their queues; adding B makes the final D contain both.
			if err := bDB.QueueDelete(b.Id); err != nil {
				t.Fatal(err)
			}

			if err := bDB.Flush(); err != nil {
				t.Fatalf("co-death: %v", err)
			}

			if aDB.Len() != 0 || bDB.Len() != 0 {
				t.Fatalf("borrow cycle did not die atomically")
			}
		})
	})
}

func TestRelationTransactionValidationAndSliceIsolation(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		w := seedRuntime(t)
		live, _ := w.userDB.FindOneBy("Id", w.user.Id)
		before := len(live.Posts)
		snapshot, err := w.userDB.Get(w.user.Id)
		if err != nil {
			t.Fatal(err)
		}

		snapshot.Posts = append(snapshot.Posts, snapshot.Posts[0])
		if len(live.Posts) != before {
			t.Fatalf("snapshot slice mutated live state before commit")
		}

		if err := w.userDB.Update(snapshot); !errors.Is(err, ErrRelationInvariant) {
			t.Fatalf("error=%v, want duplicate-owner invariant", err)
		}
	})
}
