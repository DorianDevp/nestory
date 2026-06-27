package nestory

import (
	"testing"
)

type user struct {
	Id                   int `key:"primary"`
	OwnedProfile	     *profile `rel:"own,Id"`
	OptionsFavouritePost *post  `rel:"option,Id"`
	InversedPosts        []*post `rel:"inverse,Author"`
}

func (u user) GetId() int {
	return u.Id
}

type profile struct {
	Id              int `key:"primary"`
	BorrowedAvatar  *avatar `rel:"borrow,Id"`
}

func (p profile) GetId() int {
	return p.Id
}

type post struct {
	Id     int `key:"primary"`
	Title  string
	Author *user `rel:"option,Id"`
}

func (p post) GetId() int {
	return p.Id
}

type avatar struct {
	Id   int `key:"primary"`
	Path string
}

func (a avatar) GetId() int {
	return a.Id
}

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

		t.Cleanup(func() {
			DataDir = originalDir
			resetRegistries()
			mustRegister(t)
		})

		fn(t)
	})
}

func mustRegister(t *testing.T) {
	t.Helper()

	err := Register[user]()
	err = Register[profile]()
	err = Register[post]()
	err = Register[avatar]()

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

func seed(t *testing.T) {
	t.Helper()

	avt := avatar {
		Id: 0,
		Path: "/assets/user_avatar.jpg",
	}

	prf := profile{
		Id: 0,
		BorrowedAvatar: &avt,
	}

	usr := user{ 
		Id: 0, 
		OwnedProfile: &prf, 
		OptionsFavouritePost: nil, 
		InversedPosts: make([]*post, 0),
	}

	posts := []*post {
		{ Id: 0, Title: "One", Author: &usr },
	}

	avatarDb := Open[avatar]()
	profileDb := Open[profile]()
	userDb := Open[user]()
	postDb := Open[post]()

	save(t, avatarDb, "avatar_test", &avt)
	save(t, profileDb, "profile_test", &prf)
	save(t, userDb, "user_test", &usr)
	for _, p := range posts {
		save(t, postDb, "post_test", p)
	}
}

func TestRelations(t *testing.T) {
	// General relation behaviour

	// Happy paths
	runIsolated(t, "Seeds work", func(t *testing.T) {
		seed(t)

		avatarDb := Open[avatar]()
		profileDb := Open[profile]()
		userDb := Open[user]()
		postDb := Open[post]()

		if avatarDb.counter > 0 {
			t.Error("avatarDB not loaded")
		}

		if profileDb.counter > 0 {
			t.Error("ProfileDB not loaded")
		} else if prf, err := profileDb.Get(1); err != nil || prf.GetId() != 1 || prf.BorrowedAvatar == nil {
			t.Error("incorrectly loaded profiles")
		}

		if userDb.counter > 0 {
			t.Error("userDB not loaded")
		} else if usr, err := userDb.Get(1); err != nil || usr.GetId() != 1 || len(usr.InversedPosts) < 1 {
			t.Error("Incorrectly loaded user")
		} 

		if postDb.counter > 0 {
			t.Error("postDB not loaded")
		} else if post, err := postDb.Get(1); err != nil || post.GetId() != 1 || post.Author == nil {
			t.Error("Incorrectly loaded post")
		}
	})

	// Edge cases

	runIsolated(t, "Owner throws error on empty relation", func(t *testing.T) {
		userDb := Open[user]()

		err := userDb.UpdateWithin(1, func (u *user) {
			u.OwnedProfile = nil
		})

		err = userDb.Flush()

		if err != nil {
			t.Error(err)
		}
	})

	runIsolated(t, "Owner cascadely deletes children", func(t *testing.T) {
		userDb := Open[user]()
		user, _ := userDb.Get(1)

		del(t, userDb, "user_delete", user)
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
