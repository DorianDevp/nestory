package nestory

import (
	"fmt"
	"testing"
)

// Relation primitives exercised here:
//   - relto: pointer field. Stored as a flattened FK column named <FieldName><Tag>
//     in the gob schema. Used for O2O and M2O navigation.
//   - mapby: slice-of-pointer field. Not stored. On load, resolved by scanning the
//     target entity and matching <tag-field-on-child> == parent.Id. Used for O2M
//     and as each side of an M2M (via a junction entity with two plain int FKs).
//
// Each test below defines its OWN entity types and registers only those. This is
// deliberate: Open -> fillRelation walks every relto/mapby field on a type and
// panics if a referenced target type was never registered. By giving each test a
// tUser-shaped type carrying only the relation under test, we never register more
// than the scenario needs. It also documents, per test, the exact schema that the
// behaviour depends on.

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
	fmt.Println((*e).GetId(), "DUPA")

	if err := db.Flush(); err != nil {
		t.Fatalf("save %s: %v", label, err)
	}
}

func TestRelations(t *testing.T) {
	runIsolated(t, "O2O user->profile->settings", func(t *testing.T) {
		register := func() {
			mustRegister(t, Register[o2oSettings]())
			mustRegister(t, Register[o2oProfile]())
			mustRegister(t, Register[o2oUser]())
		}
		// fillRelation runs per base, at Open. The nested chain only resolves if
		// every base in it is opened — otherwise profile.Settings stays the hollow
		// {Id} pointer from inflateSlice and the live instance is never substituted.
		openAll := func() {
			Open[o2oSettings]()
			Open[o2oProfile]()
			Open[o2oUser]()
		}

		register()
		settings := &o2oSettings{Theme: "dark"}
		save(t, Open[o2oSettings](), "settings", settings)
		profile := &o2oProfile{Nickname: "alice", Settings: settings}
		save(t, Open[o2oProfile](), "profile", profile)
		user := &o2oUser{Name: "Alice", Profile: profile}
		save(t, Open[o2oUser](), "user", user)

		wantUser, wantProfile, wantSettings := user.Id, profile.Id, settings.Id

		resetRegistries()
		register()
		openAll()
		got := *Open[o2oUser]().AllEntities()[0]
		if got.Id != wantUser {
			t.Fatalf("user.Id: got %d, want %d", got.Id, wantUser)
		}

		if got.Profile == nil {
			t.Fatalf("user.Profile is nil (want Profile{Id=%d})", wantProfile)
		}
		if got.Profile.Id != wantProfile {
			t.Errorf("profile.Id: got %d, want %d", got.Profile.Id, wantProfile)
		}
		if got.Profile.Nickname != "alice" {
			t.Errorf("profile.Nickname: got %q, want %q", got.Profile.Nickname, "alice")
		}
		if got.Profile.Settings == nil {
			t.Fatalf("nested profile.Settings is nil")
		}
		if got.Profile.Settings.Id != wantSettings {
			t.Errorf("settings.Id: got %d, want %d", got.Profile.Settings.Id, wantSettings)
		}
		if got.Profile.Settings.Theme != "dark" {
			t.Errorf("settings.Theme: got %q, want %q", got.Profile.Settings.Theme, "dark")
		}
	})

	runIsolated(t, "O2M user.Posts via mapby", func(t *testing.T) {
		register := func() {
			mustRegister(t, Register[o2mPost]())
			mustRegister(t, Register[o2mUser]())
		}

		register()
		user := &o2mUser{Name: "Alice"}
		save(t, Open[o2mUser](), "user", user)
		save(t, Open[o2mPost](), "post1", &o2mPost{UserId: user.Id, Title: "Hello"})
		save(t, Open[o2mPost](), "post2", &o2mPost{UserId: user.Id, Title: "World"})

		wantUser := user.Id

		resetRegistries()
		register()
		got := *Open[o2mUser]().AllEntities()[0]
		if len(got.Posts) != 2 {
			t.Fatalf("expected 2 posts, got %d", len(got.Posts))
		}
		titles := map[string]bool{}
		for _, p := range got.Posts {
			titles[p.Title] = true
			if p.UserId != wantUser {
				t.Errorf("post FK mismatch: post.UserId=%d, want %d", p.UserId, wantUser)
			}
		}
		if !titles["Hello"] || !titles["World"] {
			t.Errorf("missing post titles, got %v", titles)
		}
	})

	runIsolated(t, "M2O comment.Author via relto", func(t *testing.T) {
		register := func() {
			mustRegister(t, Register[m2oUser]())
			mustRegister(t, Register[m2oComment]())
		}

		register()
		user := &m2oUser{Name: "Alice"}
		save(t, Open[m2oUser](), "user", user)
		save(t, Open[m2oComment](), "comment", &m2oComment{Body: "first!", Author: user})

		wantUser := user.Id

		resetRegistries()
		register()
		got := *Open[m2oComment]().AllEntities()[0]
		if got.Author == nil {
			t.Fatalf("comment.Author is nil")
		}
		if got.Author.Id != wantUser {
			t.Errorf("author.Id: got %d, want %d", got.Author.Id, wantUser)
		}
		if got.Author.Name != "Alice" {
			t.Errorf("author.Name: got %q, want %q", got.Author.Name, "Alice")
		}
	})

	runIsolated(t, "M2M via junction", func(t *testing.T) {
		register := func() {
			mustRegister(t, Register[m2mUserTag]())
			mustRegister(t, Register[m2mTag]())
			mustRegister(t, Register[m2mUser]())
		}

		register()
		user := &m2mUser{Name: "Alice"}
		save(t, Open[m2mUser](), "user", user)
		tagGo := &m2mTag{Label: "go"}
		save(t, Open[m2mTag](), "tagGo", tagGo)
		tagTest := &m2mTag{Label: "test"}
		save(t, Open[m2mTag](), "tagTest", tagTest)
		save(t, Open[m2mUserTag](), "link1", &m2mUserTag{UserId: user.Id, TagId: tagGo.Id})
		save(t, Open[m2mUserTag](), "link2", &m2mUserTag{UserId: user.Id, TagId: tagTest.Id})

		wantUser, wantTagGo, wantTagTest := user.Id, tagGo.Id, tagTest.Id

		resetRegistries()
		register()

		// user side: both links present, each pointing back at the user.
		gotUser := *Open[m2mUser]().AllEntities()[0]
		if len(gotUser.UserTags) != 2 {
			t.Fatalf("user side: expected 2 links, got %d", len(gotUser.UserTags))
		}
		seenTagIds := map[int]bool{}
		for _, l := range gotUser.UserTags {
			if l.UserId != wantUser {
				t.Errorf("link FK mismatch: link.UserId=%d, want %d", l.UserId, wantUser)
			}
			seenTagIds[l.TagId] = true
		}
		if !seenTagIds[wantTagGo] || !seenTagIds[wantTagTest] {
			t.Errorf("missing tag FK on user side, got %v", seenTagIds)
		}

		// tag side: each tag resolves its single back-link to the user.
		tagsById := map[int]*m2mTag{}
		for _, ptr := range Open[m2mTag]().AllEntities() {
			tagsById[ptr.Id] = ptr
		}
		for _, id := range []int{wantTagGo, wantTagTest} {
			tag, ok := tagsById[id]
			if !ok {
				t.Errorf("tag id=%d missing after reload", id)
				continue
			}
			if len(tag.UserTags) != 1 {
				t.Errorf("tag side: tag id=%d expected 1 link, got %d", id, len(tag.UserTags))
				continue
			}
			if tag.UserTags[0].UserId != wantUser {
				t.Errorf("tag id=%d back-link UserId: got %d, want %d",
					id, tag.UserTags[0].UserId, wantUser)
			}
		}
	})

	runIsolated(t, "Deleted FK does not crashes DB", func(t *testing.T) {
		register := func() {
			mustRegister(t, Register[o2oProfile]())
			mustRegister(t, Register[o2oUser]())
		}

		openAll := func() {
			Open[o2oProfile]()
			Open[o2oUser]()
		}

		register()

		profile := &o2oProfile{Nickname: "alice" }
		save(t, Open[o2oProfile](), "profile", profile)

		user := &o2oUser{Name: "Alice", Profile: profile}
		save(t, Open[o2oUser](), "user", user)

		wantUser, wantProfile := user.Id, profile.Id

		resetRegistries()

		register()
		openAll()

		got := *Open[o2oUser]().AllEntities()[0]
		if got.Id != wantUser {
			t.Fatalf("user.Id: got %d, want %d", got.Id, wantUser)
		}

		if got.Profile == nil {
			t.Fatalf("user.Profile is nil (want Profile{Id=%d})", wantProfile)
		}

		if got.Profile.Id != wantProfile {
			t.Errorf("profile.Id: got %d, want %d", got.Profile.Id, wantProfile)
		}

		if got.Profile.Nickname != "alice" {
			t.Errorf("profile.Nickname: got %q, want %q", got.Profile.Nickname, "alice")
		}

		del(t, Open[o2oProfile](), "profile", profile)

		resetRegistries()

		register()
		openAll()

		gotAfter := *Open[o2oUser]().AllEntities()[0]

		profileDB := Open[o2oProfile]()

		if profile, _ := profileDB.Get(1); profile != nil {
			t.Fatalf("Profile was not deleted")
		}

		if gotAfter.Id != wantUser {
			t.Fatalf("user.Id: gotAfter %d, want %d", got.Id, wantUser)
		}

		if gotAfter.Id != wantUser {
			t.Fatalf("user.Id: gotAfter %d, want %d", got.Id, wantUser)
		}

		if gotAfter.Profile != nil {
			t.Fatalf("Profile Id: got %d, want %+v", gotAfter.Profile.Id,  nil)
		}
	})

}
