package nestory

import (
	"testing"
)

// Test entities live in the nestory package so the test can reset the global
// entityRegistry/baseRegistry and swap DataDir for a temp dir.
//
// Relation primitives in this package:
//   - relto: pointer field. Stored as a flattened FK column named <FieldName><Tag>
//     in the gob schema. Used for O2O and M2O navigation.
//   - mapby: slice-of-pointer field. Not stored. On load, resolved by scanning the
//     target entity and matching <tag-field-on-child> == parent.Id. Used for O2M
//     and as each side of an M2M (via a junction entity with two plain int FKs).
//
// Constraint: a child cannot have BOTH a plain int FK (for mapby) AND a relto
// pointer back to the same parent — they collapse to the same schema column name
// (e.g. "AuthorId") and reflect.StructOf panics on duplicate fields. So the M2M
// junction below uses plain int FKs only, with no back-pointers.

type tSettings struct {
	Id    int `key:"primary"`
	Theme string
}

func (s tSettings) GetId() int { return s.Id }

type tProfile struct {
	Id       int `key:"primary"`
	Nickname string
	Settings *tSettings `relto:"Id"` // O2O
}

func (p tProfile) GetId() int { return p.Id }

type tPost struct {
	Id     int `key:"primary"`
	UserId int // plain FK, consumed by tUser.Posts mapby
	Title  string
}

func (p tPost) GetId() int { return p.Id }

type tComment struct {
	Id     int `key:"primary"`
	Body   string
	Author *tUser `relto:"Id"` // M2O — no back-mapby from tUser
}

func (c tComment) GetId() int { return c.Id }

type tUserTag struct {
	Id     int `key:"primary"`
	UserId int
	TagId  int
}

func (ut tUserTag) GetId() int { return ut.Id }

type tTag struct {
	Id       int `key:"primary"`
	Label    string
	UserTags []*tUserTag `mapby:"TagId"` // M2M side B
}

func (t tTag) GetId() int { return t.Id }

type tUser struct {
	Id       int `key:"primary"`
	Name     string
	Profile  *tProfile   `relto:"Id"`     // O2O
	Posts    []*tPost    `mapby:"UserId"` // O2M
	UserTags []*tUserTag `mapby:"UserId"` // M2M side A
}

func (u tUser) GetId() int { return u.Id }

func resetRegistries() {
	entityRegistry = make(map[string]any)
	baseRegistry = make(map[string]any)
}

func registerAllForTest() {
	Register[tSettings]()
	Register[tProfile]()
	Register[tPost]()
	Register[tComment]()
	Register[tUserTag]()
	Register[tTag]()
	Register[tUser]()
}

func TestRelationsRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := DataDir
	DataDir = tmpDir
	t.Cleanup(func() {
		DataDir = originalDir
		resetRegistries()
	})

	// ===================== phase 1: save =====================
	resetRegistries()
	registerAllForTest()

	settingsBase := Open[tSettings]()
	profileBase := Open[tProfile]()
	postBase := Open[tPost]()
	commentBase := Open[tComment]()
	userTagBase := Open[tUserTag]()
	tagBase := Open[tTag]()
	userBase := Open[tUser]()

	settings := &tSettings{Theme: "dark"}
	mustQueue(t, settingsBase.AddToPersistQueue(settings))
	mustFlush(t, "settings", settingsBase.Flush())

	profile := &tProfile{Nickname: "alice", Settings: settings}
	mustQueue(t, profileBase.AddToPersistQueue(profile))
	mustFlush(t, "profile", profileBase.Flush())

	user := &tUser{Name: "Alice", Profile: profile}
	mustQueue(t, userBase.AddToPersistQueue(user))
	mustFlush(t, "user", userBase.Flush())

	// NOTE: AddToPersistQueue derives the next Id from len(GoEntity), so queueing
	// two items before Flush makes them share Id=1. Flush after each item to get
	// distinct, monotonically increasing Ids.
	post1 := &tPost{UserId: user.Id, Title: "Hello"}
	mustQueue(t, postBase.AddToPersistQueue(post1))
	mustFlush(t, "post1", postBase.Flush())
	post2 := &tPost{UserId: user.Id, Title: "World"}
	mustQueue(t, postBase.AddToPersistQueue(post2))
	mustFlush(t, "post2", postBase.Flush())

	comment := &tComment{Body: "first!", Author: user}
	mustQueue(t, commentBase.AddToPersistQueue(comment))
	mustFlush(t, "comment", commentBase.Flush())

	tagGo := &tTag{Label: "go"}
	mustQueue(t, tagBase.AddToPersistQueue(tagGo))
	mustFlush(t, "tagGo", tagBase.Flush())
	tagTest := &tTag{Label: "test"}
	mustQueue(t, tagBase.AddToPersistQueue(tagTest))
	mustFlush(t, "tagTest", tagBase.Flush())

	link1 := &tUserTag{UserId: user.Id, TagId: tagGo.Id}
	mustQueue(t, userTagBase.AddToPersistQueue(link1))
	mustFlush(t, "link1", userTagBase.Flush())
	link2 := &tUserTag{UserId: user.Id, TagId: tagTest.Id}
	mustQueue(t, userTagBase.AddToPersistQueue(link2))
	mustFlush(t, "link2", userTagBase.Flush())

	savedUserId := user.Id
	savedProfileId := profile.Id
	savedSettingsId := settings.Id
	savedTagGoId := tagGo.Id
	savedTagTestId := tagTest.Id

	t.Logf("phase 1 done: user=%d profile=%d settings=%d tagGo=%d tagTest=%d",
		savedUserId, savedProfileId, savedSettingsId, savedTagGoId, savedTagTestId)

	// ===================== phase 2: reload =====================
	resetRegistries()
	registerAllForTest()

	_ = Open[tSettings]()
	_ = Open[tProfile]()
	_ = Open[tPost]()
	reloadedCommentBase := Open[tComment]()
	_ = Open[tUserTag]()
	reloadedTagBase := Open[tTag]()
	reloadedUserBase := Open[tUser]()

	if got := reloadedUserBase.Len(); got != 1 {
		t.Fatalf("expected 1 user after reload, got %d", got)
	}
	reloadedUser := *reloadedUserBase.All()[0]
	if reloadedUser.Id != savedUserId {
		t.Errorf("user.Id mismatch: got %d, want %d", reloadedUser.Id, savedUserId)
	}

	t.Run("O2O user->profile->settings", func(t *testing.T) {
		if reloadedUser.Profile == nil {
			t.Fatalf("user.Profile is nil (expected Profile{Id=%d})", savedProfileId)
		}
		if reloadedUser.Profile.Id != savedProfileId {
			t.Errorf("profile.Id: got %d, want %d", reloadedUser.Profile.Id, savedProfileId)
		}
		if reloadedUser.Profile.Nickname != "alice" {
			t.Errorf("profile.Nickname: got %q, want %q",
				reloadedUser.Profile.Nickname, "alice")
		}
		if reloadedUser.Profile.Settings == nil {
			t.Fatalf("nested: profile.Settings is nil")
		}
		if reloadedUser.Profile.Settings.Id != savedSettingsId {
			t.Errorf("settings.Id: got %d, want %d",
				reloadedUser.Profile.Settings.Id, savedSettingsId)
		}
		if reloadedUser.Profile.Settings.Theme != "dark" {
			t.Errorf("settings.Theme: got %q, want %q",
				reloadedUser.Profile.Settings.Theme, "dark")
		}
	})

	t.Run("O2M user.Posts via mapby", func(t *testing.T) {
		if got := len(reloadedUser.Posts); got != 2 {
			t.Fatalf("expected 2 posts, got %d", got)
		}
		titles := map[string]bool{}
		for _, p := range reloadedUser.Posts {
			titles[p.Title] = true
			if p.UserId != reloadedUser.Id {
				t.Errorf("post FK mismatch: post.UserId=%d, want %d",
					p.UserId, reloadedUser.Id)
			}
		}
		if !titles["Hello"] || !titles["World"] {
			t.Errorf("missing post titles, got %v", titles)
		}
	})

	t.Run("M2O comment.Author via relto", func(t *testing.T) {
		if got := reloadedCommentBase.Len(); got != 1 {
			t.Fatalf("expected 1 comment, got %d", got)
		}
		c := *reloadedCommentBase.All()[0]
		if c.Author == nil {
			t.Fatalf("comment.Author is nil")
		}
		if c.Author.Id != savedUserId {
			t.Errorf("author.Id: got %d, want %d", c.Author.Id, savedUserId)
		}
		if c.Author.Name != "Alice" {
			t.Errorf("author.Name: got %q, want %q", c.Author.Name, "Alice")
		}
	})

	t.Run("M2M via junction", func(t *testing.T) {
		if got := len(reloadedUser.UserTags); got != 2 {
			t.Errorf("user side: expected 2 links, got %d", got)
		}
		seenTagIds := map[int]bool{}
		for _, l := range reloadedUser.UserTags {
			if l.UserId != reloadedUser.Id {
				t.Errorf("link FK mismatch: link.UserId=%d, want %d",
					l.UserId, reloadedUser.Id)
			}
			seenTagIds[l.TagId] = true
		}
		if !seenTagIds[savedTagGoId] || !seenTagIds[savedTagTestId] {
			t.Errorf("missing tag FK on user side, got %v", seenTagIds)
		}

		tagsById := map[int]*tTag{}
		for _, ptr := range reloadedTagBase.All() {
			tagsById[ptr.Id] = ptr
		}
		for _, id := range []int{savedTagGoId, savedTagTestId} {
			tag, ok := tagsById[id]
			if !ok {
				t.Errorf("tag id=%d missing after reload", id)
				continue
			}
			if got := len(tag.UserTags); got != 1 {
				t.Errorf("tag side: tag id=%d expected 1 link, got %d", id, got)
				continue
			}
			if tag.UserTags[0].UserId != savedUserId {
				t.Errorf("tag id=%d back-link UserId: got %d, want %d",
					id, tag.UserTags[0].UserId, savedUserId)
			}
		}
	})
}

func mustQueue(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("AddToPersistQueue failed: %v", err)
	}
}

func mustFlush(t *testing.T, label string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Flush(%s) failed: %v", label, err)
	}
}
