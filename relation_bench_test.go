package nestory

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

type relationBenchOwner struct {
	Id       int `key:"primary"`
	Name     string
	Children []*relationBenchChild `rel:"own,OwnerID"`
}

func (owner relationBenchOwner) GetId() int { return owner.Id }

type relationBenchChild struct {
	Id      int `key:"primary"`
	OwnerID int
	Value   int
}

func (child relationBenchChild) GetId() int { return child.Id }

type relationBenchOneOwner struct {
	Id      int `key:"primary"`
	Value   int
	Profile *relationBenchProfile `rel:"own,Id"`
}

func (owner relationBenchOneOwner) GetId() int { return owner.Id }

type relationBenchProfile struct {
	Id    int `key:"primary"`
	Value int
	Owner *relationBenchOneOwner `rel:"ownedby,Id"`
}

func (profile relationBenchProfile) GetId() int { return profile.Id }

type parallelBenchA struct {
	Id    int `key:"primary"`
	Value int
}

func (entity parallelBenchA) GetId() int { return entity.Id }

type parallelBenchB struct {
	Id    int `key:"primary"`
	Value int
}

func (entity parallelBenchB) GetId() int { return entity.Id }

var relationBranchSizes = []int{0, 1, 10, 100, 1000}

func newRelationBenchDB(tb testing.TB, children int) (*DB[relationBenchOwner], *DB[relationBenchChild], int) {
	tb.Helper()
	DataDir = tb.TempDir()
	resetRegistries()
	if err := Register[relationBenchChild](); err != nil {
		tb.Fatal(err)
	}

	if err := Register[relationBenchOwner](); err != nil {
		tb.Fatal(err)
	}

	childDB := Open[relationBenchChild]()
	ownerDB := Open[relationBenchOwner]()
	owner := &relationBenchOwner{Name: "root", Children: make([]*relationBenchChild, 0, children)}
	ownerDB.Unsafe().Create(owner)
	for i := 0; i < children; i++ {
		child := &relationBenchChild{OwnerID: owner.Id, Value: i}
		childDB.Unsafe().Create(child)
		owner.Children = append(owner.Children, child)
	}

	if err := ownerDB.Unsafe().Flush(); err != nil {
		tb.Fatal(err)
	}

	return ownerDB, childDB, owner.Id
}

func newOneToOneBenchDB(tb testing.TB) (*DB[relationBenchOneOwner], int) {
	tb.Helper()
	DataDir = tb.TempDir()
	resetRegistries()
	if err := Register[relationBenchProfile](); err != nil {
		tb.Fatal(err)
	}

	if err := Register[relationBenchOneOwner](); err != nil {
		tb.Fatal(err)
	}

	profileDB := Open[relationBenchProfile]()
	ownerDB := Open[relationBenchOneOwner]()
	owner := &relationBenchOneOwner{}
	profile := &relationBenchProfile{Owner: owner}
	owner.Profile = profile
	ownerDB.Unsafe().Create(owner)
	profileDB.Unsafe().Create(profile)
	if err := ownerDB.Unsafe().Flush(); err != nil {
		tb.Fatal(err)
	}

	return ownerDB, owner.Id
}

func BenchmarkRelationBranchLifecycle(b *testing.B) {
	for _, children := range relationBranchSizes {
		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				branch, err := ownerDB.Get(id)
				if err != nil {
					b.Fatal(err)
				}

				if err := ownerDB.Update(branch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRelationView(b *testing.B) {
	for _, children := range relationBranchSizes {
		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				if err := ownerDB.View(id, func(owner *relationBenchOwner) error {
					if len(owner.Children) != children {
						b.Fatal("wrong ownership branch")
					}

					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRelationOneToOneLifecycle(b *testing.B) {
	quiet(b)
	ownerDB, id := newOneToOneBenchDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		branch, err := ownerDB.Get(id)
		if err != nil {
			b.Fatal(err)
		}

		if err := ownerDB.Update(branch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRelationScalarRootWrite(b *testing.B) {
	for _, children := range relationBranchSizes {
		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
					if i%2 == 0 {
						owner.Name = "even"
					} else {
						owner.Name = "odd"
					}

					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRelationLeafWrite(b *testing.B) {
	for _, children := range relationBranchSizes[1:] {
		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
					owner.Children[len(owner.Children)-1].Value = i
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRelationEdgeWrite(b *testing.B) {
	for _, children := range []int{2, 10, 100} {
		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
					owner.Children[0], owner.Children[1] = owner.Children[1], owner.Children[0]
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRelationInvalidCommit(b *testing.B) {
	for _, children := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
					owner.Children = append(owner.Children, owner.Children[0])
					return nil
				})
				if !errors.Is(err, ErrRelationInvariant) {
					b.Fatalf("error = %v, want %v", err, ErrRelationInvariant)
				}
			}
		})
	}
}

func BenchmarkRelationCascadeDelete(b *testing.B) {
	for _, children := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			quiet(b)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ownerDB, _, id := newRelationBenchDB(b, children)
				b.StartTimer()

				if err := ownerDB.Delete(id); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnsafeMutationBatchFlush(b *testing.B) {
	const records = 10000

	for _, mutations := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mutations=%d", mutations), func(b *testing.B) {
			db := newBenchDB(b, records)
			unsafe := db.Unsafe()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for j := 0; j < mutations; j++ {
					id := (i*mutations+j)%records + 1
					item, err := unsafe.Get(id)
					if err != nil {
						b.Fatal(err)
					}

					item.Age++
				}

				if err := unsafe.Flush(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newParallelBenchDBs(tb testing.TB, records int) (*DB[parallelBenchA], *DB[parallelBenchB]) {
	tb.Helper()
	DataDir = tb.TempDir()
	resetRegistries()
	if err := Register[parallelBenchA](); err != nil {
		tb.Fatal(err)
	}

	if err := Register[parallelBenchB](); err != nil {
		tb.Fatal(err)
	}

	first := Open[parallelBenchA]()
	second := Open[parallelBenchB]()
	for i := 0; i < records; i++ {
		first.Unsafe().Create(&parallelBenchA{})
		second.Unsafe().Create(&parallelBenchB{})
	}

	if err := first.Unsafe().Flush(); err != nil {
		tb.Fatal(err)
	}

	return first, second
}

func BenchmarkParallelSafeWrite(b *testing.B) {
	const records = 10000

	b.Run("same-row", func(b *testing.B) {
		quiet(b)
		first, _ := newParallelBenchDBs(b, records)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := first.UpdateWithin(1, func(entity *parallelBenchA) error {
					entity.Value++
					return nil
				}); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	b.Run("distinct-rows", func(b *testing.B) {
		quiet(b)
		first, _ := newParallelBenchDBs(b, records)
		var sequence atomic.Uint64
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				id := int(sequence.Add(1)-1)%records + 1
				if err := first.UpdateWithin(id, func(entity *parallelBenchA) error {
					entity.Value++
					return nil
				}); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	b.Run("distinct-wals", func(b *testing.B) {
		quiet(b)
		first, second := newParallelBenchDBs(b, records)
		var sequence atomic.Uint64
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				next := sequence.Add(1) - 1
				id := int(next/2)%records + 1
				var err error
				if next%2 == 0 {
					err = first.UpdateWithin(id, func(entity *parallelBenchA) error {
						entity.Value++
						return nil
					})
				} else {
					err = second.UpdateWithin(id, func(entity *parallelBenchB) error {
						entity.Value++
						return nil
					})
				}

				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
