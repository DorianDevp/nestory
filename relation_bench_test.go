package nestory

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
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

type dialogBenchNode struct {
	Id       int `key:"primary"`
	ParentID int
	Text     string
	Children []*dialogBenchNode `rel:"own,ParentID"`
}

func (node dialogBenchNode) GetId() int { return node.Id }

var relationBranchSizes = []int{0, 1, 10, 100, 1000}

// BenchmarkTowerScalarRootWriteStress is opt-in because the largest setup
// deliberately retains a million-node ownership graph and its Tower shadow.
// Run with:
// NESTORY_STRESS=1 go test -run '^$' -bench '^BenchmarkTowerScalarRootWriteStress$' -benchtime=1x -benchmem
func BenchmarkTowerScalarRootWriteStress(b *testing.B) {
	if os.Getenv("NESTORY_STRESS") != "1" {
		b.Skip("set NESTORY_STRESS=1 to allocate the full stress graph")
	}

	for _, children := range []int{10, 100, 1000, 10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)

			coldStarted := time.Now()
			if err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
				owner.Name = "cold"
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			coldElapsed := time.Since(coldStarted)

			b.ResetTimer()
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
					if iteration%2 == 0 {
						owner.Name = "even"
					} else {
						owner.Name = "odd"
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(coldElapsed.Nanoseconds()), "cold-ns")
		})
	}
}

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

func newDialogBenchDB(tb testing.TB, nodes int) (*DB[dialogBenchNode], *dialogBenchNode, []*dialogBenchNode) {
	tb.Helper()
	DataDir = tb.TempDir()
	resetRegistries()
	if err := Register[dialogBenchNode](); err != nil {
		tb.Fatal(err)
	}

	db := Open[dialogBenchNode]()
	all := make([]*dialogBenchNode, 0, nodes)
	root := &dialogBenchNode{Text: "root"}
	db.Unsafe().Create(root)
	all = append(all, root)
	for index := 1; index < nodes; index++ {
		parent := all[(index-1)/2]
		node := &dialogBenchNode{ParentID: parent.Id, Text: "message"}
		db.Unsafe().Create(node)
		parent.Children = append(parent.Children, node)
		all = append(all, node)
	}

	if err := db.Unsafe().Flush(); err != nil {
		tb.Fatal(err)
	}

	return db, root, all
}

type wideDialogBench struct {
	db       *DB[dialogBenchNode]
	leftID   int
	rightID  int
	branchID int
	scalarID int
}

func newWideDialogBenchDB(tb testing.TB, nodes int) wideDialogBench {
	tb.Helper()
	if nodes < 5 {
		tb.Fatal("wide dialog benchmark needs at least five nodes")
	}

	DataDir = tb.TempDir()
	resetRegistries()
	if err := Register[dialogBenchNode](); err != nil {
		tb.Fatal(err)
	}

	db := Open[dialogBenchNode]()
	root := &dialogBenchNode{Text: "root"}
	left := &dialogBenchNode{Text: "left"}
	right := &dialogBenchNode{Text: "right"}
	branch := &dialogBenchNode{Text: "branch"}
	scalar := &dialogBenchNode{Text: "scalar"}
	for _, node := range []*dialogBenchNode{root, left, right, branch, scalar} {
		db.Unsafe().Create(node)
	}

	left.ParentID = root.Id
	right.ParentID = root.Id
	branch.ParentID = left.Id
	scalar.ParentID = root.Id
	left.Children = []*dialogBenchNode{branch}
	root.Children = []*dialogBenchNode{left, right, scalar}
	for len(root.Children)+2 < nodes {
		filler := &dialogBenchNode{ParentID: root.Id, Text: "filler"}
		db.Unsafe().Create(filler)
		root.Children = append(root.Children, filler)
	}

	if err := db.Unsafe().Flush(); err != nil {
		tb.Fatal(err)
	}

	return wideDialogBench{
		db: db, leftID: left.Id, rightID: right.Id,
		branchID: branch.Id, scalarID: scalar.Id,
	}
}

func findDialogNode(root *dialogBenchNode, id int) *dialogBenchNode {
	if root.Id == id {
		return root
	}

	for _, child := range root.Children {
		if found := findDialogNode(child, id); found != nil {
			return found
		}
	}

	return nil
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

func BenchmarkDialogTreeAddBranch(b *testing.B) {
	for _, nodes := range []int{50, 100} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			quiet(b)
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				db, root, all := newDialogBenchDB(b, nodes)
				parentID := all[nodes/3].Id
				b.StartTimer()

				err := db.Transaction(func(tx *Tx[dialogBenchNode]) error {
					branch := &dialogBenchNode{Text: "branch-0"}
					branch.Children = []*dialogBenchNode{{Text: "branch-1", Children: []*dialogBenchNode{{Text: "branch-2"}}}}
					current, getErr := tx.Get(root.Id)
					if getErr != nil {
						return getErr
					}

					parent := findDialogNode(current, parentID)
					parent.Children = append(parent.Children, branch)
					return tx.Create(branch)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRelationIndexedCreate(b *testing.B) {
	for _, graphChildren := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("graph-children=%d", graphChildren), func(b *testing.B) {
			quiet(b)
			DataDir = b.TempDir()
			resetRegistries()
			if err := Register[relationBenchChild](); err != nil {
				b.Fatal(err)
			}

			if err := Register[relationBenchOwner](); err != nil {
				b.Fatal(err)
			}

			childDB := Open[relationBenchChild]()
			ownerDB := Open[relationBenchOwner]()
			target := &relationBenchOwner{Name: "target"}
			filler := &relationBenchOwner{Name: "filler"}
			ownerDB.Unsafe().Create(target)
			ownerDB.Unsafe().Create(filler)
			for index := range graphChildren {
				owner := filler
				if index < 10 {
					owner = target
				}

				child := &relationBenchChild{OwnerID: owner.Id, Value: index}
				childDB.Unsafe().Create(child)
				owner.Children = append(owner.Children, child)
			}

			if err := ownerDB.Unsafe().Flush(); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for iteration := range b.N {
				child := &relationBenchChild{Value: graphChildren + iteration}
				err := ownerDB.Transaction(func(tx *Tx[relationBenchOwner]) error {
					owner, err := tx.Get(target.Id)
					if err != nil {
						return err
					}

					owner.Children = append(owner.Children, child)
					return childDB.Join(tx).Create(child)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDialogTreeReparentBranch(b *testing.B) {
	for _, nodes := range []int{50, 100} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			quiet(b)
			db, root, all := newDialogBenchDB(b, nodes)
			leftID := all[1].Id
			rightID := all[2].Id
			branchID := all[3].Id
			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				fromID, toID := leftID, rightID
				if iteration%2 == 1 {
					fromID, toID = rightID, leftID
				}

				err := db.UpdateWithin(root.Id, func(current *dialogBenchNode) error {
					from := findDialogNode(current, fromID)
					to := findDialogNode(current, toID)
					for index, child := range from.Children {
						if child.Id != branchID {
							continue
						}

						from.Children = append(from.Children[:index], from.Children[index+1:]...)
						to.Children = append(to.Children, child)
						return nil
					}

					return fmt.Errorf("branch %d is not owned by %d", branchID, fromID)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDialogTreeTargetedScalarWrite(b *testing.B) {
	for _, nodes := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			quiet(b)
			world := newWideDialogBenchDB(b, nodes)
			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				err := world.db.UpdateWithin(world.scalarID, func(node *dialogBenchNode) error {
					if iteration%2 == 0 {
						node.Text = "scalar-even"
					} else {
						node.Text = "scalar-odd"
					}

					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDialogTreeTargetedReparent(b *testing.B) {
	for _, nodes := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			quiet(b)
			world := newWideDialogBenchDB(b, nodes)
			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				if err := reparentDialogBranch(world, iteration); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDialogTreeScalarLatencyUnderReparent(b *testing.B) {
	for _, nodes := range []int{100, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			quiet(b)
			world := newWideDialogBenchDB(b, nodes)
			stop := make(chan struct{})
			ready := make(chan error, 1)
			done := make(chan error, 1)
			go continuouslyReparentDialog(world, stop, ready, done)
			if err := <-ready; err != nil {
				b.Fatal(err)
			}

			latencies := make([]int64, b.N)
			b.ResetTimer()
			for iteration := range b.N {
				started := time.Now()
				err := world.db.UpdateWithin(world.scalarID, func(node *dialogBenchNode) error {
					if iteration%2 == 0 {
						node.Text = "scalar-even"
					} else {
						node.Text = "scalar-odd"
					}

					return nil
				})
				latencies[iteration] = time.Since(started).Nanoseconds()
				if err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()

			close(stop)
			if err := <-done; err != nil {
				b.Fatal(err)
			}

			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			b.ReportMetric(float64(nearestRank(latencies, 50)), "p50-ns/op")
			b.ReportMetric(float64(nearestRank(latencies, 95)), "p95-ns/op")
			b.ReportMetric(float64(nearestRank(latencies, 99)), "p99-ns/op")
		})
	}
}

func continuouslyReparentDialog(world wideDialogBench, stop <-chan struct{}, ready, done chan<- error) {
	for iteration := 0; ; iteration++ {
		select {
		case <-stop:
			done <- nil
			return
		default:
		}

		err := reparentDialogBranch(world, iteration)
		if iteration == 0 {
			ready <- err
		}

		if err != nil {
			done <- err
			return
		}
	}
}

func reparentDialogBranch(world wideDialogBench, iteration int) error {
	fromID, toID := world.leftID, world.rightID
	if iteration%2 == 1 {
		fromID, toID = toID, fromID
	}

	return world.db.Transaction(func(tx *Tx[dialogBenchNode]) error {
		from, err := tx.Get(fromID)
		if err != nil {
			return err
		}

		to, err := tx.Get(toID)
		if err != nil {
			return err
		}

		if len(from.Children) != 1 || from.Children[0].Id != world.branchID {
			return fmt.Errorf("branch %d is not owned by %d", world.branchID, fromID)
		}

		to.Children = append(to.Children, from.Children[0])
		from.Children = nil

		return nil
	})
}

func nearestRank(samples []int64, percentile int) int64 {
	index := (len(samples)*percentile+99)/100 - 1
	if index < 0 {
		return 0
	}

	return samples[index]
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

// BenchmarkTowerVersusBranchClone runs the same logical write — set the root's
// scalar Name — down all three paths, so the numbers differ only by what each
// does around fn. The Tower path keeps a shadow and diffs it; the other two
// clone the whole ownership branch per call. It exists to locate the branch
// size where keeping a replica starts paying for itself.
func BenchmarkTowerVersusBranchClone(b *testing.B) {
	for _, children := range []int{1, 2, 4, 10, 100, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("children=%d/tower", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			if err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
				owner.Name = "warm"
				return nil
			}); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				if err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
					owner.Name = strconv.Itoa(iteration)
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("children=%d/transaction", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				err := ownerDB.Transaction(func(tx *Tx[relationBenchOwner]) error {
					return tx.UpdateWithin(id, func(owner *relationBenchOwner) error {
						owner.Name = strconv.Itoa(iteration)
						return nil
					})
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("children=%d/get-update", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				branch, err := ownerDB.Get(id)
				if err != nil {
					b.Fatal(err)
				}

				branch.Name = strconv.Itoa(iteration)
				if err := ownerDB.Update(branch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTrackedRootWrite is BenchmarkTowerVersusBranchClone's tower case with
// the write declared, so the diff sees one node instead of the whole branch. The
// gap between the two is what the declaration is worth.
func BenchmarkTrackedRootWrite(b *testing.B) {
	for _, children := range []int{1, 2, 4, 10, 100, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("children=%d/tracked", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			warmTowerReplica(b, ownerDB, id)

			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				err := ownerDB.Tracked().UpdateWithin(id, func(_ *Writes, owner *relationBenchOwner) error {
					owner.Name = strconv.Itoa(iteration)
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("children=%d/untracked", children), func(b *testing.B) {
			quiet(b)
			ownerDB, _, id := newRelationBenchDB(b, children)
			warmTowerReplica(b, ownerDB, id)

			b.ResetTimer()
			b.ReportAllocs()
			for iteration := range b.N {
				err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
					owner.Name = strconv.Itoa(iteration)
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func warmTowerReplica(tb testing.TB, ownerDB *DB[relationBenchOwner], id int) {
	tb.Helper()

	if err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
		owner.Name = "warm"
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
}

// BenchmarkTowerParallelBranches writes scalar roots whose ownership branches
// are disjoint, so nothing about the work itself forces them into a queue. It
// exists to keep the Tower from quietly going back to one writer at a time.
func BenchmarkTowerParallelBranches(b *testing.B) {
	const owners, children = 8, 2000

	quiet(b)
	DataDir = b.TempDir()
	resetRegistries()
	if err := Register[relationBenchChild](); err != nil {
		b.Fatal(err)
	}

	if err := Register[relationBenchOwner](); err != nil {
		b.Fatal(err)
	}

	childDB := Open[relationBenchChild]()
	ownerDB := Open[relationBenchOwner]()
	ids := make([]int, owners)
	for index := range owners {
		owner := &relationBenchOwner{Name: "root"}
		ownerDB.Unsafe().Create(owner)
		for value := range children {
			child := &relationBenchChild{OwnerID: owner.Id, Value: value}
			childDB.Unsafe().Create(child)
			owner.Children = append(owner.Children, child)
		}

		ids[index] = owner.Id
	}

	if err := ownerDB.Unsafe().Flush(); err != nil {
		b.Fatal(err)
	}

	// Warm the replica so the loop measures steady state, not the first build.
	if err := ownerDB.UpdateWithin(ids[0], func(owner *relationBenchOwner) error {
		owner.Name = "warm"
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	var sequence atomic.Uint64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := ids[int(sequence.Add(1)-1)%owners]
		value := 0
		for pb.Next() {
			value++
			if err := ownerDB.UpdateWithin(id, func(owner *relationBenchOwner) error {
				owner.Name = strconv.Itoa(value)
				return nil
			}); err != nil {
				b.Error(err)
				return
			}
		}
	})
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
