package nestory

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// assertCommittedIndexConsistent rebuilds the committed relation index from
// scratch and deep-compares it against the delta-maintained one. This is the
// harness that turns the class of bug this session shipped twice — a delta
// leaving derived state stale in a way no behavioural test noticed — into a
// red test. Call it after any operation that publishes through a delta.
func assertCommittedIndexConsistent(t *testing.T) {
	t.Helper()

	published := committedRelationIndexSnapshot()
	if published == nil {
		return
	}

	nodes, err := collectRelationNodes(false, nil)
	if err != nil {
		t.Fatalf("differential: collect nodes: %v", err)
	}

	model, err := buildRelationModel(nodes)
	if err != nil {
		t.Fatalf("differential: rebuild model: %v", err)
	}

	rebuilt := buildCommittedRelationIndex(model, nil, 0)

	if diff := diffCommittedIndexes(published, rebuilt); diff != "" {
		t.Fatalf("differential: delta-maintained index diverged from full rebuild:\n%s", diff)
	}

	// Ownership children live outside the index, in committedOwnership.outgoing;
	// a delta updates them through addCommittedChild/removeCommittedChild, so
	// they can drift independently of everything above.
	committedOwnership.RLock()
	outgoing := committedOwnership.outgoing
	for child, owner := range rebuilt.owners {
		if !containsNodeKey(outgoing[owner], child) {
			committedOwnership.RUnlock()
			t.Fatalf("differential: outgoing[%v] is missing child %v", owner, child)
		}
	}

	for owner, children := range outgoing {
		for _, child := range children {
			if rebuilt.owners[child] != owner {
				committedOwnership.RUnlock()
				t.Fatalf("differential: outgoing[%v] holds %v, whose owner is %v", owner, child, rebuilt.owners[child])
			}
		}
	}

	committedOwnership.RUnlock()
}

// diffCommittedIndexes reports every way two indexes disagree, empty when none.
// Comparison is by value semantics, not representation: field target order
// matters (an own slice is ordered), incoming counts do not.
func diffCommittedIndexes(published, rebuilt *committedRelationIndex) string {
	var out strings.Builder

	compareNodeSets(&out, published.nodes, rebuilt.nodes)

	for key, target := range rebuilt.targets {
		got, present := published.targets[key]
		if !present {
			fmt.Fprintf(&out, "targets: missing %v\n", key)
			continue
		}

		if got != target {
			fmt.Fprintf(&out, "targets[%v] = %v, want %v\n", key, got, target)
		}
	}

	for key, target := range published.targets {
		if _, present := rebuilt.targets[key]; !present && !target.duplicate {
			fmt.Fprintf(&out, "targets: stale extra %v -> %v\n", key, target)
		}
	}

	for key, want := range rebuilt.fields {
		got, present := published.fields[key]
		if !present {
			if len(want.targets) > 0 {
				fmt.Fprintf(&out, "fields: missing %v\n", key)
			}

			continue
		}

		if !reflect.DeepEqual(normalizeTargets(got.targets), normalizeTargets(want.targets)) {
			fmt.Fprintf(&out, "fields[%v].targets = %v, want %v\n", key, got.targets, want.targets)
		}
	}

	for key, got := range published.fields {
		if _, present := rebuilt.fields[key]; !present && len(got.targets) > 0 {
			fmt.Fprintf(&out, "fields: stale extra %v with targets %v\n", key, got.targets)
		}
	}

	for key, want := range rebuilt.owners {
		if got, present := published.owners[key]; !present || got != want {
			fmt.Fprintf(&out, "owners[%v] = %v (present=%t), want %v\n", key, published.owners[key], present, want)
		}
	}

	for key := range published.owners {
		if _, present := rebuilt.owners[key]; !present {
			fmt.Fprintf(&out, "owners: stale extra %v\n", key)
		}
	}

	compareIncoming(&out, published, rebuilt)

	return out.String()
}

func compareNodeSets(out *strings.Builder, published, rebuilt map[nodeKey]reflect.Value) {
	for key, value := range rebuilt {
		got, present := published[key]
		if !present {
			fmt.Fprintf(out, "nodes: missing %v\n", key)
			continue
		}

		if got.Pointer() != value.Pointer() {
			fmt.Fprintf(out, "nodes[%v]: different live pointer\n", key)
		}
	}

	for key := range published {
		if _, present := rebuilt[key]; !present {
			fmt.Fprintf(out, "nodes: stale extra %v\n", key)
		}
	}
}

// compareIncoming compares multiplicity maps, ignoring entries whose count
// dropped to zero: decrementIncoming keeps empty buckets on purpose.
func compareIncoming(out *strings.Builder, published, rebuilt *committedRelationIndex) {
	counts := func(index *committedRelationIndex) map[string]int {
		flat := make(map[string]int)
		for node, incoming := range index.incoming {
			for _, entry := range incoming.entries {
				if entry.count != 0 {
					flat[fmt.Sprintf("%v<-%v/%d", node, entry.field.holder, entry.field.field)] = entry.count
				}
			}
		}

		return flat
	}

	got, want := counts(published), counts(rebuilt)
	for key, count := range want {
		if got[key] != count {
			fmt.Fprintf(out, "incoming[%s] = %d, want %d\n", key, got[key], count)
		}
	}

	for key, count := range got {
		if _, present := want[key]; !present {
			fmt.Fprintf(out, "incoming: stale extra %s = %d\n", key, count)
		}
	}
}

func normalizeTargets(targets []nodeKey) []nodeKey {
	if len(targets) == 0 {
		return nil
	}

	return targets
}

func containsNodeKey(keys []nodeKey, wanted nodeKey) bool {
	for _, key := range keys {
		if key == wanted {
			return true
		}
	}

	return false
}

// TestDifferentialRandomizedWorkload drives every write API in a random
// interleaving and checks the delta-maintained index against a full rebuild
// after each operation. The seed is logged so a failure replays.
func TestDifferentialRandomizedWorkload(t *testing.T) {
	isolatedRelations(t, func(t *testing.T) {
		ownerDB, childDB, owner, children := seedTowerBranch(t)

		second := &relationBenchOwner{Name: "second"}
		ownerDB.Unsafe().Create(second)
		if err := ownerDB.Unsafe().Flush(); err != nil {
			t.Fatal(err)
		}

		assertCommittedIndexConsistent(t)

		operations := []struct {
			name string
			run  func(step int) error
		}{
			{"tower-scalar", func(step int) error {
				return ownerDB.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
					shadow.Name = fmt.Sprintf("tower-%d", step)
					return nil
				})
			}},
			{"detached-scalar", func(step int) error {
				branch, err := ownerDB.Get(owner.Id)
				if err != nil {
					return err
				}

				branch.Name = fmt.Sprintf("detached-%d", step)

				return ownerDB.Update(branch)
			}},
			{"unsafe-create-flush", func(step int) error {
				child := &relationBenchChild{OwnerID: owner.Id, Value: 1000 + step}
				childDB.Unsafe().Create(child)
				owner.Children = append(owner.Children, child)

				return childDB.Unsafe().Flush()
			}},
			{"unsafe-reorder-flush", func(step int) error {
				if len(owner.Children) < 2 {
					return nil
				}

				owner.Children[0], owner.Children[1] = owner.Children[1], owner.Children[0]

				return ownerDB.Unsafe().Flush()
			}},
			{"transaction-scalar", func(step int) error {
				return ownerDB.Transaction(func(tx *Tx[relationBenchOwner]) error {
					return tx.UpdateWithin(owner.Id, func(shadow *relationBenchOwner) error {
						shadow.Name = fmt.Sprintf("tx-%d", step)
						return nil
					})
				})
			}},
			{"leaf-write", func(step int) error {
				return childDB.UpdateWithin(children[0].Id, func(child *relationBenchChild) error {
					child.Value = step
					return nil
				})
			}},
			{"second-root-write", func(step int) error {
				return ownerDB.UpdateWithin(second.Id, func(shadow *relationBenchOwner) error {
					shadow.Name = fmt.Sprintf("second-%d", step)
					return nil
				})
			}},
		}

		// A fixed multiplicative generator: deterministic, so a failure names the
		// exact prefix that produced it.
		const steps = 200
		state := uint64(0x9E3779B97F4A7C15)
		for step := range steps {
			state = state*6364136223846793005 + 1442695040888963407
			operation := operations[state%uint64(len(operations))]
			if err := operation.run(step); err != nil {
				t.Fatalf("step %d (%s): %v", step, operation.name, err)
			}

			assertCommittedIndexConsistent(t)
			if t.Failed() {
				t.Fatalf("diverged after step %d (%s)", step, operation.name)
			}
		}
	})
}
