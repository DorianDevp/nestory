package nestory

import (
	"fmt"
	"reflect"
	"sort"
)

type relationHolderField struct {
	holder nodeKey
	field  int
}

type indexedRelationField struct {
	spec    relationSpec
	targets []nodeKey
}

type committedRelationIndex struct {
	nodes        map[nodeKey]reflect.Value
	targets      map[relationTargetKey]indexedRelationTarget
	targetFields map[reflect.Type]map[string]struct{}
	backFields   map[reflect.Type]map[int]struct{}
	fields       map[relationHolderField]indexedRelationField
	incoming     map[nodeKey]map[relationHolderField]int
	owners       map[nodeKey]nodeKey
	children     map[nodeKey]map[nodeKey]struct{}
}

type relationOwnerChange struct {
	before    nodeKey
	hadBefore bool
	after     nodeKey
	hasAfter  bool
}

type relationIndexDelta struct {
	index          *committedRelationIndex
	model          *relationModel
	fields         map[relationHolderField]indexedRelationField
	ownerChanges   map[nodeKey]relationOwnerChange
	inverseTargets map[nodeKey]struct{}
}

func buildRelationIndexDelta(resources []touchedResource) (*relationIndexDelta, bool, error) {
	index := committedRelationIndexSnapshot()
	if index == nil {
		return nil, false, nil
	}

	overrides := make(map[nodeKey]relationGraphNode, len(resources))
	for _, resource := range resources {
		runtime, ok := baseRegistry[resource.dbName].(relationRuntime)
		if !ok {
			return nil, false, nil
		}

		key := nodeKey{typ: runtime.relationType(), id: resource.id}
		if _, exists := index.nodes[key]; !exists {
			return nil, false, nil
		}

		if relationIndexKeyChanged(index, key, reflect.ValueOf(resource.work)) {
			return nil, false, nil
		}

		overrides[key] = relationGraphNode{key: key, value: reflect.ValueOf(resource.work)}
	}

	model := &relationModel{
		nodes: make(map[nodeKey]relationGraphNode), targets: index.targets, targetFields: index.targetFields,
		owners: make(map[nodeKey]nodeKey), outgoing: make(map[nodeKey][]nodeKey),
	}
	delta := &relationIndexDelta{
		index: index, model: model,
		fields:       make(map[relationHolderField]indexedRelationField),
		ownerChanges: make(map[nodeKey]relationOwnerChange), inverseTargets: make(map[nodeKey]struct{}),
	}
	scannedIncoming := make(map[nodeKey][]incomingOwn)
	scannedOwnedBy := make(map[nodeKey]incomingOwn)
	for key, node := range overrides {
		model.nodes[key] = node
		specs, err := relationSpecs(key.typ)
		if err != nil {
			return nil, false, err
		}

		for _, spec := range specs {
			if spec.kind == inverseRelation {
				delta.inverseTargets[key] = struct{}{}
				continue
			}

			fieldKey := relationHolderField{holder: key, field: spec.fieldIndex}
			if _, exists := index.fields[fieldKey]; !exists {
				return nil, false, nil
			}

			delta.fields[fieldKey] = indexedRelationField{spec: spec}
		}

		if err := scanNodeRelations(model, node, scannedIncoming, scannedOwnedBy); err != nil {
			return nil, true, err
		}
	}

	if err := validateRequiredRelations(model, nil); err != nil {
		return nil, true, err
	}

	for _, ref := range model.refs {
		fieldKey := relationHolderField{holder: ref.holder, field: ref.spec.fieldIndex}
		field := delta.fields[fieldKey]
		field.targets = append(field.targets, ref.target)
		delta.fields[fieldKey] = field
		if _, exists := model.nodes[ref.target]; !exists {
			model.nodes[ref.target] = relationGraphNode{key: ref.target, value: index.nodes[ref.target]}
		}
	}

	if err := delta.validateOwnership(); err != nil {
		return nil, true, err
	}

	if !delta.supportsOwnershipChanges() {
		return nil, false, nil
	}

	delta.collectInverseTargets()

	return delta, true, nil
}

func relationIndexKeyChanged(index *committedRelationIndex, key nodeKey, after reflect.Value) bool {
	before := index.nodes[key]
	for field := range index.targetFields[key.typ] {
		if !relationFieldEqual(before.Elem().FieldByName(field), after.Elem().FieldByName(field)) {
			return true
		}
	}

	for field := range index.backFields[key.typ] {
		if !relationFieldEqual(before.Elem().Field(field), after.Elem().Field(field)) {
			return true
		}
	}

	return false
}

func (delta *relationIndexDelta) validateOwnership() error {
	affected := make(map[nodeKey]struct{})
	for fieldKey, field := range delta.fields {
		old := delta.index.fields[fieldKey]
		if field.spec.kind == ownRelation {
			for _, target := range old.targets {
				affected[target] = struct{}{}
			}

			for _, target := range field.targets {
				affected[target] = struct{}{}
			}
		}

		if field.spec.kind == ownedByRelation {
			affected[fieldKey.holder] = struct{}{}
		}
	}

	for child := range affected {
		raw := delta.effectiveIncomingOwn(child)
		back, hasBack := delta.effectiveOwnedBy(child)
		if len(raw) > 1 {
			return fmt.Errorf("%w: %s has more than one owner", ErrRelationInvariant, child)
		}

		if len(raw) == 1 && hasBack && raw[0].owner != back.owner {
			return fmt.Errorf("%w: own and ownedby disagree for %s (%s vs %s)", ErrRelationInvariant, child, raw[0].owner, back.owner)
		}

		after, hasAfter := resolvedOwner(raw, back, hasBack)
		before, hadBefore := delta.index.owners[child]
		if before == after && hadBefore == hasAfter {
			continue
		}

		delta.ownerChanges[child] = relationOwnerChange{
			before: before, hadBefore: hadBefore, after: after, hasAfter: hasAfter,
		}
	}

	for child := range affected {
		if err := delta.validateOwnerChain(child); err != nil {
			return err
		}
	}

	return nil
}

func (delta *relationIndexDelta) effectiveIncomingOwn(child nodeKey) []incomingOwn {
	out := make([]incomingOwn, 0, 1)
	for fieldKey, count := range delta.index.incoming[child] {
		if _, replaced := delta.fields[fieldKey]; replaced {
			continue
		}

		field := delta.index.fields[fieldKey]
		if field.spec.kind == ownRelation && count > 0 {
			out = append(out, incomingOwn{owner: fieldKey.holder, spec: field.spec})
		}
	}

	for fieldKey, field := range delta.fields {
		if field.spec.kind != ownRelation {
			continue
		}

		for _, target := range field.targets {
			if target == child {
				out = append(out, incomingOwn{owner: fieldKey.holder, spec: field.spec})
			}
		}
	}

	return out
}

func (delta *relationIndexDelta) effectiveOwnedBy(child nodeKey) (incomingOwn, bool) {
	specs, err := relationSpecs(child.typ)
	if err != nil {
		return incomingOwn{}, false
	}

	for _, spec := range specs {
		if spec.kind != ownedByRelation {
			continue
		}

		fieldKey := relationHolderField{holder: child, field: spec.fieldIndex}
		field, changed := delta.fields[fieldKey]
		if !changed {
			field = delta.index.fields[fieldKey]
		}

		if len(field.targets) == 1 {
			return incomingOwn{owner: field.targets[0], spec: spec}, true
		}
	}

	return incomingOwn{}, false
}

func (delta *relationIndexDelta) validateOwnerChain(start nodeKey) error {
	seen := make(map[nodeKey]struct{})
	for at := start; ; {
		if _, duplicate := seen[at]; duplicate {
			return fmt.Errorf("%w: ownership cycle involving %s", ErrRelationInvariant, at)
		}

		seen[at] = struct{}{}
		owner, found := delta.effectiveOwner(at)
		if !found {
			return nil
		}

		at = owner
	}
}

func (delta *relationIndexDelta) effectiveOwner(child nodeKey) (nodeKey, bool) {
	if changed, exists := delta.ownerChanges[child]; exists {
		return changed.after, changed.hasAfter
	}

	owner, found := delta.index.owners[child]
	return owner, found
}

func (delta *relationIndexDelta) supportsOwnershipChanges() bool {
	for _, change := range delta.ownerChanges {
		if change.hadBefore && !change.hasAfter {
			return false
		}
	}

	return true
}

func (delta *relationIndexDelta) collectInverseTargets() {
	for fieldKey, field := range delta.fields {
		if field.spec.kind != borrowRelation && field.spec.kind != optionRelation {
			continue
		}

		for _, target := range delta.index.fields[fieldKey].targets {
			delta.inverseTargets[target] = struct{}{}
		}

		for _, target := range field.targets {
			delta.inverseTargets[target] = struct{}{}
		}
	}
}

func (delta *relationIndexDelta) publishAndRewire() {
	committedOwnership.Lock()
	index := committedOwnership.index
	if index != delta.index {
		committedOwnership.Unlock()
		panic("nestory: committed relation index changed under graph lock")
	}

	for fieldKey, replacement := range delta.fields {
		old := index.fields[fieldKey]
		for _, target := range old.targets {
			index.decrementIncoming(target, fieldKey)
		}

		replacement.targets = append([]nodeKey(nil), replacement.targets...)
		index.fields[fieldKey] = replacement
		for _, target := range replacement.targets {
			index.incrementIncoming(target, fieldKey)
		}
	}

	for child, change := range delta.ownerChanges {
		if change.hadBefore {
			index.removeChild(change.before, child)
			removeCommittedChild(change.before, child)
			delete(index.owners, child)
		}

		if change.hasAfter {
			index.owners[child] = change.after
			index.addChild(change.after, child)
			addCommittedChild(change.after, child)
		}
	}

	committedOwnership.graph = nil
	committedOwnership.branches = make(map[nodeKey][]nodeKey)
	committedOwnership.Unlock()

	delta.rewire(index)
}

func removeCommittedChild(owner, child nodeKey) {
	children := committedOwnership.outgoing[owner]
	for i, candidate := range children {
		if candidate != child {
			continue
		}

		committedOwnership.outgoing[owner] = append(children[:i], children[i+1:]...)
		if len(committedOwnership.outgoing[owner]) == 0 {
			delete(committedOwnership.outgoing, owner)
		}

		return
	}
}

func addCommittedChild(owner, child nodeKey) {
	for _, candidate := range committedOwnership.outgoing[owner] {
		if candidate == child {
			return
		}
	}

	committedOwnership.outgoing[owner] = append(committedOwnership.outgoing[owner], child)
}

func (delta *relationIndexDelta) rewire(index *committedRelationIndex) {
	for fieldKey, indexed := range delta.fields {
		value := index.nodes[fieldKey.holder]
		setIndexedRelationField(value.Elem().Field(indexed.spec.fieldIndex), indexed.targets, index.nodes)
	}

	for target := range delta.inverseTargets {
		value, exists := index.nodes[target]
		if !exists {
			continue
		}

		specs, err := relationSpecs(target.typ)
		if err != nil {
			continue
		}

		for _, spec := range specs {
			if spec.kind != inverseRelation {
				continue
			}

			holders := index.inverseHolders(target, spec)
			setIndexedRelationField(value.Elem().Field(spec.fieldIndex), holders, index.nodes)
		}
	}
}

func setIndexedRelationField(field reflect.Value, targets []nodeKey, nodes map[nodeKey]reflect.Value) {
	if field.Kind() != reflect.Slice {
		if len(targets) == 0 {
			field.SetZero()
			return
		}

		field.Set(nodes[targets[0]])
		return
	}

	wired := reflect.MakeSlice(field.Type(), len(targets), len(targets))
	for i, target := range targets {
		wired.Index(i).Set(nodes[target])
	}

	field.Set(wired)
}

func (index *committedRelationIndex) inverseHolders(target nodeKey, inverse relationSpec) []nodeKey {
	seen := make(map[nodeKey]struct{})
	for fieldKey, count := range index.incoming[target] {
		field := index.fields[fieldKey]
		if count <= 0 || field.spec.owner != inverse.target || field.spec.fieldName != inverse.matchField {
			continue
		}

		if field.spec.kind != borrowRelation && field.spec.kind != optionRelation {
			continue
		}

		seen[fieldKey.holder] = struct{}{}
	}

	holders := make([]nodeKey, 0, len(seen))
	for holder := range seen {
		holders = append(holders, holder)
	}

	sort.Slice(holders, func(i, j int) bool {
		if holders[i].typ.Name() != holders[j].typ.Name() {
			return holders[i].typ.Name() < holders[j].typ.Name()
		}

		return holders[i].id < holders[j].id
	})

	return holders
}

func buildCommittedRelationIndex(model *relationModel, deleted map[nodeKey]struct{}) *committedRelationIndex {
	nodes := make(map[nodeKey]relationGraphNode, len(model.nodes)-len(deleted))
	values := make(map[nodeKey]reflect.Value, len(model.nodes)-len(deleted))
	for key, node := range model.nodes {
		if _, dies := deleted[key]; dies {
			continue
		}

		nodes[key] = node
		values[key] = node.value
	}

	targets := buildRelationTargetIndexFromFields(nodes, model.targetFields)
	index := &committedRelationIndex{
		nodes: values, targets: targets, targetFields: model.targetFields,
		backFields: make(map[reflect.Type]map[int]struct{}),
		fields:     make(map[relationHolderField]indexedRelationField),
		incoming:   make(map[nodeKey]map[relationHolderField]int),
		owners:     make(map[nodeKey]nodeKey, len(model.owners)),
		children:   make(map[nodeKey]map[nodeKey]struct{}),
	}

	indexRelationFields(index, nodes)
	for _, ref := range model.refs {
		if _, holderDies := deleted[ref.holder]; holderDies {
			continue
		}

		if _, targetDies := deleted[ref.target]; targetDies {
			continue
		}

		fieldKey := relationHolderField{holder: ref.holder, field: ref.spec.fieldIndex}
		field := index.fields[fieldKey]
		field.targets = append(field.targets, ref.target)
		index.fields[fieldKey] = field
		index.incrementIncoming(ref.target, fieldKey)
	}

	for child, owner := range model.owners {
		if _, childDies := deleted[child]; childDies {
			continue
		}

		if _, ownerDies := deleted[owner]; ownerDies {
			continue
		}

		index.owners[child] = owner
		index.addChild(owner, child)
	}

	return index
}

func indexRelationFields(index *committedRelationIndex, nodes map[nodeKey]relationGraphNode) {
	visited := make(map[reflect.Type]struct{})
	for key := range nodes {
		specs, err := relationSpecs(key.typ)
		if err != nil {
			continue
		}

		for _, spec := range specs {
			if spec.kind != inverseRelation {
				fieldKey := relationHolderField{holder: key, field: spec.fieldIndex}
				index.fields[fieldKey] = indexedRelationField{spec: spec}
			}
		}

		if _, found := visited[key.typ]; found {
			continue
		}

		visited[key.typ] = struct{}{}

		for _, spec := range specs {
			if spec.kind != ownRelation || !spec.many {
				continue
			}

			back, found := spec.target.FieldByName(spec.matchField)
			if !found {
				continue
			}

			if index.backFields[spec.target] == nil {
				index.backFields[spec.target] = make(map[int]struct{})
			}

			index.backFields[spec.target][back.Index[0]] = struct{}{}
		}
	}
}

func (index *committedRelationIndex) incrementIncoming(target nodeKey, field relationHolderField) {
	if index.incoming[target] == nil {
		index.incoming[target] = make(map[relationHolderField]int)
	}

	index.incoming[target][field]++
}

func (index *committedRelationIndex) decrementIncoming(target nodeKey, field relationHolderField) {
	fields := index.incoming[target]
	fields[field]--
	if fields[field] <= 0 {
		delete(fields, field)
	}

	if len(fields) == 0 {
		delete(index.incoming, target)
	}
}

func (index *committedRelationIndex) addChild(owner, child nodeKey) {
	if index.children[owner] == nil {
		index.children[owner] = make(map[nodeKey]struct{})
	}

	index.children[owner][child] = struct{}{}
}

func (index *committedRelationIndex) removeChild(owner, child nodeKey) {
	delete(index.children[owner], child)
	if len(index.children[owner]) == 0 {
		delete(index.children, owner)
	}
}
