package nestory

import (
	"cmp"
	"fmt"
	"maps"
	"reflect"
	"slices"
)

type relationHolderField struct {
	holder nodeKey
	field  int
}

// incomingCounts holds the (holder field → multiplicity) pairs pointing at one
// node. A map per node costs about five times this shape: Go allocates a whole
// group of slots on first insert, and most nodes are referenced from one or two
// fields, so nearly all of that group is paid for and never used.
//
// lookup only materializes once a node's fan-in makes the linear scan worth
// avoiding — the same trade nodeKeySet makes above eight keys. Without it a
// lookup table with 60,000 referents would make index construction quadratic.
type incomingCounts struct {
	entries []incomingCount
	lookup  map[relationHolderField]int // field → position in entries
}

type incomingCount struct {
	field relationHolderField
	count int
}

const incomingLookupThreshold = 8

func (counts *incomingCounts) position(field relationHolderField) int {
	if counts.lookup != nil {
		if position, ok := counts.lookup[field]; ok {
			return position
		}

		return -1
	}

	for position := range counts.entries {
		if counts.entries[position].field == field {
			return position
		}
	}

	return -1
}

func (counts *incomingCounts) increment(field relationHolderField) {
	if position := counts.position(field); position >= 0 {
		counts.entries[position].count++

		return
	}

	counts.entries = append(counts.entries, incomingCount{field: field, count: 1})
	if counts.lookup != nil {
		counts.lookup[field] = len(counts.entries) - 1

		return
	}

	if len(counts.entries) > incomingLookupThreshold {
		counts.lookup = make(map[relationHolderField]int, len(counts.entries))
		for position, entry := range counts.entries {
			counts.lookup[entry.field] = position
		}
	}
}

// decrement removes an exhausted entry by swapping the last one into its slot,
// so the entries slice never leaves holes for the readers to skip.
func (counts *incomingCounts) decrement(field relationHolderField) {
	position := counts.position(field)
	if position < 0 {
		return
	}

	counts.entries[position].count--
	if counts.entries[position].count > 0 {
		return
	}

	last := len(counts.entries) - 1
	moved := counts.entries[last]
	counts.entries[position] = moved
	counts.entries = counts.entries[:last]

	if counts.lookup != nil {
		delete(counts.lookup, field)
		if position != last {
			counts.lookup[moved.field] = position
		}
	}
}

type indexedRelationField struct {
	spec    *relationSpec
	targets []nodeKey
}

type relationFieldDelta struct {
	key   relationHolderField
	field indexedRelationField
}

type relationFieldDeltas struct {
	values    []relationFieldDelta
	positions map[relationHolderField]int
}

func (fields *relationFieldDeltas) init(inline []relationFieldDelta, capacity int) {
	if capacity <= cap(inline) {
		fields.values = inline[:0]
		return
	}

	fields.values = make([]relationFieldDelta, 0, capacity)
}

func (fields *relationFieldDeltas) add(key relationHolderField, field indexedRelationField) {
	fields.values = append(fields.values, relationFieldDelta{key: key, field: field})
	if len(fields.values) == 9 {
		fields.positions = make(map[relationHolderField]int, len(fields.values))
		for index, entry := range fields.values {
			fields.positions[entry.key] = index
		}

		return
	}

	if fields.positions != nil {
		fields.positions[key] = len(fields.values) - 1
	}
}

func (fields *relationFieldDeltas) find(key relationHolderField) (*indexedRelationField, bool) {
	if fields.positions != nil {
		index, found := fields.positions[key]
		if !found {
			return nil, false
		}

		return &fields.values[index].field, true
	}

	for index := range fields.values {
		if fields.values[index].key == key {
			return &fields.values[index].field, true
		}
	}

	return nil, false
}

type committedRelationIndex struct {
	nodes        map[nodeKey]reflect.Value
	targets      map[relationTargetKey]indexedRelationTarget
	targetFields map[reflect.Type]map[string]struct{}
	backFields   map[reflect.Type]map[int]struct{}
	fields       map[relationHolderField]indexedRelationField
	incoming     map[nodeKey]incomingCounts
	owners       map[nodeKey]nodeKey
}

type relationOwnerChange struct {
	before    nodeKey
	hadBefore bool
	after     nodeKey
	hasAfter  bool
}

type relationOwnerDelta struct {
	child  nodeKey
	change relationOwnerChange
}

type nodeKeySet struct {
	inline   [8]nodeKey
	overflow []nodeKey
	index    map[nodeKey]struct{}
	count    int
}

func (keys *nodeKeySet) add(key nodeKey) bool {
	if keys.index != nil {
		if _, exists := keys.index[key]; exists {
			return false
		}

		keys.index[key] = struct{}{}
		keys.overflow = append(keys.overflow, key)
		keys.count++
		return true
	}

	for index := 0; index < keys.count; index++ {
		if keys.inline[index] == key {
			return false
		}
	}

	if keys.count < len(keys.inline) {
		keys.inline[keys.count] = key
		keys.count++
		return true
	}

	keys.index = make(map[nodeKey]struct{}, keys.count+1)
	for _, existing := range keys.inline {
		keys.index[existing] = struct{}{}
	}

	keys.index[key] = struct{}{}
	keys.overflow = append(keys.overflow, key)
	keys.count++

	return true
}

func (keys *nodeKeySet) at(index int) nodeKey {
	if index < len(keys.inline) {
		return keys.inline[index]
	}

	return keys.overflow[index-len(keys.inline)]
}

type relationIndexDelta struct {
	index          *committedRelationIndex
	created        map[nodeKey]createdResource
	targets        map[relationTargetKey]indexedRelationTarget
	overrides      []relationIndexOverride
	overrideInline [2]relationIndexOverride
	fields         relationFieldDeltas
	fieldInline    [2]relationFieldDelta
	ownerChanges   []relationOwnerDelta
	ownerPositions map[nodeKey]int
	ownerInline    [1]relationOwnerDelta
	inverseTargets map[nodeKey]struct{}
}

type relationIndexOverride struct {
	node  relationGraphNode
	specs []relationSpec
}

func buildRelationIndexDelta(resources []touchedResource) (*relationIndexDelta, error) {
	return buildRelationIndexDeltaWithCreates(resources, nil)
}

func buildRelationCreateIndexDelta(
	resources []touchedResource,
	creates map[nodeKey]createdResource,
) (*relationIndexDelta, error) {
	return buildRelationIndexDeltaWithCreates(resources, creates)
}

func buildRelationIndexDeltaWithCreates(
	resources []touchedResource,
	creates map[nodeKey]createdResource,
) (*relationIndexDelta, error) {
	index := committedRelationIndexSnapshot()
	if index == nil {
		return nil, nil
	}

	delta := &relationIndexDelta{index: index, created: creates}
	overrides := delta.overrideInline[:0]
	if len(resources)+len(creates) > cap(overrides) {
		overrides = make([]relationIndexOverride, 0, len(resources)+len(creates))
	}

	fieldCount := 0
	for _, resource := range resources {
		runtime, ok := baseRegistry[resource.dbName].(relationRuntime)
		if !ok {
			return nil, nil
		}

		key := nodeKey{typ: runtime.relationType(), id: resource.id}
		if _, exists := index.nodes[key]; !exists {
			return nil, nil
		}

		if relationIndexKeyChanged(index, key, reflect.ValueOf(resource.work)) {
			return nil, nil
		}

		specs, err := relationSpecs(key.typ)
		if err != nil {
			return nil, err
		}

		overrides = append(overrides, relationIndexOverride{
			node:  relationGraphNode{key: key, value: reflect.ValueOf(resource.work)},
			specs: specs,
		})
		fieldCount += len(specs)
	}

	supported, err := delta.indexCreatedTargets(creates)
	if err != nil {
		return nil, err
	}

	if !supported {
		return nil, nil
	}

	for key, created := range creates {
		if _, exists := index.nodes[key]; exists {
			return nil, nil
		}

		specs, err := relationSpecs(key.typ)
		if err != nil {
			return nil, err
		}

		overrides = append(overrides, relationIndexOverride{
			node: relationGraphNode{key: key, value: created.work}, specs: specs,
		})
		fieldCount += len(specs)
	}

	delta.overrides = overrides
	delta.fields.init(delta.fieldInline[:], fieldCount)
	delta.ownerChanges = delta.ownerInline[:0]
	for _, override := range overrides {
		key := override.node.key

		for specIndex := range override.specs {
			spec := &override.specs[specIndex]
			if spec.kind == inverseRelation {
				if delta.inverseTargets == nil {
					delta.inverseTargets = make(map[nodeKey]struct{})
				}

				delta.inverseTargets[key] = struct{}{}
				continue
			}

			fieldKey := relationHolderField{holder: key, field: spec.fieldIndex}
			committed, exists := index.fields[fieldKey]
			if !exists {
				if _, created := creates[key]; created {
					committed = indexedRelationField{spec: spec}
				} else {
					return nil, nil
				}
			}

			if committed.spec == nil {
				return nil, nil
			}

			replacement := indexedRelationField{spec: spec}
			if len(committed.targets) == 0 {
				// An empty committed view cannot observe writes to its spare capacity.
				replacement.targets = committed.targets[:0]
			}

			delta.fields.add(fieldKey, replacement)
			field, _ := delta.fields.find(fieldKey)
			if err := scanIndexedRelationField(delta, override.node, field); err != nil {
				return nil, err
			}
		}
	}

	if err := delta.validateOwnership(); err != nil {
		return nil, err
	}

	if !delta.supportsOwnershipChanges() {
		return nil, nil
	}

	delta.collectInverseTargets()

	return delta, nil
}

func (delta *relationIndexDelta) indexCreatedTargets(creates map[nodeKey]createdResource) (bool, error) {
	if len(creates) == 0 {
		return true, nil
	}

	for key := range creates {
		specs, err := relationSpecs(key.typ)
		if err != nil {
			return false, err
		}

		for _, spec := range specs {
			if spec.kind == inverseRelation {
				continue
			}

			field := spec.matchField
			if spec.kind == ownRelation && spec.many {
				field = entityIDField
			}

			if _, indexed := delta.index.targetFields[spec.target][field]; !indexed {
				return false, nil
			}
		}
	}

	delta.targets = make(map[relationTargetKey]indexedRelationTarget, len(creates))
	for key, created := range creates {
		for field := range delta.index.targetFields[key.typ] {
			value, present := relationKey(created.work, field)
			if !present {
				continue
			}

			lookup := relationTargetKey{typ: key.typ, field: field, value: value.Interface()}
			if _, duplicate := delta.index.targets[lookup]; duplicate {
				return false, fmt.Errorf("%w: duplicate relation target %s.%s", ErrRelationInvariant, key.typ, field)
			}

			if _, duplicate := delta.targets[lookup]; duplicate {
				return false, fmt.Errorf("%w: duplicate relation target %s.%s", ErrRelationInvariant, key.typ, field)
			}

			delta.targets[lookup] = indexedRelationTarget{key: key}
		}
	}

	return true, nil
}

func (delta *relationIndexDelta) bindCreatedNode(key nodeKey, live reflect.Value) {
	created := delta.created[key]
	created.work = live
	delta.created[key] = created
}

func scanIndexedRelationField(delta *relationIndexDelta, holder relationGraphNode, field *indexedRelationField) error {
	value := holder.value.Elem().Field(field.spec.fieldIndex)
	if !field.spec.many {
		target, found, err := resolveIndexedRelationTarget(delta, holder.key, value, *field.spec)
		if found {
			field.targets = append(field.targets, target)
		}

		return err
	}

	var seen map[nodeKey]struct{}
	if field.spec.kind == ownRelation && value.Len() > 1 {
		seen = make(map[nodeKey]struct{}, value.Len())
	}

	for position := range value.Len() {
		target, found, err := resolveIndexedRelationTarget(delta, holder.key, value.Index(position), *field.spec)
		if err != nil {
			return err
		}

		if !found {
			continue
		}

		if seen != nil {
			if _, duplicate := seen[target]; duplicate {
				return fmt.Errorf(
					"%w: %s.%s contains owned child %s more than once",
					ErrRelationInvariant, holder.key.typ, field.spec.fieldName, target,
				)
			}

			seen[target] = struct{}{}
		}

		field.targets = append(field.targets, target)
	}

	return nil
}

func resolveIndexedRelationTarget(
	delta *relationIndexDelta,
	holder nodeKey,
	pointer reflect.Value,
	spec relationSpec,
) (nodeKey, bool, error) {
	lookup, present := graphRelationTargetKey(spec, pointer)
	target, found := delta.targets[lookup]
	if !found {
		target, found = delta.index.targets[lookup]
	}

	if target.duplicate {
		return nodeKey{}, false, fmt.Errorf("%w: %s.%s does not uniquely identify a target", ErrRelationInvariant, spec.owner, spec.fieldName)
	}

	if !present || !found {
		if relationMayBeMissing(spec) {
			return nodeKey{}, false, nil
		}

		return nodeKey{}, false, fmt.Errorf(
			"%w: required relation %s.%s on %s is nil or has no live target",
			ErrRelationInvariant, holder.typ, spec.fieldName, holder,
		)
	}

	return target.key, true, nil
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

func (delta *relationIndexDelta) materializeOwnBackReferences(
	transactionResources []touchedResource,
	changed []touchedResource,
	creates map[nodeKey]createdResource,
) ([]touchedResource, error) {
	for _, entry := range delta.fields.values {
		if entry.field.spec.kind != ownRelation || !entry.field.spec.many {
			continue
		}

		owner := delta.overrideValue(entry.key.holder)
		for _, target := range entry.field.targets {
			if created, exists := creates[target]; exists {
				if !indexedOwnBackReferenceMatches(owner, created.work, *entry.field.spec) {
					setIndexedOwnBackReference(owner, created.work, *entry.field.spec)
				}

				continue
			}

			resource, alreadyChanged, err := delta.ownershipTargetResource(target, owner, *entry.field.spec, transactionResources, changed)
			if err != nil {
				return nil, err
			}

			if resource.work == nil || indexedOwnBackReferenceMatches(owner, reflect.ValueOf(resource.work), *entry.field.spec) {
				continue
			}

			setIndexedOwnBackReference(owner, reflect.ValueOf(resource.work), *entry.field.spec)
			if !alreadyChanged {
				changed = append(changed, resource)
			}
		}
	}

	return changed, nil
}

func (delta *relationIndexDelta) ownershipTargetResource(
	target nodeKey,
	owner reflect.Value,
	spec relationSpec,
	transactionResources, changed []touchedResource,
) (touchedResource, bool, error) {
	if resource, found := indexedTouchedResource(changed, target); found {
		return resource, true, nil
	}

	if resource, found := indexedTouchedResource(transactionResources, target); found {
		return resource, false, nil
	}

	live := delta.index.nodes[target]
	if indexedOwnBackReferenceMatches(owner, live, spec) {
		return touchedResource{}, false, nil
	}

	resource, err := snapshotTouchedResource(target)

	return resource, false, err
}

func (delta *relationIndexDelta) overrideValue(key nodeKey) reflect.Value {
	for _, override := range delta.overrides {
		if override.node.key == key {
			return override.node.value
		}
	}

	return delta.index.nodes[key]
}

func indexedTouchedResource(resources []touchedResource, key nodeKey) (touchedResource, bool) {
	name := key.typ.Name()
	for _, resource := range resources {
		if resource.dbName == name && resource.id == key.id {
			return resource, true
		}
	}

	return touchedResource{}, false
}

func indexedOwnBackReferenceMatches(owner, child reflect.Value, spec relationSpec) bool {
	back := child.Elem().FieldByName(spec.matchField)
	if back.Kind() == reflect.Pointer {
		return !back.IsNil() && back.Pointer() == owner.Pointer()
	}

	id := owner.Elem().FieldByName(entityIDField)

	return scalarEqual(back, id)
}

func setIndexedOwnBackReference(owner, child reflect.Value, spec relationSpec) {
	back := child.Elem().FieldByName(spec.matchField)
	if back.Kind() == reflect.Pointer {
		back.Set(owner)
		return
	}

	back.Set(owner.Elem().FieldByName(entityIDField))
}

func (delta *relationIndexDelta) validateOwnership() error {
	var affected nodeKeySet
	for _, entry := range delta.fields.values {
		fieldKey, field := entry.key, entry.field
		old := delta.index.fields[fieldKey]
		if field.spec.kind == ownRelation {
			for _, target := range old.targets {
				affected.add(target)
			}

			for _, target := range field.targets {
				affected.add(target)
			}
		}

		if field.spec.kind == ownedByRelation {
			affected.add(fieldKey.holder)
		}
	}

	for index := 0; index < affected.count; index++ {
		child := affected.at(index)
		raw, incomingCount := delta.effectiveIncomingOwn(child)
		back, hasBack := delta.effectiveOwnedBy(child)
		if incomingCount > 1 {
			return fmt.Errorf("%w: %s has more than one owner", ErrRelationInvariant, child)
		}

		if incomingCount == 1 && hasBack && raw.owner != back.owner {
			return fmt.Errorf("%w: own and ownedby disagree for %s (%s vs %s)", ErrRelationInvariant, child, raw.owner, back.owner)
		}

		after, hasAfter := raw.owner, incomingCount == 1
		if !hasAfter && hasBack {
			after, hasAfter = back.owner, true
		}

		before, hadBefore := delta.index.owners[child]
		if before == after && hadBefore == hasAfter {
			continue
		}

		delta.setOwnerChange(child, relationOwnerChange{
			before: before, hadBefore: hadBefore, after: after, hasAfter: hasAfter,
		})
	}

	for index := 0; index < affected.count; index++ {
		child := affected.at(index)
		if err := delta.validateOwnerChain(child); err != nil {
			return err
		}
	}

	return nil
}

func (delta *relationIndexDelta) setOwnerChange(child nodeKey, change relationOwnerChange) {
	if delta.ownerPositions != nil {
		if index, exists := delta.ownerPositions[child]; exists {
			delta.ownerChanges[index].change = change
			return
		}
	}

	delta.ownerChanges = append(delta.ownerChanges, relationOwnerDelta{child: child, change: change})
	if len(delta.ownerChanges) == 9 {
		delta.ownerPositions = make(map[nodeKey]int, len(delta.ownerChanges))
		for index, entry := range delta.ownerChanges {
			delta.ownerPositions[entry.child] = index
		}

		return
	}

	if delta.ownerPositions != nil {
		delta.ownerPositions[child] = len(delta.ownerChanges) - 1
	}
}

func (delta *relationIndexDelta) ownerChange(child nodeKey) (relationOwnerChange, bool) {
	if delta.ownerPositions != nil {
		index, found := delta.ownerPositions[child]
		if !found {
			return relationOwnerChange{}, false
		}

		return delta.ownerChanges[index].change, true
	}

	for _, entry := range delta.ownerChanges {
		if entry.child == child {
			return entry.change, true
		}
	}

	return relationOwnerChange{}, false
}

func (delta *relationIndexDelta) effectiveIncomingOwn(child nodeKey) (incomingOwn, int) {
	var first incomingOwn
	count := 0
	for _, entry := range delta.index.incoming[child].entries {
		fieldKey, multiplicity := entry.field, entry.count
		if _, replaced := delta.fields.find(fieldKey); replaced {
			continue
		}

		field := delta.index.fields[fieldKey]
		if field.spec.kind == ownRelation && multiplicity > 0 {
			if count == 0 {
				first = incomingOwn{owner: fieldKey.holder, spec: field.spec}
			}

			count += multiplicity
		}
	}

	for _, entry := range delta.fields.values {
		fieldKey, field := entry.key, entry.field
		if field.spec.kind != ownRelation {
			continue
		}

		for _, target := range field.targets {
			if target == child {
				if count == 0 {
					first = incomingOwn{owner: fieldKey.holder, spec: field.spec}
				}

				count++
			}
		}
	}

	return first, count
}

func (delta *relationIndexDelta) effectiveOwnedBy(child nodeKey) (incomingOwn, bool) {
	specs, err := relationSpecs(child.typ)
	if err != nil {
		return incomingOwn{}, false
	}

	for index := range specs {
		spec := &specs[index]
		if spec.kind != ownedByRelation {
			continue
		}

		fieldKey := relationHolderField{holder: child, field: spec.fieldIndex}
		field, changed := delta.fields.find(fieldKey)
		if !changed {
			indexed := delta.index.fields[fieldKey]
			field = &indexed
		}

		if len(field.targets) == 1 {
			return incomingOwn{owner: field.targets[0], spec: spec}, true
		}
	}

	return incomingOwn{}, false
}

func (delta *relationIndexDelta) validateOwnerChain(start nodeKey) error {
	slow := start
	fast := start
	for {
		var found bool
		slow, found = delta.effectiveOwner(slow)
		if !found {
			return nil
		}

		fast, found = delta.effectiveOwner(fast)
		if !found {
			return nil
		}

		fast, found = delta.effectiveOwner(fast)
		if !found {
			return nil
		}

		if slow == fast {
			return fmt.Errorf("%w: ownership cycle involving %s", ErrRelationInvariant, slow)
		}
	}
}

func (delta *relationIndexDelta) effectiveOwner(child nodeKey) (nodeKey, bool) {
	if changed, exists := delta.ownerChange(child); exists {
		return changed.after, changed.hasAfter
	}

	owner, found := delta.index.owners[child]
	return owner, found
}

func (delta *relationIndexDelta) supportsOwnershipChanges() bool {
	for _, entry := range delta.ownerChanges {
		change := entry.change
		if change.hadBefore && !change.hasAfter {
			return false
		}
	}

	return true
}

func (delta *relationIndexDelta) collectInverseTargets() {
	for _, entry := range delta.fields.values {
		fieldKey, field := entry.key, entry.field
		if field.spec.kind != borrowRelation && field.spec.kind != optionRelation {
			continue
		}

		if delta.inverseTargets == nil {
			delta.inverseTargets = make(map[nodeKey]struct{})
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

	for key, created := range delta.created {
		index.nodes[key] = created.work
	}

	for lookup, target := range delta.targets {
		index.targets[lookup] = target
	}

	for _, entry := range delta.fields.values {
		fieldKey, replacement := entry.key, entry.field
		old := index.fields[fieldKey]
		for _, target := range old.targets {
			index.decrementIncoming(target, fieldKey)
		}

		if len(replacement.targets) == 0 && cap(replacement.targets) == 0 {
			// Keep the removed edge's buffer for a later move back to this field.
			replacement.targets = old.targets[:0]
		}

		index.fields[fieldKey] = replacement
		for _, target := range replacement.targets {
			index.incrementIncoming(target, fieldKey)
		}
	}

	for _, entry := range delta.ownerChanges {
		child, change := entry.child, entry.change
		if change.hadBefore {
			removeCommittedChild(change.before, child)
			delete(index.owners, child)
		}

		if change.hasAfter {
			index.owners[child] = change.after
			addCommittedChild(change.after, child)
		}
	}

	committedOwnership.graph = nil
	clear(committedOwnership.branches)
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
	for _, entry := range delta.fields.values {
		fieldKey, indexed := entry.key, entry.field
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

	var wired reflect.Value
	if field.Cap() >= len(targets) {
		wired = field.Slice(0, len(targets))
	} else {
		wired = reflect.MakeSlice(field.Type(), len(targets), len(targets))
	}

	for i, target := range targets {
		wired.Index(i).Set(nodes[target])
	}

	field.Set(wired)
}

func (index *committedRelationIndex) inverseHolders(target nodeKey, inverse relationSpec) []nodeKey {
	seen := make(map[nodeKey]struct{})
	for _, entry := range index.incoming[target].entries {
		fieldKey, count := entry.field, entry.count
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

	slices.SortFunc(holders, func(a, b nodeKey) int {
		if byName := cmp.Compare(a.typ.Name(), b.typ.Name()); byName != 0 {
			return byName
		}

		return cmp.Compare(a.id, b.id)
	})

	return holders
}

func buildCommittedRelationIndex(model *relationModel, deleted map[nodeKey]struct{}) *committedRelationIndex {
	// With nothing deleted the surviving node set is model.nodes itself, so the
	// filtered copy this used to build was a duplicate map per node. Reading it
	// is safe: nothing writes through the nodes local, and index.nodes below is a
	// separate map of a different type.
	//
	// model.targets was already derived from exactly these nodes and these
	// targetFields, so recomputing it walked every node and every target field a
	// second time for an identical result. It cannot be shared, though —
	// publishAndRewire assigns into index.targets when a create is published, and
	// that would write into the model committedOwnership.graph still holds. A map
	// clone is the cheap half of the old work and keeps the two independent.
	nodes, targets := model.nodes, maps.Clone(model.targets)
	if len(deleted) > 0 {
		nodes = make(map[nodeKey]relationGraphNode, len(model.nodes)-len(deleted))
		for key, node := range model.nodes {
			if _, dies := deleted[key]; dies {
				continue
			}

			nodes[key] = node
		}

		targets = buildRelationTargetIndexFromFields(nodes, model.targetFields)
	}

	if targets == nil {
		targets = make(map[relationTargetKey]indexedRelationTarget)
	}

	values := make(map[nodeKey]reflect.Value, len(nodes))
	for key, node := range nodes {
		values[key] = node.value
	}

	index := &committedRelationIndex{
		nodes: values, targets: targets, targetFields: model.targetFields,
		backFields: make(map[reflect.Type]map[int]struct{}),
		fields:     make(map[relationHolderField]indexedRelationField),
		incoming:   make(map[nodeKey]incomingCounts),
		owners:     make(map[nodeKey]nodeKey, len(model.owners)),
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

		for specIndex := range specs {
			spec := &specs[specIndex]
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
	counts := index.incoming[target]
	counts.increment(field)
	index.incoming[target] = counts
}

func (index *committedRelationIndex) decrementIncoming(target nodeKey, field relationHolderField) {
	counts, ok := index.incoming[target]
	if !ok {
		return
	}

	counts.decrement(field)

	// Keep an empty bucket: repeated repoints commonly return to this target.
	index.incoming[target] = counts
}
