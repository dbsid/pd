// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tablegroup

import (
	"bytes"
	"sort"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

type protectedRange struct {
	start   []byte
	end     []byte
	groupID uint64
}

type policyIndex struct {
	groups  map[uint64]*table_grouppb.TableGroup
	regions map[uint64]uint64
	ranges  []protectedRange
}

func newPolicyIndex() policyIndex {
	return policyIndex{
		groups:  make(map[uint64]*table_grouppb.TableGroup),
		regions: make(map[uint64]uint64),
	}
}

func (p *policyIndex) upsert(group *table_grouppb.TableGroup) error {
	if err := validateStoredGroup(group); err != nil {
		return err
	}
	bindings, err := fragmentBindingsForGroup(group)
	if err != nil {
		return err
	}
	groupID := group.GetIdentity().GetTableGroupId()
	previous := p.groups[groupID]
	if previous != nil && previous.GetIdentity().GetKeyspaceId() != group.GetIdentity().GetKeyspaceId() {
		return operationConflict(group.GetIdentity(), nil, "Table Group keyspace identity cannot change")
	}
	for _, fragment := range bindings {
		if existingGroupID, ok := p.regions[fragment.binding.GetRegionId()]; ok && existingGroupID != groupID {
			return alreadyExists("Region is already bound to another Table Group", group.GetIdentity())
		}
	}
	if previous != nil {
		previousBindings, err := fragmentBindingsForGroup(previous)
		if err != nil {
			return err
		}
		for _, fragment := range previousBindings {
			delete(p.regions, fragment.binding.GetRegionId())
		}
	}
	p.groups[groupID] = cloneGroup(group)
	for _, fragment := range bindings {
		p.regions[fragment.binding.GetRegionId()] = groupID
	}
	if previous == nil {
		p.rebuildRanges()
	}
	return nil
}

func (p *policyIndex) remove(groupID uint64) {
	group := p.groups[groupID]
	if group == nil {
		return
	}
	bindings, err := fragmentBindingsForGroup(group)
	if err != nil {
		return
	}
	for _, fragment := range bindings {
		delete(p.regions, fragment.binding.GetRegionId())
	}
	delete(p.groups, groupID)
	p.rebuildRanges()
}

func (p *policyIndex) rebuildRanges() {
	p.ranges = make([]protectedRange, 0, len(p.groups))
	for groupID, group := range p.groups {
		bound := keyspace.MakeRegionBound(group.GetIdentity().GetKeyspaceId())
		p.ranges = append(p.ranges, protectedRange{
			start:   bytes.Clone(bound.TxnLeftBound),
			end:     bytes.Clone(bound.TxnRightBound),
			groupID: groupID,
		})
	}
	sort.Slice(p.ranges, func(i, j int) bool {
		return bytes.Compare(p.ranges[i].start, p.ranges[j].start) < 0
	})
}

func (p *policyIndex) groupForRegion(region *metapb.Region) *table_grouppb.TableGroup {
	if region == nil {
		return nil
	}
	if groupID, ok := p.regions[region.GetId()]; ok {
		return p.groups[groupID]
	}
	if groupID := p.overlappingGroupID(region.GetStartKey(), region.GetEndKey()); groupID != 0 {
		return p.groups[groupID]
	}
	return nil
}

func (p *policyIndex) overlappingGroupID(start, end []byte) uint64 {
	i := sort.Search(len(p.ranges), func(i int) bool {
		return bytes.Compare(p.ranges[i].end, start) > 0
	})
	if i == len(p.ranges) {
		return 0
	}
	protected := p.ranges[i]
	if len(end) != 0 && bytes.Compare(end, protected.start) <= 0 {
		return 0
	}
	return protected.groupID
}

func (p *policyIndex) ensureSplitAllowed(region *metapb.Region, source table_grouppb.SplitSource) error {
	if source == table_grouppb.SplitSource_SPLIT_SOURCE_UNSPECIFIED {
		return invalidArgument("split source must be specified")
	}
	if group := p.groupForRegion(region); group != nil {
		reason := table_grouppb.SplitRejectionReason_SPLIT_REJECTION_REASON_POLICY_FORBIDS_SPLIT
		if !regionMatchesGroup(region, group, false) {
			reason = table_grouppb.SplitRejectionReason_SPLIT_REJECTION_REASON_REGION_BINDING_MISMATCH
		}
		return splitForbidden(group.GetIdentity(), source, reason, "Table Group policy forbids Region split")
	}
	return ensureMirrorSplitAllowed(region, source)
}

func (p *policyIndex) ensureMergeAllowed(source, target *metapb.Region) error {
	if source == nil || target == nil {
		return invalidArgument("missing Region for merge")
	}
	if group := p.groupForRegion(source); group != nil {
		return operationConflict(group.GetIdentity(), nil, "Table Group policy forbids Region merge")
	}
	if group := p.groupForRegion(target); group != nil {
		return operationConflict(group.GetIdentity(), nil, "Table Group policy forbids Region merge")
	}
	return ensureMirrorMergeAllowed(source, target)
}

func ensureMirrorSplitAllowed(region *metapb.Region, source table_grouppb.SplitSource) error {
	if region == nil {
		return invalidArgument("missing Region for split")
	}
	mirror := region.GetTableGroup()
	if mirror == nil {
		return nil
	}
	identity := &table_grouppb.TableGroupIdentity{
		KeyspaceId:   mirror.GetKeyspaceId(),
		TableGroupId: mirror.GetTableGroupId(),
	}
	reason := table_grouppb.SplitRejectionReason_SPLIT_REJECTION_REASON_POLICY_FORBIDS_SPLIT
	if mirror.GetTableGroupId() == 0 || mirror.GetAppliedMetadataVersion() == 0 {
		reason = table_grouppb.SplitRejectionReason_SPLIT_REJECTION_REASON_REGION_BINDING_MISMATCH
	}
	return splitForbidden(identity, source, reason, "Table Group Region mirror forbids split")
}

func ensureMirrorMergeAllowed(source, target *metapb.Region) error {
	if source == nil || target == nil {
		return invalidArgument("missing Region for merge")
	}
	for _, region := range []*metapb.Region{source, target} {
		mirror := region.GetTableGroup()
		if mirror == nil {
			continue
		}
		identity := &table_grouppb.TableGroupIdentity{
			KeyspaceId:   mirror.GetKeyspaceId(),
			TableGroupId: mirror.GetTableGroupId(),
		}
		return operationConflict(identity, nil, "Table Group Region mirror forbids merge")
	}
	return nil
}

func regionMatchesGroup(region *metapb.Region, group *table_grouppb.TableGroup, requireAppliedVersion bool) bool {
	if region == nil || group == nil || group.GetIdentity() == nil {
		return false
	}
	fragment, ok := findFragmentBinding(group, region.GetId())
	if !ok || !proto.Equal(region.GetRegionEpoch(), fragment.binding.GetRegionEpoch()) {
		return false
	}
	return regionMatchesFragment(region, group, fragment, requireAppliedVersion)
}

func regionMatchesGroupObservation(
	region *metapb.Region,
	group *table_grouppb.TableGroup,
	requireAppliedVersion bool,
) bool {
	if region == nil || group == nil || group.GetIdentity() == nil {
		return false
	}
	fragment, ok := findFragmentBinding(group, region.GetId())
	if !ok || !regionEpochFollowsBinding(region.GetRegionEpoch(), fragment.binding.GetRegionEpoch()) {
		return false
	}
	return regionMatchesFragment(region, group, fragment, requireAppliedVersion)
}

// Region version is immutable because Table Groups forbid splits. ConfVer may
// advance when PD moves peers without changing the fragment's key range.
func regionEpochFollowsBinding(observed, binding *metapb.RegionEpoch) bool {
	return observed != nil && binding != nil &&
		observed.GetVersion() == binding.GetVersion() &&
		observed.GetConfVer() >= binding.GetConfVer()
}

func regionMatchesFragment(
	region *metapb.Region,
	group *table_grouppb.TableGroup,
	fragment fragmentBinding,
	requireAppliedVersion bool,
) bool {
	return regionMatchesFragmentRange(region, group) &&
		regionMatchesFragmentMirror(region, group, fragment, requireAppliedVersion)
}

func regionMatchesFragmentRange(region *metapb.Region, group *table_grouppb.TableGroup) bool {
	bound := keyspace.MakeRegionBound(group.GetIdentity().GetKeyspaceId())
	if group.GetPartitioning() == nil {
		if !bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) || !bytes.Equal(region.GetEndKey(), bound.TxnRightBound) {
			return false
		}
	} else if bytes.Compare(region.GetStartKey(), bound.TxnLeftBound) < 0 ||
		bytes.Compare(region.GetEndKey(), bound.TxnRightBound) > 0 ||
		bytes.Compare(region.GetStartKey(), region.GetEndKey()) >= 0 {
		return false
	}
	return true
}

func regionMatchesFragmentMirror(
	region *metapb.Region,
	group *table_grouppb.TableGroup,
	fragment fragmentBinding,
	requireAppliedVersion bool,
) bool {
	mirror := region.GetTableGroup()
	if mirror == nil || mirror.GetKeyspaceId() != group.GetIdentity().GetKeyspaceId() ||
		mirror.GetTableGroupId() != group.GetIdentity().GetTableGroupId() ||
		mirror.GetFragmentId() != fragment.fragmentID {
		return false
	}
	if requireAppliedVersion && (mirror.GetAppliedMetadataVersion() == 0 ||
		mirror.GetAppliedMetadataVersion() < fragment.binding.GetAppliedMetadataVersion() ||
		mirror.GetAppliedMetadataVersion() > group.GetMetadataVersion()) {
		return false
	}
	return true
}

// SplitPolicyProvider supplies an authoritative or watched Table Group split decision.
type SplitPolicyProvider interface {
	EnsureTableGroupSplitAllowed(region *metapb.Region, source table_grouppb.SplitSource) error
}

// MergePolicyProvider supplies an authoritative or watched Table Group merge decision.
type MergePolicyProvider interface {
	EnsureTableGroupMergeAllowed(source, target *metapb.Region) error
}

// SplitSourceFromReason maps the existing PD split reason to the U1 policy source.
func SplitSourceFromReason(reason pdpb.SplitReason) table_grouppb.SplitSource {
	switch reason {
	case pdpb.SplitReason_LOAD:
		return table_grouppb.SplitSource_SPLIT_SOURCE_AUTOMATIC_LOAD
	case pdpb.SplitReason_ADMIN:
		return table_grouppb.SplitSource_SPLIT_SOURCE_MANUAL_REQUEST
	default:
		return table_grouppb.SplitSource_SPLIT_SOURCE_AUTOMATIC_SIZE
	}
}

// EnsureSplitAllowed applies provider policy and always preserves mirror-only fail-closed behavior.
func EnsureSplitAllowed(provider SplitPolicyProvider, region *metapb.Region, source table_grouppb.SplitSource) error {
	if provider != nil {
		return provider.EnsureTableGroupSplitAllowed(region, source)
	}
	return ensureMirrorSplitAllowed(region, source)
}

// EnsureMergeAllowed applies provider policy and always preserves mirror-only fail-closed behavior.
func EnsureMergeAllowed(provider MergePolicyProvider, source, target *metapb.Region) error {
	if provider != nil {
		return provider.EnsureTableGroupMergeAllowed(source, target)
	}
	return ensureMirrorMergeAllowed(source, target)
}

// SplitPolicyRegistry is a read-only watched policy registry for the scheduling service.
type SplitPolicyRegistry struct {
	mu    syncutil.RWMutex
	index policyIndex
}

// NewSplitPolicyRegistry creates an empty scheduling policy registry.
func NewSplitPolicyRegistry() *SplitPolicyRegistry {
	return &SplitPolicyRegistry{index: newPolicyIndex()}
}

// Sync applies a monotonic authoritative snapshot to the registry.
func (r *SplitPolicyRegistry) Sync(group *table_grouppb.TableGroup) error {
	if err := validateStoredGroup(group); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	groupID := group.GetIdentity().GetTableGroupId()
	if current := r.index.groups[groupID]; current != nil {
		if group.GetMetadataVersion() < current.GetMetadataVersion() || group.GetStatusVersion() < current.GetStatusVersion() {
			return staleMetadata(group.GetIdentity(), group.GetMetadataVersion(), current.GetMetadataVersion())
		}
		if group.GetMetadataVersion() == current.GetMetadataVersion() && !equalMetadataSpec(group, current) {
			return operationConflict(group.GetIdentity(), nil, "equal metadata-version Table Group specs differ")
		}
		if group.GetStatusVersion() == current.GetStatusVersion() && !equalObservedStatus(group, current) {
			return operationConflict(group.GetIdentity(), nil, "equal status-version Table Group observations differ")
		}
		if group.GetMetadataVersion() == current.GetMetadataVersion() && group.GetStatusVersion() == current.GetStatusVersion() {
			return nil
		}
	}
	return r.index.upsert(group)
}

func equalMetadataSpec(left, right *table_grouppb.TableGroup) bool {
	leftSpec := cloneGroup(left)
	rightSpec := cloneGroup(right)
	leftSpec.StatusVersion = 0
	rightSpec.StatusVersion = 0
	leftSpec.CapacityStatus = nil
	rightSpec.CapacityStatus = nil
	clearObservedRegionState(leftSpec)
	clearObservedRegionState(rightSpec)
	return proto.Equal(leftSpec, rightSpec)
}

func equalObservedStatus(left, right *table_grouppb.TableGroup) bool {
	leftBindings, leftErr := fragmentBindingsForGroup(left)
	rightBindings, rightErr := fragmentBindingsForGroup(right)
	if leftErr != nil || rightErr != nil || len(leftBindings) != len(rightBindings) {
		return false
	}
	for index := range leftBindings {
		if leftBindings[index].fragmentID != rightBindings[index].fragmentID ||
			!proto.Equal(leftBindings[index].binding.GetRegionEpoch(), rightBindings[index].binding.GetRegionEpoch()) ||
			leftBindings[index].binding.GetAppliedMetadataVersion() != rightBindings[index].binding.GetAppliedMetadataVersion() {
			return false
		}
	}
	return proto.Equal(left.GetCapacityStatus(), right.GetCapacityStatus())
}

func clearObservedRegionState(group *table_grouppb.TableGroup) {
	clearBinding := func(binding *table_grouppb.TableGroupRegionBinding) {
		if binding == nil {
			return
		}
		binding.AppliedMetadataVersion = 0
		if binding.GetRegionEpoch() != nil {
			binding.RegionEpoch.ConfVer = 0
		}
	}
	clearBinding(group.GetRegionBinding())
	for _, fragment := range group.GetFragmentBindings() {
		clearBinding(fragment.GetRegionBinding())
	}
}

// Remove deletes one group from the read-only registry.
func (r *SplitPolicyRegistry) Remove(groupID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.index.remove(groupID)
}

// EnsureTableGroupSplitAllowed implements SplitPolicyProvider.
func (r *SplitPolicyRegistry) EnsureTableGroupSplitAllowed(region *metapb.Region, source table_grouppb.SplitSource) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.index.ensureSplitAllowed(region, source)
}

// EnsureTableGroupMergeAllowed implements MergePolicyProvider.
func (r *SplitPolicyRegistry) EnsureTableGroupMergeAllowed(source, target *metapb.Region) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.index.ensureMergeAllowed(source, target)
}

// ValidateRegion validates a Region against watched Table Group authority.
func (r *SplitPolicyRegistry) ValidateRegion(region *metapb.Region) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	group := r.index.groupForRegion(region)
	if group == nil {
		if region != nil && region.GetTableGroup() != nil {
			return regionMismatch(nil, "Region mirror has no Table Group authority")
		}
		return nil
	}
	return validateRegionSnapshot(region, group)
}

func cloneGroup(group *table_grouppb.TableGroup) *table_grouppb.TableGroup {
	if group == nil {
		return nil
	}
	return proto.Clone(group).(*table_grouppb.TableGroup)
}
