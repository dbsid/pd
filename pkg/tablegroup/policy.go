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
	groupID := group.GetIdentity().GetTableGroupId()
	previous := p.groups[groupID]
	if previous != nil && previous.GetIdentity().GetKeyspaceId() != group.GetIdentity().GetKeyspaceId() {
		return operationConflict(group.GetIdentity(), nil, "Table Group keyspace identity cannot change")
	}
	regionID := group.GetRegionBinding().GetRegionId()
	if existingGroupID, ok := p.regions[regionID]; ok && existingGroupID != groupID {
		return alreadyExists("Region is already bound to another Table Group", group.GetIdentity())
	}
	if previous != nil && previous.GetRegionBinding().GetRegionId() != regionID {
		delete(p.regions, previous.GetRegionBinding().GetRegionId())
	}
	p.groups[groupID] = cloneGroup(group)
	p.regions[regionID] = groupID
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
	delete(p.regions, group.GetRegionBinding().GetRegionId())
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

func regionMatchesGroup(region *metapb.Region, group *table_grouppb.TableGroup, requireAppliedVersion bool) bool {
	if region == nil || group == nil || group.GetIdentity() == nil || group.GetRegionBinding() == nil {
		return false
	}
	binding := group.GetRegionBinding()
	if region.GetId() != binding.GetRegionId() || !proto.Equal(region.GetRegionEpoch(), binding.GetRegionEpoch()) {
		return false
	}
	bound := keyspace.MakeRegionBound(group.GetIdentity().GetKeyspaceId())
	if !bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) || !bytes.Equal(region.GetEndKey(), bound.TxnRightBound) {
		return false
	}
	mirror := region.GetTableGroup()
	if mirror == nil || mirror.GetKeyspaceId() != group.GetIdentity().GetKeyspaceId() ||
		mirror.GetTableGroupId() != group.GetIdentity().GetTableGroupId() {
		return false
	}
	if requireAppliedVersion && (mirror.GetAppliedMetadataVersion() == 0 ||
		mirror.GetAppliedMetadataVersion() < binding.GetAppliedMetadataVersion() ||
		mirror.GetAppliedMetadataVersion() > group.GetMetadataVersion()) {
		return false
	}
	return true
}

// SplitPolicyProvider supplies an authoritative or watched Table Group split decision.
type SplitPolicyProvider interface {
	EnsureTableGroupSplitAllowed(region *metapb.Region, source table_grouppb.SplitSource) error
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
	leftSpec.RegionBinding.AppliedMetadataVersion = 0
	rightSpec.RegionBinding.AppliedMetadataVersion = 0
	return proto.Equal(leftSpec, rightSpec)
}

func equalObservedStatus(left, right *table_grouppb.TableGroup) bool {
	return left.GetRegionBinding().GetAppliedMetadataVersion() == right.GetRegionBinding().GetAppliedMetadataVersion() &&
		proto.Equal(left.GetCapacityStatus(), right.GetCapacityStatus())
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
	if !regionMatchesGroup(region, group, true) {
		return regionMismatch(group.GetIdentity(), "Region does not match Table Group authority")
	}
	return nil
}

func cloneGroup(group *table_grouppb.TableGroup) *table_grouppb.TableGroup {
	if group == nil {
		return nil
	}
	return proto.Clone(group).(*table_grouppb.TableGroup)
}
