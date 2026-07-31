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
	"context"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/storage/kv"
)

// ValidateRegion validates and reconciles a Region heartbeat before PD caches it.
func (m *Manager) ValidateRegion(ctx context.Context, region *metapb.Region) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	group := m.index.groupForRegion(region)
	if group == nil {
		mirrorPresent := region != nil && region.GetTableGroup() != nil
		m.mu.RUnlock()
		if mirrorPresent {
			return regionMismatch(nil, "Region mirror has no Table Group authority")
		}
		return nil
	}
	err := validateRegionSnapshot(region, group)
	fragment, found := findFragmentBinding(group, region.GetId())
	applied := region.GetTableGroup().GetAppliedMetadataVersion()
	needsUpdate := err == nil && found &&
		(applied > fragment.binding.GetAppliedMetadataVersion() ||
			region.GetRegionEpoch().GetConfVer() > fragment.binding.GetRegionEpoch().GetConfVer())
	m.mu.RUnlock()
	if err != nil || !needsUpdate {
		return err
	}
	return m.reconcileRegionProgress(ctx, region)
}

func validateRegionSnapshot(region *metapb.Region, group *table_grouppb.TableGroup) error {
	if region == nil {
		return regionMismatch(group.GetIdentity(), "missing Region heartbeat metadata")
	}
	fragment, found := findFragmentBinding(group, region.GetId())
	if !found {
		return regionMismatch(group.GetIdentity(), "Region is not bound to a Table Group fragment")
	}
	if !regionEpochFollowsBinding(region.GetRegionEpoch(), fragment.binding.GetRegionEpoch()) {
		return epochMismatch(group.GetIdentity(), "Region epoch does not follow Table Group binding")
	}
	if !regionMatchesFragmentRange(region, group) {
		return regionMismatch(group.GetIdentity(), "Region identity, range, or mirror differs from Table Group authority")
	}
	mirror := region.GetTableGroup()
	if mirror == nil {
		if group.GetState() == table_grouppb.TableGroupState_TABLE_GROUP_STATE_CREATING &&
			fragment.binding.GetAppliedMetadataVersion() == 0 {
			return nil
		}
		return regionMismatch(group.GetIdentity(), "Region mirror is missing after Table Group attachment")
	}
	if !regionMatchesFragmentMirror(region, group, fragment, false) {
		return regionMismatch(group.GetIdentity(), "Region identity, range, or mirror differs from Table Group authority")
	}
	applied := mirror.GetAppliedMetadataVersion()
	if applied == 0 && group.GetState() == table_grouppb.TableGroupState_TABLE_GROUP_STATE_CREATING &&
		fragment.binding.GetAppliedMetadataVersion() == 0 {
		return nil
	}
	if applied == 0 || applied < fragment.binding.GetAppliedMetadataVersion() {
		return staleMetadata(group.GetIdentity(), fragment.binding.GetAppliedMetadataVersion(), applied)
	}
	if applied > group.GetMetadataVersion() {
		return staleMetadata(group.GetIdentity(), group.GetMetadataVersion(), applied)
	}
	return nil
}

func (m *Manager) reconcileRegionProgress(ctx context.Context, region *metapb.Region) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	group := m.index.groupForRegion(region)
	if group == nil {
		return regionMismatch(nil, "Table Group authority disappeared during heartbeat reconcile")
	}
	if err := validateRegionSnapshot(region, group); err != nil {
		return err
	}
	applied := region.GetTableGroup().GetAppliedMetadataVersion()
	fragment, found := findFragmentBinding(group, region.GetId())
	if !found {
		return regionMismatch(group.GetIdentity(), "Region is not bound to a Table Group fragment")
	}
	confVer := region.GetRegionEpoch().GetConfVer()
	appliedAdvanced := applied > fragment.binding.GetAppliedMetadataVersion()
	confVerAdvanced := confVer > fragment.binding.GetRegionEpoch().GetConfVer()
	if !appliedAdvanced && !confVerAdvanced {
		return nil
	}
	next := cloneGroup(group)
	nextFragment, found := findFragmentBinding(next, region.GetId())
	if !found {
		return regionMismatch(group.GetIdentity(), "Table Group fragment disappeared during heartbeat reconcile")
	}
	if appliedAdvanced {
		nextFragment.binding.AppliedMetadataVersion = applied
	}
	if confVerAdvanced {
		nextFragment.binding.RegionEpoch = &metapb.RegionEpoch{
			Version: region.GetRegionEpoch().GetVersion(),
			ConfVer: confVer,
		}
	}
	next.StatusVersion++

	var token string
	var currentRecord, nextRecord *operationRecord
	if group.GetState() == table_grouppb.TableGroupState_TABLE_GROUP_STATE_CREATING &&
		allFragmentBindingsApplied(next, group.GetMetadataVersion()) {
		next.MetadataVersion++
		next.State = table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE
		token = tokenHex(group.GetPendingOperation().GetToken())
		currentRecord = m.operations[token]
		if currentRecord == nil || currentRecord.Kind != table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE {
			return operationConflict(group.GetIdentity(), group.GetPendingOperation().GetToken(), "create operation history is missing")
		}
		clonedRecord := *currentRecord
		clonedRecord.Outcome = operationOutcomeActive
		nextRecord = &clonedRecord
		next.PendingOperation = nil
	}

	var err error
	if nextRecord != nil {
		err = m.persistExistingOperation(ctx, group, next, token, currentRecord, nextRecord)
	} else {
		err = m.persistStatus(ctx, group, next)
	}
	if err != nil {
		return err
	}
	if err := m.index.upsert(next); err != nil {
		return err
	}
	if nextRecord != nil {
		m.operations[token] = nextRecord
		tableGroupLifecycleCounter.WithLabelValues("create", "active").Inc()
	}
	return nil
}

func allFragmentBindingsApplied(group *table_grouppb.TableGroup, metadataVersion uint64) bool {
	bindings, err := fragmentBindingsForGroup(group)
	if err != nil {
		return false
	}
	for _, fragment := range bindings {
		if fragment.binding.GetAppliedMetadataVersion() != metadataVersion {
			return false
		}
	}
	return true
}

func (m *Manager) persistStatus(ctx context.Context, current, next *table_grouppb.TableGroup) error {
	return m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		persisted, err := m.storage.LoadTableGroup(txn, current.GetIdentity().GetTableGroupId())
		if err != nil {
			return err
		}
		if err := samePersistedGroup(persisted, current); err != nil {
			return err
		}
		return m.storage.SaveTableGroup(txn, next)
	})
}
