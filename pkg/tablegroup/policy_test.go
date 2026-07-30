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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/keyspace"
)

func TestTableGroupSplitPolicyRejectsIdentityRangeAndMirrorPaths(t *testing.T) {
	group := validStoredGroup(101, 10)
	registry := NewSplitPolicyRegistry()
	require.NoError(t, registry.Sync(group))
	bound := keyspace.MakeRegionBound(1)
	exact := &metapb.Region{
		Id:          10,
		StartKey:    bytes.Clone(bound.TxnLeftBound),
		EndKey:      bytes.Clone(bound.TxnRightBound),
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		TableGroup: &metapb.TableGroupRegionMeta{
			KeyspaceId: 1, TableGroupId: 101, AppliedMetadataVersion: 1,
		},
	}

	for _, source := range []table_grouppb.SplitSource{
		table_grouppb.SplitSource_SPLIT_SOURCE_AUTOMATIC_SIZE,
		table_grouppb.SplitSource_SPLIT_SOURCE_AUTOMATIC_KEY_COUNT,
		table_grouppb.SplitSource_SPLIT_SOURCE_AUTOMATIC_LOAD,
		table_grouppb.SplitSource_SPLIT_SOURCE_MANUAL_REQUEST,
		table_grouppb.SplitSource_SPLIT_SOURCE_ADMIN_COMMAND,
		table_grouppb.SplitSource_SPLIT_SOURCE_RECOVERY_REPLAY,
	} {
		err := registry.EnsureTableGroupSplitAllowed(exact, source)
		requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_SPLIT_FORBIDDEN)
		require.Equal(t, source, ErrorDetail(err).GetSplitSource())
	}

	missingMirror := proto.Clone(exact).(*metapb.Region)
	missingMirror.TableGroup = nil
	err := registry.EnsureTableGroupSplitAllowed(missingMirror,
		table_grouppb.SplitSource_SPLIT_SOURCE_MANUAL_REQUEST)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_SPLIT_FORBIDDEN)
	require.Equal(t, table_grouppb.SplitRejectionReason_SPLIT_REJECTION_REASON_REGION_BINDING_MISMATCH,
		ErrorDetail(err).GetSplitRejectionReason())

	child := &metapb.Region{
		Id:       11,
		StartKey: bytes.Clone(bound.TxnLeftBound),
		EndKey:   append(bytes.Clone(bound.TxnLeftBound), 0),
	}
	err = registry.EnsureTableGroupSplitAllowed(child,
		table_grouppb.SplitSource_SPLIT_SOURCE_RECOVERY_REPLAY)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_SPLIT_FORBIDDEN)

	ordinary := &metapb.Region{Id: 99, StartKey: []byte("a"), EndKey: []byte("b")}
	require.NoError(t, registry.EnsureTableGroupSplitAllowed(ordinary,
		table_grouppb.SplitSource_SPLIT_SOURCE_MANUAL_REQUEST))

	mirrorOnly := proto.Clone(exact).(*metapb.Region)
	requireErrorCode(t, EnsureSplitAllowed(nil, mirrorOnly,
		table_grouppb.SplitSource_SPLIT_SOURCE_AUTOMATIC_SIZE),
		table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_SPLIT_FORBIDDEN)
}

func TestTableGroupSplitPolicyRegistryRejectsStaleOrConflictingSnapshot(t *testing.T) {
	registry := NewSplitPolicyRegistry()
	current := validStoredGroup(101, 10)
	current.MetadataVersion = 2
	require.NoError(t, registry.Sync(current))
	originalRange := &registry.index.ranges[0]

	stale := cloneGroup(current)
	stale.MetadataVersion = 1
	requireErrorCode(t, registry.Sync(stale),
		table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_STALE_METADATA_VERSION)

	conflict := cloneGroup(current)
	conflict.RegionBinding.ShardId = 99
	requireErrorCode(t, registry.Sync(conflict),
		table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_OPERATION_CONFLICT)

	statusConflict := cloneGroup(current)
	statusConflict.StatusVersion++
	statusConflict.RegionBinding.ShardId = 99
	requireErrorCode(t, registry.Sync(statusConflict),
		table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_OPERATION_CONFLICT)

	statusUpdate := cloneGroup(current)
	statusUpdate.StatusVersion++
	statusUpdate.RegionBinding.AppliedMetadataVersion++
	require.NoError(t, registry.Sync(statusUpdate))
	require.Same(t, originalRange, &registry.index.ranges[0], "status-only updates must not rebuild protected ranges")

	metadataUpdate := cloneGroup(statusUpdate)
	metadataUpdate.MetadataVersion++
	metadataUpdate.CapacityStatus.State = table_grouppb.CapacityState_CAPACITY_STATE_HEALTHY
	requireErrorCode(t, registry.Sync(metadataUpdate),
		table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_OPERATION_CONFLICT)
}

func TestHashFragmentSplitPolicyIndexesAndValidatesEveryRegion(t *testing.T) {
	group := validHashFragmentGroup()
	group.MetadataVersion = 2
	registry := NewSplitPolicyRegistry()
	require.NoError(t, registry.Sync(group))
	require.Len(t, registry.index.regions, 9)

	bound := keyspace.MakeRegionBound(group.GetIdentity().GetKeyspaceId())
	regions := make([]*metapb.Region, 0, 9)
	for fragmentID, fragment := range group.GetFragmentBindings() {
		startKey := append(bytes.Clone(bound.TxnLeftBound), byte(fragmentID))
		if fragmentID == 0 {
			startKey = bytes.Clone(bound.TxnLeftBound)
		}
		endKey := append(bytes.Clone(bound.TxnLeftBound), byte(fragmentID+1))
		if fragmentID+1 == len(group.GetFragmentBindings()) {
			endKey = bytes.Clone(bound.TxnRightBound)
		}
		region := &metapb.Region{
			Id:          fragment.GetRegionBinding().GetRegionId(),
			StartKey:    startKey,
			EndKey:      endKey,
			RegionEpoch: proto.Clone(fragment.GetRegionBinding().GetRegionEpoch()).(*metapb.RegionEpoch),
			TableGroup: &metapb.TableGroupRegionMeta{
				KeyspaceId:             group.GetIdentity().GetKeyspaceId(),
				TableGroupId:           group.GetIdentity().GetTableGroupId(),
				AppliedMetadataVersion: fragment.GetRegionBinding().GetAppliedMetadataVersion(),
				FragmentId:             uint32(fragmentID),
			},
		}
		regions = append(regions, region)
		require.NoError(t, registry.ValidateRegion(region))
		requireErrorCode(t, registry.EnsureTableGroupSplitAllowed(
			region,
			table_grouppb.SplitSource_SPLIT_SOURCE_AUTOMATIC_SIZE,
		), table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_SPLIT_FORBIDDEN)
	}

	wrongFragment := proto.Clone(regions[4]).(*metapb.Region)
	wrongFragment.TableGroup.FragmentId = 5
	requireErrorCode(t, registry.ValidateRegion(wrongFragment),
		table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_REGION_MISMATCH)

	statusUpdate := cloneGroup(group)
	statusUpdate.StatusVersion++
	statusUpdate.FragmentBindings[4].RegionBinding.AppliedMetadataVersion = group.GetMetadataVersion()
	require.NoError(t, registry.Sync(statusUpdate))
	require.Equal(t, group.GetMetadataVersion(),
		registry.index.groups[group.GetIdentity().GetTableGroupId()].GetFragmentBindings()[4].GetRegionBinding().GetAppliedMetadataVersion())
}
