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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/table_grouppb"
)

func TestFragmentBindingsAcceptLegacyAndNineHashFragments(t *testing.T) {
	legacy := validStoredGroup(101, 10)
	bindings, err := fragmentBindingsForGroup(legacy)
	require.NoError(t, err)
	require.Len(t, bindings, 1)
	require.Equal(t, uint32(0), bindings[0].fragmentID)

	distributed := validHashFragmentGroup()
	bindings, err = fragmentBindingsForGroup(distributed)
	require.NoError(t, err)
	require.Len(t, bindings, 9)
	for fragmentID, binding := range bindings {
		require.Equal(t, uint32(fragmentID), binding.fragmentID)
		require.Equal(t, uint64(10+fragmentID), binding.binding.GetRegionId())
	}
}

func TestFragmentBindingsRejectNonCanonicalRepresentations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*table_grouppb.TableGroup)
	}{
		{
			name: "legacy and fragments mixed",
			mutate: func(group *table_grouppb.TableGroup) {
				group.RegionBinding = &table_grouppb.TableGroupRegionBinding{
					RegionId: 99, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}, ShardId: 99,
				}
			},
		},
		{
			name: "missing fragment",
			mutate: func(group *table_grouppb.TableGroup) {
				group.FragmentBindings = group.FragmentBindings[:8]
			},
		},
		{
			name: "unordered fragment",
			mutate: func(group *table_grouppb.TableGroup) {
				group.FragmentBindings[4].FragmentId = 5
			},
		},
		{
			name: "duplicate Region",
			mutate: func(group *table_grouppb.TableGroup) {
				group.FragmentBindings[8].RegionBinding.RegionId = group.FragmentBindings[7].RegionBinding.RegionId
			},
		},
		{
			name: "duplicate Shard",
			mutate: func(group *table_grouppb.TableGroup) {
				group.FragmentBindings[8].RegionBinding.ShardId = group.FragmentBindings[7].RegionBinding.ShardId
			},
		},
		{
			name: "unknown hash algorithm",
			mutate: func(group *table_grouppb.TableGroup) {
				group.Partitioning.HashAlgorithm = table_grouppb.TableGroupHashAlgorithm_TABLE_GROUP_HASH_ALGORITHM_UNSPECIFIED
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			group := proto.Clone(validHashFragmentGroup()).(*table_grouppb.TableGroup)
			test.mutate(group)
			_, err := fragmentBindingsForGroup(group)
			requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
		})
	}
}

func validHashFragmentGroup() *table_grouppb.TableGroup {
	const count uint32 = 9
	group := validStoredGroup(101, 10)
	group.RegionBinding = nil
	group.Partitioning = &table_grouppb.TableGroupPartitioning{
		Method:         table_grouppb.TableGroupPartitionMethod_TABLE_GROUP_PARTITION_METHOD_HASH,
		PartitionCount: count,
		HashAlgorithm:  table_grouppb.TableGroupHashAlgorithm_TABLE_GROUP_HASH_ALGORITHM_MODULO_U64_V1,
	}
	group.FragmentBindings = make([]*table_grouppb.TableGroupFragmentBinding, 0, count)
	for fragmentID := range count {
		group.FragmentBindings = append(group.FragmentBindings, &table_grouppb.TableGroupFragmentBinding{
			FragmentId: fragmentID,
			RegionBinding: &table_grouppb.TableGroupRegionBinding{
				RegionId:               10 + uint64(fragmentID),
				RegionEpoch:            &metapb.RegionEpoch{Version: 1, ConfVer: 1},
				ShardId:                20 + uint64(fragmentID),
				AppliedMetadataVersion: group.GetMetadataVersion(),
			},
		})
	}
	return group
}
