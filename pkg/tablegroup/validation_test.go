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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/table_grouppb"
)

func TestTableGroupMembershipValidationRequiresCanonicalCompleteSnapshot(t *testing.T) {
	tests := []struct {
		name       string
		membership *table_grouppb.TableGroupMembership
		valid      bool
	}{
		{
			name: "canonical",
			membership: &table_grouppb.TableGroupMembership{Version: 2, Members: []*table_grouppb.TableGroupMember{
				{TableId: 1, IndexIds: []uint64{2, 3}},
				{TableId: 2, PartitionId: 10},
			}},
			valid: true,
		},
		{
			name: "duplicate member",
			membership: &table_grouppb.TableGroupMembership{Version: 2, Members: []*table_grouppb.TableGroupMember{
				{TableId: 1}, {TableId: 1},
			}},
		},
		{
			name: "unsorted index",
			membership: &table_grouppb.TableGroupMembership{Version: 2, Members: []*table_grouppb.TableGroupMember{
				{TableId: 1, IndexIds: []uint64{3, 2}},
			}},
		},
		{
			name:       "zero version",
			membership: &table_grouppb.TableGroupMembership{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMembership(test.membership)
			if test.valid {
				require.NoError(t, err)
			} else {
				requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
			}
		})
	}
}

func TestTableGroupCreateValidationRejectsOversizedTokenBeforeHashing(t *testing.T) {
	env := newManagerTestEnv(t)
	env.request.OperationToken = make([]byte, maxOperationTokenBytes+1)
	_, err := env.manager.Create(context.Background(), env.request)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
	require.Equal(t, 0, env.allocator.callCount())
}

func TestTableGroupCreateValidationBoundsPlacementBeforeHashing(t *testing.T) {
	t.Run("location labels", func(t *testing.T) {
		env := newManagerTestEnv(t)
		env.request.PlacementIntent.LocationLabels = make([]string, maxLocationLabels+1)
		_, err := env.manager.Create(context.Background(), env.request)
		requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
		require.Equal(t, 0, env.allocator.callCount())
	})

	t.Run("combined constraints", func(t *testing.T) {
		env := newManagerTestEnv(t)
		env.request.PlacementIntent.ReplicaConstraints = make([]*table_grouppb.PlacementConstraint, maxPlacementConstraints)
		env.request.PlacementIntent.LeaderConstraints = []*table_grouppb.PlacementConstraint{{}}
		_, err := env.manager.Create(context.Background(), env.request)
		requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
		require.Equal(t, 0, env.allocator.callCount())
	})
}

func TestStoredTableGroupRequiresBoundedCapacityStatus(t *testing.T) {
	group := validStoredGroup(1, 1)
	group.CapacityStatus = nil
	requireErrorCode(t, validateStoredGroup(group),
		table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)

	group.CapacityStatus = &table_grouppb.CapacityStatus{
		State: table_grouppb.CapacityState_CAPACITY_STATE_UNKNOWN,
		LimitingDimensions: []table_grouppb.CapacityDimension{
			table_grouppb.CapacityDimension_CAPACITY_DIMENSION_DATA_BYTES,
			table_grouppb.CapacityDimension_CAPACITY_DIMENSION_DATA_BYTES,
		},
	}
	requireErrorCode(t, validateStoredGroup(group),
		table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
}
