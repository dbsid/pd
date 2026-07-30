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
	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/table_grouppb"
)

// GetRoute derives a complete TiProxy routing snapshot from Table Group
// authority and current Region/store state.
func (m *Manager) GetRoute(identity *table_grouppb.TableGroupIdentity) (*table_grouppb.TableGroupRoute, error) {
	if err := validateIdentity(identity); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	group, err := m.getLocked(identity)
	if err != nil {
		return nil, err
	}
	if group.GetState() != table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE {
		return nil, invalidState(identity, "Table Group route is unavailable before the group is active")
	}
	bindings, err := fragmentBindingsForGroup(group)
	if err != nil {
		return nil, err
	}

	partitioning := group.GetPartitioning()
	if partitioning == nil {
		partitioning = &table_grouppb.TableGroupPartitioning{
			Method:         table_grouppb.TableGroupPartitionMethod_TABLE_GROUP_PARTITION_METHOD_SINGLE,
			PartitionCount: 1,
		}
	} else {
		partitioning = proto.Clone(partitioning).(*table_grouppb.TableGroupPartitioning)
	}
	route := &table_grouppb.TableGroupRoute{
		Identity:                proto.Clone(group.GetIdentity()).(*table_grouppb.TableGroupIdentity),
		MetadataVersion:         group.GetMetadataVersion(),
		StatusVersion:           group.GetStatusVersion(),
		ActiveMembershipVersion: group.GetActiveMembership().GetVersion(),
		Partitioning:            partitioning,
		FragmentRoutes:          make([]*table_grouppb.TableGroupFragmentRoute, 0, len(bindings)),
	}
	for _, fragment := range bindings {
		binding := fragment.binding
		if binding.GetAppliedMetadataVersion() != group.GetMetadataVersion() {
			return nil, staleMetadata(identity, group.GetMetadataVersion(), binding.GetAppliedMetadataVersion())
		}
		region := m.regions.GetRegion(binding.GetRegionId())
		if region == nil || !regionMatchesGroup(region.GetMeta(), group, true) ||
			region.GetMeta().GetTableGroup().GetAppliedMetadataVersion() != group.GetMetadataVersion() {
			return nil, regionMismatch(identity, "Table Group fragment Region is not route-ready")
		}
		leader := region.GetLeader()
		if leader == nil || leader.GetStoreId() == 0 {
			return nil, regionMismatch(identity, "Table Group fragment has no current leader")
		}
		store := m.regions.GetStore(leader.GetStoreId())
		if store == nil || !store.IsServing() || store.GetMeta().GetSqlAddress() == "" {
			return nil, regionMismatch(identity, "Table Group fragment leader has no serving SQL endpoint")
		}
		route.FragmentRoutes = append(route.FragmentRoutes, &table_grouppb.TableGroupFragmentRoute{
			FragmentId:       fragment.fragmentID,
			RegionBinding:    proto.Clone(binding).(*table_grouppb.TableGroupRegionBinding),
			LeaderStoreId:    leader.GetStoreId(),
			LeaderSqlAddress: store.GetMeta().GetSqlAddress(),
		})
	}
	return route, nil
}
