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
	"context"
	"os"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/keyspace"
	pdtablegroup "github.com/tikv/pd/pkg/tablegroup"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestTableGroupWatcherLoadsUpdatesAndDeletesPolicy(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldClusterID := keypath.ClusterID()
	keypath.SetClusterID(42)
	defer keypath.SetClusterID(oldClusterID)

	cfg := etcdutil.NewTestEtcdConfig()
	dir, err := os.MkdirTemp("", "pd_table_group_watcher_tests")
	re.NoError(err)
	re.NoError(os.RemoveAll(dir))
	cfg.Dir = dir
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer func() {
		etcd.Close()
		re.NoError(os.RemoveAll(dir))
	}()
	client, err := etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls, etcdutil.TestEtcdClientPurpose, true)
	re.NoError(err)
	defer client.Close()
	<-etcd.Server.ReadyNotify()

	// Persist authority before startup to exercise the initial-load barrier.
	group := validWatchedTableGroup()
	value, err := proto.Marshal(group)
	re.NoError(err)
	_, err = client.Put(ctx, keypath.TableGroupRecordPath(group.GetIdentity().GetTableGroupId()), string(value))
	re.NoError(err)
	policy := pdtablegroup.NewSplitPolicyRegistry()
	watcher, err := NewWatcher(ctx, client, policy)
	re.NoError(err)
	defer watcher.Close()

	bound := keyspace.MakeRegionBound(group.GetIdentity().GetKeyspaceId())
	region := &metapb.Region{
		Id:          group.GetRegionBinding().GetRegionId(),
		StartKey:    bytes.Clone(bound.TxnLeftBound),
		EndKey:      bytes.Clone(bound.TxnRightBound),
		RegionEpoch: proto.Clone(group.GetRegionBinding().GetRegionEpoch()).(*metapb.RegionEpoch),
		TableGroup: &metapb.TableGroupRegionMeta{
			KeyspaceId:   group.GetIdentity().GetKeyspaceId(),
			TableGroupId: group.GetIdentity().GetTableGroupId(), AppliedMetadataVersion: 1,
		},
	}
	re.NoError(policy.ValidateRegion(region))

	// Advance both clocks and wait until the watcher publishes the new snapshot.
	updated := proto.Clone(group).(*table_grouppb.TableGroup)
	updated.MetadataVersion++
	updated.StatusVersion++
	updated.RegionBinding.AppliedMetadataVersion++
	value, err = proto.Marshal(updated)
	re.NoError(err)
	_, err = client.Put(ctx, keypath.TableGroupRecordPath(group.GetIdentity().GetTableGroupId()), string(value))
	re.NoError(err)
	region.TableGroup.AppliedMetadataVersion = 2
	testutil.Eventually(re, func() bool { return policy.ValidateRegion(region) == nil })

	// Delete authority and prove the protected interval disappears from the registry.
	_, err = client.Delete(ctx, keypath.TableGroupRecordPath(group.GetIdentity().GetTableGroupId()))
	re.NoError(err)
	region.TableGroup = nil
	testutil.Eventually(re, func() bool { return policy.ValidateRegion(region) == nil })
}

func validWatchedTableGroup() *table_grouppb.TableGroup {
	return &table_grouppb.TableGroup{
		Identity:         &table_grouppb.TableGroupIdentity{KeyspaceId: 1, TableGroupId: 101},
		MetadataVersion:  1,
		StatusVersion:    1,
		State:            table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE,
		ActiveMembership: &table_grouppb.TableGroupMembership{Version: 1},
		RegionBinding: &table_grouppb.TableGroupRegionBinding{
			RegionId: 10, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
			ShardId: 10, AppliedMetadataVersion: 1,
		},
		SplitPolicy: &table_grouppb.SplitPolicy{Mode: table_grouppb.SplitPolicyMode_SPLIT_POLICY_MODE_FORBID},
		CapacityBudget: &table_grouppb.CapacityBudget{
			MaxDataBytes: 1, MaxWriteBytesPerSecond: 1, MaxRequestsPerSecond: 1,
			MaxRaftLogBytes: 1, MaxSnapshotBytes: 1, MaxRecoverySeconds: 1,
			LimitAction: table_grouppb.CapacityLimitAction_CAPACITY_LIMIT_ACTION_REJECT,
		},
		CapacityStatus:  &table_grouppb.CapacityStatus{State: table_grouppb.CapacityState_CAPACITY_STATE_UNKNOWN},
		PlacementIntent: &table_grouppb.PlacementIntent{ReplicaCount: 1},
	}
}
