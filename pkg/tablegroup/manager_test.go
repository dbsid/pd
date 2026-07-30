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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type testAllocator struct {
	mu         sync.Mutex
	base       uint64
	calls      int
	allocAtOps int
}

func (a *testAllocator) AllocAt(id uint64) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.allocAtOps++
	if id <= a.base {
		return false, nil
	}
	a.base = id
	return true, nil
}

func (a *testAllocator) Alloc(count uint32) (uint64, uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.base += uint64(count)
	return a.base, count, nil
}

func (a *testAllocator) SetBase(base uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.base = base
	return nil
}

func (*testAllocator) Rebase() error { return nil }

func (a *testAllocator) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func (a *testAllocator) allocAtCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.allocAtOps
}

type testKeyspaces struct {
	metas map[uint32]*keyspacepb.KeyspaceMeta
}

func (k *testKeyspaces) LoadKeyspaceByID(id uint32) (*keyspacepb.KeyspaceMeta, error) {
	if meta := k.metas[id]; meta != nil {
		return proto.Clone(meta).(*keyspacepb.KeyspaceMeta), nil
	}
	return nil, notFound(nil)
}

type testRegions struct {
	regions map[uint64]*core.RegionInfo
	stores  map[uint64]*core.StoreInfo
}

func (r *testRegions) GetRegion(id uint64) *core.RegionInfo {
	return r.regions[id]
}

func (r *testRegions) GetRegionByKey(key []byte) *core.RegionInfo {
	for _, region := range r.regions {
		if bytes.Compare(key, region.GetStartKey()) >= 0 &&
			(len(region.GetEndKey()) == 0 || bytes.Compare(key, region.GetEndKey()) < 0) {
			return region
		}
	}
	return nil
}

func (r *testRegions) GetStore(id uint64) *core.StoreInfo {
	return r.stores[id]
}

type countingStorage struct {
	storage.Storage
	saves atomic.Int64
}

type contextCheckingStorage struct {
	storage.Storage
}

func (s *contextCheckingStorage) RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Storage.RunInTxn(ctx, f)
}

func (s *countingStorage) SaveTableGroup(txn kv.Txn, group *table_grouppb.TableGroup) error {
	s.saves.Add(1)
	return s.Storage.SaveTableGroup(txn, group)
}

type managerTestEnv struct {
	storage   *countingStorage
	allocator *testAllocator
	keyspaces *testKeyspaces
	regions   *testRegions
	manager   *Manager
	request   *table_grouppb.CreateTableGroupRequest
	region    *metapb.Region
	fragments []*metapb.Region
}

func newManagerTestEnv(t *testing.T) *managerTestEnv {
	t.Helper()
	const keyspaceID = 1
	bound := keyspace.MakeRegionBound(keyspaceID)
	peer := &metapb.Peer{Id: 11, StoreId: 1}
	region := &metapb.Region{
		Id:          10,
		StartKey:    bytes.Clone(bound.TxnLeftBound),
		EndKey:      bytes.Clone(bound.TxnRightBound),
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		Peers:       []*metapb.Peer{peer},
	}
	regionInfo := core.NewRegionInfo(region, peer)
	backend := &countingStorage{Storage: storage.NewStorageWithMemoryBackend()}
	allocator := &testAllocator{base: 100}
	keyspaces := &testKeyspaces{metas: map[uint32]*keyspacepb.KeyspaceMeta{
		keyspaceID: {
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: keyspaceID},
			Name:     "table-group-test",
			State:    keyspacepb.KeyspaceState_ENABLED,
			Config:   map[string]string{keyspace.RegionBoundType: "txn"},
		},
	}}
	regions := &testRegions{
		regions: map[uint64]*core.RegionInfo{region.GetId(): regionInfo},
		stores: map[uint64]*core.StoreInfo{
			1: core.NewStoreInfo(&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving, SqlAddress: "127.0.0.1:4000"}),
		},
	}
	manager, err := NewManager(context.Background(), backend, allocator, keyspaces, regions)
	require.NoError(t, err)
	return &managerTestEnv{
		storage:   backend,
		allocator: allocator,
		keyspaces: keyspaces,
		regions:   regions,
		manager:   manager,
		request:   newCreateRequest(keyspaceID, region, []byte("create-token")),
		region:    region,
	}
}

func newCreateRequest(keyspaceID uint32, region *metapb.Region, token []byte) *table_grouppb.CreateTableGroupRequest {
	return &table_grouppb.CreateTableGroupRequest{
		KeyspaceId: keyspaceID,
		RegionBinding: &table_grouppb.TableGroupRegionBinding{
			RegionId:    region.GetId(),
			RegionEpoch: proto.Clone(region.GetRegionEpoch()).(*metapb.RegionEpoch),
			ShardId:     region.GetId(),
		},
		SplitPolicy: &table_grouppb.SplitPolicy{Mode: table_grouppb.SplitPolicyMode_SPLIT_POLICY_MODE_FORBID},
		CapacityBudget: &table_grouppb.CapacityBudget{
			MaxDataBytes:           1,
			MaxWriteBytesPerSecond: 1,
			MaxRequestsPerSecond:   1,
			MaxRaftLogBytes:        1,
			MaxSnapshotBytes:       1,
			MaxRecoverySeconds:     1,
			LimitAction:            table_grouppb.CapacityLimitAction_CAPACITY_LIMIT_ACTION_REJECT,
		},
		PlacementIntent: &table_grouppb.PlacementIntent{ReplicaCount: 1},
		OperationToken:  bytes.Clone(token),
	}
}

func newHashFragmentManagerTestEnv(t *testing.T, count uint32) *managerTestEnv {
	t.Helper()
	env := newManagerTestEnv(t)
	bound := keyspace.MakeRegionBound(env.request.GetKeyspaceId())
	env.regions.regions = make(map[uint64]*core.RegionInfo, count)
	env.fragments = make([]*metapb.Region, 0, count)
	env.request.RegionBinding = nil
	env.request.Partitioning = &table_grouppb.TableGroupPartitioning{
		Method:         table_grouppb.TableGroupPartitionMethod_TABLE_GROUP_PARTITION_METHOD_HASH,
		PartitionCount: count,
		HashAlgorithm:  table_grouppb.TableGroupHashAlgorithm_TABLE_GROUP_HASH_ALGORITHM_MODULO_U64_V1,
	}
	env.request.FragmentBindings = make([]*table_grouppb.TableGroupFragmentBinding, 0, count)
	for fragmentID := range count {
		startKey := append(bytes.Clone(bound.TxnLeftBound), byte(fragmentID))
		if fragmentID == 0 {
			startKey = bytes.Clone(bound.TxnLeftBound)
		}
		endKey := append(bytes.Clone(bound.TxnLeftBound), byte(fragmentID+1))
		if fragmentID+1 == count {
			endKey = bytes.Clone(bound.TxnRightBound)
		}
		regionID := uint64(10 + fragmentID)
		peers := []*metapb.Peer{
			{Id: uint64(1000 + fragmentID*10), StoreId: 1},
			{Id: uint64(1001 + fragmentID*10), StoreId: 2},
			{Id: uint64(1002 + fragmentID*10), StoreId: 3},
		}
		leader := peers[fragmentID%3]
		region := &metapb.Region{
			Id:          regionID,
			StartKey:    startKey,
			EndKey:      endKey,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
			Peers:       peers,
		}
		env.fragments = append(env.fragments, region)
		env.regions.regions[regionID] = core.NewRegionInfo(region, leader)
		env.request.FragmentBindings = append(env.request.FragmentBindings, &table_grouppb.TableGroupFragmentBinding{
			FragmentId: fragmentID,
			RegionBinding: &table_grouppb.TableGroupRegionBinding{
				RegionId:    regionID,
				RegionEpoch: proto.Clone(region.GetRegionEpoch()).(*metapb.RegionEpoch),
				ShardId:     uint64(20 + fragmentID),
			},
		})
	}
	env.request.PlacementIntent.ReplicaCount = 3
	env.regions.stores = map[uint64]*core.StoreInfo{
		1: core.NewStoreInfo(&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving, SqlAddress: "10.0.0.1:4000"}),
		2: core.NewStoreInfo(&metapb.Store{Id: 2, NodeState: metapb.NodeState_Serving, SqlAddress: "10.0.0.2:4000"}),
		3: core.NewStoreInfo(&metapb.Store{Id: 3, NodeState: metapb.NodeState_Serving, SqlAddress: "10.0.0.3:4000"}),
	}
	env.region = env.fragments[0]
	return env
}

func (e *managerTestEnv) heartbeat(group *table_grouppb.TableGroup, appliedVersion uint64) *metapb.Region {
	region := proto.Clone(e.region).(*metapb.Region)
	region.TableGroup = &metapb.TableGroupRegionMeta{
		KeyspaceId:             group.GetIdentity().GetKeyspaceId(),
		TableGroupId:           group.GetIdentity().GetTableGroupId(),
		AppliedMetadataVersion: appliedVersion,
	}
	return region
}

func (e *managerTestEnv) fragmentHeartbeat(
	group *table_grouppb.TableGroup,
	fragmentID uint32,
	appliedVersion uint64,
) *metapb.Region {
	region := proto.Clone(e.fragments[fragmentID]).(*metapb.Region)
	region.TableGroup = &metapb.TableGroupRegionMeta{
		KeyspaceId:             group.GetIdentity().GetKeyspaceId(),
		TableGroupId:           group.GetIdentity().GetTableGroupId(),
		AppliedMetadataVersion: appliedVersion,
		FragmentId:             fragmentID,
	}
	return region
}

func (e *managerTestEnv) validateAndCacheFragment(
	t *testing.T,
	group *table_grouppb.TableGroup,
	fragmentID uint32,
	appliedVersion uint64,
) {
	t.Helper()
	heartbeat := e.fragmentHeartbeat(group, fragmentID, appliedVersion)
	current := e.regions.GetRegion(heartbeat.GetId())
	require.NotNil(t, current)
	leader := current.GetLeader()
	require.NoError(t, e.manager.ValidateRegion(context.Background(), heartbeat))
	e.regions.regions[heartbeat.GetId()] = core.NewRegionInfo(heartbeat, leader)
}

func TestHashFragmentTableGroupActivatesOnlyAfterEveryFragmentAndRestarts(t *testing.T) {
	env := newHashFragmentManagerTestEnv(t, 9)
	created, err := env.manager.Create(context.Background(), env.request)
	require.NoError(t, err)
	require.Equal(t, table_grouppb.TableGroupState_TABLE_GROUP_STATE_CREATING, created.GetState())
	require.Nil(t, created.GetRegionBinding())
	require.Len(t, created.GetFragmentBindings(), 9)

	// The authority snapshot must not retain request-owned protobuf pointers.
	env.request.FragmentBindings[0].RegionBinding.RegionId = 999
	unchanged, err := env.manager.Get(created.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, uint64(10), unchanged.GetFragmentBindings()[0].GetRegionBinding().GetRegionId())
	env.request.FragmentBindings[0].RegionBinding.RegionId = 10

	for fragmentID := range uint32(8) {
		require.NoError(t, env.manager.ValidateRegion(
			context.Background(),
			env.fragmentHeartbeat(created, fragmentID, created.GetMetadataVersion()),
		))
		current, getErr := env.manager.Get(created.GetIdentity())
		require.NoError(t, getErr)
		require.Equal(t, table_grouppb.TableGroupState_TABLE_GROUP_STATE_CREATING, current.GetState())
		require.Equal(t, created.GetMetadataVersion(),
			current.GetFragmentBindings()[fragmentID].GetRegionBinding().GetAppliedMetadataVersion())
	}

	require.NoError(t, env.manager.ValidateRegion(
		context.Background(),
		env.fragmentHeartbeat(created, 8, created.GetMetadataVersion()),
	))
	active, err := env.manager.Get(created.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE, active.GetState())
	require.Equal(t, uint64(2), active.GetMetadataVersion())
	for fragmentID, fragment := range active.GetFragmentBindings() {
		require.Equal(t, uint32(fragmentID), fragment.GetFragmentId())
		require.Equal(t, uint64(1), fragment.GetRegionBinding().GetAppliedMetadataVersion())
		discovered, getErr := env.manager.GetByRegion(
			active.GetIdentity().GetKeyspaceId(),
			fragment.GetRegionBinding().GetRegionId(),
		)
		require.NoError(t, getErr)
		require.True(t, proto.Equal(active, discovered))
	}

	restarted, err := NewManager(context.Background(), env.storage, env.allocator, env.keyspaces, env.regions)
	require.NoError(t, err)
	for _, fragment := range active.GetFragmentBindings() {
		discovered, getErr := restarted.GetByRegion(
			active.GetIdentity().GetKeyspaceId(),
			fragment.GetRegionBinding().GetRegionId(),
		)
		require.NoError(t, getErr)
		require.True(t, proto.Equal(active, discovered))
	}
}

func TestHashFragmentTableGroupRejectsNonContiguousRegions(t *testing.T) {
	env := newHashFragmentManagerTestEnv(t, 9)
	region := proto.Clone(env.fragments[4]).(*metapb.Region)
	region.StartKey = append(region.StartKey, 0)
	env.fragments[4] = region
	env.regions.regions[region.GetId()] = core.NewRegionInfo(region, region.GetPeers()[0])

	_, err := env.manager.Create(context.Background(), env.request)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_REGION_MISMATCH)
}

func TestHashFragmentTableGroupRouteTracksLeadersAndRequiresReadySQLStores(t *testing.T) {
	env := newHashFragmentManagerTestEnv(t, 9)
	created, err := env.manager.Create(context.Background(), env.request)
	require.NoError(t, err)
	for fragmentID := range uint32(9) {
		env.validateAndCacheFragment(t, created, fragmentID, created.GetMetadataVersion())
	}
	active, err := env.manager.Get(created.GetIdentity())
	require.NoError(t, err)

	_, err = env.manager.GetRoute(active.GetIdentity())
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_STALE_METADATA_VERSION)
	for fragmentID := range uint32(9) {
		current, getErr := env.manager.Get(active.GetIdentity())
		require.NoError(t, getErr)
		env.validateAndCacheFragment(t, current, fragmentID, current.GetMetadataVersion())
	}

	route, err := env.manager.GetRoute(active.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, table_grouppb.TableGroupPartitionMethod_TABLE_GROUP_PARTITION_METHOD_HASH,
		route.GetPartitioning().GetMethod())
	require.Equal(t, uint32(9), route.GetPartitioning().GetPartitionCount())
	require.Len(t, route.GetFragmentRoutes(), 9)
	leaders := make(map[uint64]int)
	for fragmentID, fragmentRoute := range route.GetFragmentRoutes() {
		require.Equal(t, uint32(fragmentID), fragmentRoute.GetFragmentId())
		require.NotEmpty(t, fragmentRoute.GetLeaderSqlAddress())
		require.Equal(t, route.GetMetadataVersion(), fragmentRoute.GetRegionBinding().GetAppliedMetadataVersion())
		leaders[fragmentRoute.GetLeaderStoreId()]++
	}
	require.Equal(t, map[uint64]int{1: 3, 2: 3, 3: 3}, leaders)

	route.FragmentRoutes[0].LeaderSqlAddress = "mutated"
	unchanged, err := env.manager.GetRoute(active.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:4000", unchanged.GetFragmentRoutes()[0].GetLeaderSqlAddress())

	fragmentZero := env.regions.GetRegion(10)
	regionMeta := proto.Clone(fragmentZero.GetMeta()).(*metapb.Region)
	env.regions.regions[10] = core.NewRegionInfo(regionMeta, regionMeta.GetPeers()[1])
	transferred, err := env.manager.GetRoute(active.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, uint64(2), transferred.GetFragmentRoutes()[0].GetLeaderStoreId())
	require.Equal(t, "10.0.0.2:4000", transferred.GetFragmentRoutes()[0].GetLeaderSqlAddress())

	storeMeta := proto.Clone(env.regions.GetStore(2).GetMeta()).(*metapb.Store)
	storeMeta.SqlAddress = ""
	env.regions.stores[2] = core.NewStoreInfo(storeMeta)
	_, err = env.manager.GetRoute(active.GetIdentity())
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_REGION_MISMATCH)
}

func TestLegacyTableGroupRouteNormalizesSinglePartition(t *testing.T) {
	env := newManagerTestEnv(t)
	created, err := env.manager.Create(context.Background(), env.request)
	require.NoError(t, err)
	heartbeat := env.heartbeat(created, created.GetMetadataVersion())
	require.NoError(t, env.manager.ValidateRegion(context.Background(), heartbeat))
	env.regions.regions[heartbeat.GetId()] = core.NewRegionInfo(heartbeat, heartbeat.GetPeers()[0])
	active, err := env.manager.Get(created.GetIdentity())
	require.NoError(t, err)
	heartbeat = env.heartbeat(active, active.GetMetadataVersion())
	require.NoError(t, env.manager.ValidateRegion(context.Background(), heartbeat))
	env.regions.regions[heartbeat.GetId()] = core.NewRegionInfo(heartbeat, heartbeat.GetPeers()[0])

	route, err := env.manager.GetRoute(active.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, table_grouppb.TableGroupPartitionMethod_TABLE_GROUP_PARTITION_METHOD_SINGLE,
		route.GetPartitioning().GetMethod())
	require.Equal(t, uint32(1), route.GetPartitioning().GetPartitionCount())
	require.Len(t, route.GetFragmentRoutes(), 1)
	require.Equal(t, uint32(0), route.GetFragmentRoutes()[0].GetFragmentId())
}

func (e *managerTestEnv) createAndActivate(t *testing.T) *table_grouppb.TableGroup {
	t.Helper()
	created, err := e.manager.Create(context.Background(), e.request)
	require.NoError(t, err)
	require.NoError(t, e.manager.ValidateRegion(context.Background(), e.heartbeat(created, created.GetMetadataVersion())))
	active, err := e.manager.Get(created.GetIdentity())
	require.NoError(t, err)
	return active
}

func requireErrorCode(t *testing.T, err error, code table_grouppb.TableGroupErrorCode) {
	t.Helper()
	require.Error(t, err)
	detail := ErrorDetail(err)
	require.NotNil(t, detail)
	require.Equal(t, code, detail.GetCode())
}

func TestTableGroupLifecycleIsIdempotentAndRecoverableAfterRestart(t *testing.T) {
	env := newManagerTestEnv(t)
	created, err := env.manager.Create(context.Background(), env.request)
	require.NoError(t, err)
	require.Equal(t, uint64(101), created.GetIdentity().GetTableGroupId())
	require.Equal(t, uint64(1), created.GetMetadataVersion())
	require.Equal(t, table_grouppb.TableGroupState_TABLE_GROUP_STATE_CREATING, created.GetState())

	replayed, err := env.manager.Create(context.Background(), env.request)
	require.NoError(t, err)
	require.True(t, proto.Equal(created, replayed))
	require.Equal(t, 1, env.allocator.callCount())

	conflictRequest := proto.Clone(env.request).(*table_grouppb.CreateTableGroupRequest)
	conflictRequest.PlacementIntent.ReplicaCount = 3
	_, err = env.manager.Create(context.Background(), conflictRequest)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_OPERATION_CONFLICT)

	_, err = env.manager.Get(created.GetIdentity())
	require.NoError(t, err)
	mutated, err := env.manager.Get(created.GetIdentity())
	require.NoError(t, err)
	mutated.MetadataVersion = 99
	unchanged, err := env.manager.Get(created.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, uint64(1), unchanged.GetMetadataVersion())

	err = env.manager.ValidateRegion(context.Background(), env.region)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_REGION_MISMATCH)
	require.NoError(t, env.manager.ValidateRegion(context.Background(), env.heartbeat(created, 1)))
	active, err := env.manager.Get(created.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE, active.GetState())
	require.Equal(t, uint64(2), active.GetMetadataVersion())
	require.Equal(t, uint64(1), active.GetRegionBinding().GetAppliedMetadataVersion())

	require.NoError(t, env.manager.ValidateRegion(context.Background(), env.heartbeat(active, 2)))
	active, err = env.manager.Get(active.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, uint64(2), active.GetMetadataVersion())
	require.Equal(t, uint64(2), active.GetRegionBinding().GetAppliedMetadataVersion())
	statusVersion := active.GetStatusVersion()
	saves := env.storage.saves.Load()
	require.NoError(t, env.manager.ValidateRegion(context.Background(), env.heartbeat(active, 2)))
	require.Equal(t, saves, env.storage.saves.Load(), "unchanged heartbeat must not persist")

	membership := &table_grouppb.TableGroupMembership{
		Version: 2,
		Members: []*table_grouppb.TableGroupMember{{TableId: 100, IndexIds: []uint64{101, 102}}},
	}
	prepareRequest := &table_grouppb.PrepareMembershipRequest{
		Identity:                active.GetIdentity(),
		ExpectedMetadataVersion: active.GetMetadataVersion(),
		ProposedMembership:      membership,
		OperationToken:          []byte("membership-token"),
	}
	prepared, err := env.manager.PrepareMembership(context.Background(), prepareRequest)
	require.NoError(t, err)
	require.Equal(t, uint64(3), prepared.GetMetadataVersion())
	require.Equal(t, statusVersion, prepared.GetStatusVersion())
	require.Equal(t, uint64(1), prepared.GetActiveMembership().GetVersion())

	replayedPrepare, err := env.manager.PrepareMembership(context.Background(), prepareRequest)
	require.NoError(t, err)
	require.True(t, proto.Equal(prepared, replayedPrepare))

	require.NoError(t, env.manager.ValidateRegion(context.Background(), env.heartbeat(prepared, 3)))
	prepared, err = env.manager.Get(prepared.GetIdentity())
	require.NoError(t, err)
	require.Equal(t, uint64(3), prepared.GetMetadataVersion(), "status reconcile must not advance metadata")
	require.Equal(t, uint64(3), prepared.GetRegionBinding().GetAppliedMetadataVersion())

	commitRequest := &table_grouppb.CommitMembershipRequest{
		Identity:                prepared.GetIdentity(),
		ExpectedMetadataVersion: prepared.GetMetadataVersion(),
		MembershipVersion:       membership.GetVersion(),
		OperationToken:          bytes.Clone(prepareRequest.GetOperationToken()),
	}
	committed, err := env.manager.CommitMembership(context.Background(), commitRequest)
	require.NoError(t, err)
	require.Equal(t, uint64(4), committed.GetMetadataVersion())
	require.Equal(t, uint64(2), committed.GetActiveMembership().GetVersion())
	require.Nil(t, committed.GetPreparedMembership())

	replayedCommit, err := env.manager.CommitMembership(context.Background(), commitRequest)
	require.NoError(t, err)
	require.True(t, proto.Equal(committed, replayedCommit))
	abortAfterCommit := &table_grouppb.AbortMembershipRequest{
		Identity:                committed.GetIdentity(),
		ExpectedMetadataVersion: prepareRequest.GetExpectedMetadataVersion() + 1,
		MembershipVersion:       membership.GetVersion(),
		OperationToken:          bytes.Clone(prepareRequest.GetOperationToken()),
	}
	_, err = env.manager.AbortMembership(context.Background(), abortAfterCommit)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_OPERATION_CONFLICT)

	restarted, err := NewManager(context.Background(), env.storage, env.allocator, env.keyspaces, env.regions)
	require.NoError(t, err)
	reconciled, err := restarted.Reconcile(committed.GetIdentity(), env.request.GetOperationToken())
	require.NoError(t, err)
	require.True(t, proto.Equal(committed, reconciled))
	replayedAfterRestart, err := restarted.CommitMembership(context.Background(), commitRequest)
	require.NoError(t, err)
	require.True(t, proto.Equal(committed, replayedAfterRestart))
	require.Equal(t, 1, env.allocator.callCount())
}

func TestTableGroupCanBeDiscoveredByRegionBeforeMirrorAttachment(t *testing.T) {
	env := newManagerTestEnv(t)
	created, err := env.manager.Create(context.Background(), env.request)
	require.NoError(t, err)
	require.Nil(t, env.region.GetTableGroup())

	discovered, err := env.manager.GetByRegion(env.request.GetKeyspaceId(), env.region.GetId())
	require.NoError(t, err)
	require.True(t, proto.Equal(created, discovered))

	_, err = env.manager.GetByRegion(env.request.GetKeyspaceId()+1, env.region.GetId())
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_KEYSPACE_MISMATCH)
	_, err = env.manager.GetByRegion(env.request.GetKeyspaceId(), env.region.GetId()+1)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_NOT_FOUND)
	_, err = env.manager.GetByRegion(env.request.GetKeyspaceId(), 0)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
}

func TestTableGroupAbortPreservesActiveMembershipAndReplays(t *testing.T) {
	env := newManagerTestEnv(t)
	active := env.createAndActivate(t)
	prepare := &table_grouppb.PrepareMembershipRequest{
		Identity:                active.GetIdentity(),
		ExpectedMetadataVersion: active.GetMetadataVersion(),
		ProposedMembership: &table_grouppb.TableGroupMembership{
			Version: 2,
			Members: []*table_grouppb.TableGroupMember{{TableId: 1}},
		},
		OperationToken: []byte("abort-token"),
	}
	prepared, err := env.manager.PrepareMembership(context.Background(), prepare)
	require.NoError(t, err)
	abort := &table_grouppb.AbortMembershipRequest{
		Identity:                prepared.GetIdentity(),
		ExpectedMetadataVersion: prepared.GetMetadataVersion(),
		MembershipVersion:       prepared.GetPreparedMembership().GetVersion(),
		OperationToken:          bytes.Clone(prepare.GetOperationToken()),
	}
	aborted, err := env.manager.AbortMembership(context.Background(), abort)
	require.NoError(t, err)
	require.Equal(t, uint64(4), aborted.GetMetadataVersion())
	require.Equal(t, uint64(1), aborted.GetActiveMembership().GetVersion())
	require.Nil(t, aborted.GetPreparedMembership())
	replayed, err := env.manager.AbortMembership(context.Background(), abort)
	require.NoError(t, err)
	require.True(t, proto.Equal(aborted, replayed))
}

func TestTableGroupConcurrentIdenticalCreateAllocatesOneID(t *testing.T) {
	env := newManagerTestEnv(t)
	const workers = 16
	groups := make(chan *table_grouppb.TableGroup, workers)
	errors := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			group, err := env.manager.Create(context.Background(), env.request)
			groups <- group
			errors <- err
		}()
	}
	wg.Wait()
	close(groups)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	for group := range groups {
		require.Equal(t, uint64(101), group.GetIdentity().GetTableGroupId())
	}
	require.Equal(t, 1, env.allocator.callCount())
}

func TestTableGroupRequestedIDUsesGlobalAllocator(t *testing.T) {
	env := newManagerTestEnv(t)
	env.request.RequestedTableGroupId = 150

	created, err := env.manager.Create(context.Background(), env.request)
	require.NoError(t, err)
	require.Equal(t, uint64(150), created.GetIdentity().GetTableGroupId())
	require.Equal(t, 0, env.allocator.callCount())
	require.Equal(t, 1, env.allocator.allocAtCount())

	replayed, err := env.manager.Create(context.Background(), env.request)
	require.NoError(t, err)
	require.True(t, proto.Equal(created, replayed))
	require.Equal(t, 1, env.allocator.allocAtCount())

	collision := newManagerTestEnv(t)
	collision.request.RequestedTableGroupId = 100
	_, err = collision.manager.Create(context.Background(), collision.request)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_ALREADY_EXISTS)
	require.Equal(t, 1, collision.allocator.allocAtCount())
}

func TestTableGroupCreateRequiresExplicitTransactionalKeyspace(t *testing.T) {
	env := newManagerTestEnv(t)
	delete(env.keyspaces.metas[env.request.GetKeyspaceId()].Config, keyspace.RegionBoundType)

	_, err := env.manager.Create(context.Background(), env.request)
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_KEYSPACE_MISMATCH)
	require.Equal(t, 0, env.allocator.callCount())
}

func TestTableGroupMutationUsesCallerContext(t *testing.T) {
	env := newManagerTestEnv(t)
	checkedStorage := &contextCheckingStorage{Storage: env.storage.Storage}
	manager, err := NewManager(context.Background(), checkedStorage, env.allocator, env.keyspaces, env.regions)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = manager.Create(ctx, env.request)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, env.allocator.callCount())
}

func TestTableGroupRecordKeyMustMatchPersistedIdentity(t *testing.T) {
	backend := storage.NewStorageWithMemoryBackend()
	group := validStoredGroup(102, 20)
	value, err := proto.Marshal(group)
	require.NoError(t, err)
	require.NoError(t, backend.RunInTxn(context.Background(), func(txn kv.Txn) error {
		return txn.Save(keypath.TableGroupRecordPath(101), string(value))
	}))

	_, err = NewManager(context.Background(), backend, &testAllocator{}, &testKeyspaces{}, &testRegions{})
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
}

func TestTableGroupInitializationRejectsMissingOrConflictingOperationHistory(t *testing.T) {
	t.Run("missing pending history", func(t *testing.T) {
		env := newManagerTestEnv(t)
		_, err := env.manager.Create(context.Background(), env.request)
		require.NoError(t, err)
		require.NoError(t, env.storage.RunInTxn(context.Background(), func(txn kv.Txn) error {
			return txn.Remove(keypath.TableGroupOperationPath(tokenHex(env.request.GetOperationToken())))
		}))

		_, err = NewManager(context.Background(), env.storage, env.allocator, env.keyspaces, env.regions)
		requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
	})

	t.Run("pending kind mismatch", func(t *testing.T) {
		env := newManagerTestEnv(t)
		created, err := env.manager.Create(context.Background(), env.request)
		require.NoError(t, err)
		record := &operationRecord{
			SchemaVersion:         operationRecordSchemaVersion,
			Kind:                  table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_UPDATE_MEMBERSHIP,
			KeyspaceID:            created.GetIdentity().GetKeyspaceId(),
			TableGroupID:          created.GetIdentity().GetTableGroupId(),
			BaseMetadataVersion:   1,
			TargetMetadataVersion: 2,
			PrepareHash:           env.manager.operations[tokenHex(env.request.GetOperationToken())].PrepareHash,
			Outcome:               operationOutcomePrepared,
		}
		value, err := record.marshal()
		require.NoError(t, err)
		require.NoError(t, env.storage.RunInTxn(context.Background(), func(txn kv.Txn) error {
			return env.storage.SaveTableGroupOperation(txn, tokenHex(env.request.GetOperationToken()), value)
		}))

		_, err = NewManager(context.Background(), env.storage, env.allocator, env.keyspaces, env.regions)
		requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
	})

	t.Run("active group with unfinished create history", func(t *testing.T) {
		env := newManagerTestEnv(t)
		active := env.createAndActivate(t)
		token := tokenHex(env.request.GetOperationToken())
		record := *env.manager.operations[token]
		record.Outcome = operationOutcomeCreating
		value, err := record.marshal()
		require.NoError(t, err)
		require.NoError(t, env.storage.RunInTxn(context.Background(), func(txn kv.Txn) error {
			return env.storage.SaveTableGroupOperation(txn, token, value)
		}))

		_, err = NewManager(context.Background(), env.storage, env.allocator, env.keyspaces, env.regions)
		requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT)
		require.Equal(t, table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE, active.GetState())
	})
}

func TestTableGroupDuplicatePersistedKeyspaceFailsManagerInitialization(t *testing.T) {
	backend := storage.NewStorageWithMemoryBackend()
	group1 := validStoredGroup(101, 10)
	group2 := validStoredGroup(102, 20)
	err := backend.RunInTxn(context.Background(), func(txn kv.Txn) error {
		if err := backend.SaveTableGroup(txn, group1); err != nil {
			return err
		}
		return backend.SaveTableGroup(txn, group2)
	})
	require.NoError(t, err)
	_, err = NewManager(context.Background(), backend, &testAllocator{}, &testKeyspaces{}, &testRegions{})
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_ALREADY_EXISTS)
}

func TestTableGroupDuplicatePersistedFragmentRegionFailsManagerInitialization(t *testing.T) {
	backend := storage.NewStorageWithMemoryBackend()
	group1 := validHashFragmentGroup()
	group2 := validStoredGroup(102, group1.GetFragmentBindings()[4].GetRegionBinding().GetRegionId())
	group2.Identity.KeyspaceId = 2
	err := backend.RunInTxn(context.Background(), func(txn kv.Txn) error {
		if err := backend.SaveTableGroup(txn, group1); err != nil {
			return err
		}
		return backend.SaveTableGroup(txn, group2)
	})
	require.NoError(t, err)

	_, err = NewManager(context.Background(), backend, &testAllocator{}, &testKeyspaces{}, &testRegions{})
	requireErrorCode(t, err, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_ALREADY_EXISTS)
}

func validStoredGroup(groupID, regionID uint64) *table_grouppb.TableGroup {
	return &table_grouppb.TableGroup{
		Identity:         &table_grouppb.TableGroupIdentity{KeyspaceId: 1, TableGroupId: groupID},
		MetadataVersion:  1,
		StatusVersion:    1,
		State:            table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE,
		ActiveMembership: &table_grouppb.TableGroupMembership{Version: 1},
		RegionBinding: &table_grouppb.TableGroupRegionBinding{
			RegionId:               regionID,
			RegionEpoch:            &metapb.RegionEpoch{Version: 1, ConfVer: 1},
			ShardId:                regionID,
			AppliedMetadataVersion: 1,
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
