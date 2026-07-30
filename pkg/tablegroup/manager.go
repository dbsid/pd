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

// Package tablegroup implements the PD-owned Table Group lifecycle authority.
package tablegroup

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// KeyspaceProvider loads keyspace metadata needed to prove dedicated ownership.
type KeyspaceProvider interface {
	LoadKeyspaceByID(spaceID uint32) (*keyspacepb.KeyspaceMeta, error)
}

// RegionProvider supplies the current Region identity and range.
type RegionProvider interface {
	GetRegion(regionID uint64) *core.RegionInfo
	GetRegionByKey(regionKey []byte) *core.RegionInfo
	GetStore(storeID uint64) *core.StoreInfo
}

// Manager is the sole writable authority for Table Group metadata.
type Manager struct {
	mu        syncutil.RWMutex
	storage   endpoint.TableGroupStorage
	allocator id.Allocator
	keyspaces KeyspaceProvider
	regions   RegionProvider

	index      policyIndex
	byKeyspace map[uint32]uint64
	operations map[string]*operationRecord
}

// NewManager loads, validates, and indexes persisted Table Group authority.
func NewManager(
	ctx context.Context,
	storage endpoint.TableGroupStorage,
	allocator id.Allocator,
	keyspaces KeyspaceProvider,
	regions RegionProvider,
) (*Manager, error) {
	if ctx == nil || storage == nil || allocator == nil || keyspaces == nil || regions == nil {
		return nil, invalidArgument("Table Group manager dependencies must not be nil")
	}
	manager := &Manager{
		storage:    storage,
		allocator:  allocator,
		keyspaces:  keyspaces,
		regions:    regions,
		index:      newPolicyIndex(),
		byKeyspace: make(map[uint32]uint64),
		operations: make(map[string]*operationRecord),
	}
	if err := manager.initialize(ctx); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) initialize(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.storage.LoadAllTableGroups(func(recordGroupID uint64, group *table_grouppb.TableGroup) error {
		if err := validateStoredGroup(group); err != nil {
			return err
		}
		groupID := group.GetIdentity().GetTableGroupId()
		if recordGroupID != groupID {
			return invalidArgument("persisted Table Group record key conflicts with identity")
		}
		keyspaceID := group.GetIdentity().GetKeyspaceId()
		if m.index.groups[groupID] != nil {
			return alreadyExists("duplicate persisted Table Group ID", group.GetIdentity())
		}
		if existingGroupID, ok := m.byKeyspace[keyspaceID]; ok && existingGroupID != groupID {
			return alreadyExists("duplicate persisted Table Group keyspace binding", group.GetIdentity())
		}
		if err := m.index.upsert(group); err != nil {
			return err
		}
		m.byKeyspace[keyspaceID] = groupID
		return nil
	}); err != nil {
		return err
	}

	for groupID, group := range m.index.groups {
		err := m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
			indexedGroupID, ok, err := m.storage.LoadTableGroupKeyspaceIndex(txn, group.GetIdentity().GetKeyspaceId())
			if err != nil {
				return err
			}
			if !ok || indexedGroupID != groupID {
				return invalidArgument("persisted Table Group keyspace index conflicts with record")
			}
			bindings, err := fragmentBindingsForGroup(group)
			if err != nil {
				return err
			}
			for _, fragment := range bindings {
				indexedGroupID, ok, err = m.storage.LoadTableGroupRegionIndex(txn, fragment.binding.GetRegionId())
				if err != nil {
					return err
				}
				if !ok || indexedGroupID != groupID {
					return invalidArgument("persisted Table Group Region index conflicts with record")
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	if err := m.storage.LoadAllTableGroupOperations(func(token string, value string) error {
		tokenBytes, err := hex.DecodeString(token)
		if err != nil || tokenHex(tokenBytes) != token {
			return invalidArgument("persisted Table Group operation key is not canonical hexadecimal")
		}
		if err := validateOperationToken(tokenBytes); err != nil {
			return err
		}
		record, err := unmarshalOperationRecord(value)
		if err != nil {
			return err
		}
		group := m.index.groups[record.TableGroupID]
		if group == nil || group.GetIdentity().GetKeyspaceId() != record.KeyspaceID {
			return invalidArgument("persisted Table Group operation references unknown identity")
		}
		if _, ok := m.operations[token]; ok {
			return invalidArgument("duplicate persisted Table Group operation token")
		}
		m.operations[token] = record
		return nil
	}); err != nil {
		return err
	}
	return m.validateOperationHistory()
}

// Create creates or exactly replays one Table Group authority record.
func (m *Manager) Create(ctx context.Context, request *table_grouppb.CreateTableGroupRequest) (*table_grouppb.TableGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateCreateRequest(request); err != nil {
		tableGroupLifecycleCounter.WithLabelValues("create", "invalid").Inc()
		return nil, err
	}
	requestHash, err := createRequestHash(request)
	if err != nil {
		return nil, err
	}
	token := tokenHex(request.GetOperationToken())

	m.mu.RLock()
	if record := m.operations[token]; record != nil {
		group, replayErr := m.resolvePrepareReplay(record, table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE, requestHash, request.GetOperationToken())
		m.mu.RUnlock()
		return group, replayErr
	}
	m.mu.RUnlock()

	regions, err := m.validateCreateTargets(request)
	if err != nil {
		tableGroupLifecycleCounter.WithLabelValues("create", "invalid").Inc()
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if record := m.operations[token]; record != nil {
		return m.resolvePrepareReplay(record, table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE, requestHash, request.GetOperationToken())
	}

	requestedGroupID := request.GetRequestedTableGroupId()
	identity := &table_grouppb.TableGroupIdentity{KeyspaceId: request.GetKeyspaceId(), TableGroupId: requestedGroupID}
	if requestedGroupID != 0 && m.index.groups[requestedGroupID] != nil {
		return nil, alreadyExists("Table Group ID already exists", identity)
	}
	if existingGroupID, ok := m.byKeyspace[request.GetKeyspaceId()]; ok {
		return nil, alreadyExists(fmt.Sprintf("keyspace is already bound to Table Group %d", existingGroupID), identity)
	}
	for _, region := range regions {
		if existingGroupID, ok := m.index.regions[region.GetID()]; ok {
			return nil, alreadyExists(fmt.Sprintf("Region is already bound to Table Group %d", existingGroupID), identity)
		}
	}

	groupID := requestedGroupID
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if groupID == 0 {
		groupID, _, err = m.allocator.Alloc(1)
		if err != nil {
			return nil, err
		}
	} else {
		allocated, err := m.allocator.AllocAt(groupID)
		if err != nil {
			return nil, err
		}
		if !allocated {
			return nil, alreadyExists("requested Table Group ID is already globally reserved", identity)
		}
	}
	identity.TableGroupId = groupID

	group := &table_grouppb.TableGroup{
		Identity:         identity,
		MetadataVersion:  1,
		StatusVersion:    1,
		State:            table_grouppb.TableGroupState_TABLE_GROUP_STATE_CREATING,
		ActiveMembership: &table_grouppb.TableGroupMembership{Version: 1},
		PendingOperation: &table_grouppb.TableGroupOperation{
			Token:                 append([]byte(nil), request.GetOperationToken()...),
			Kind:                  table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE,
			BaseMetadataVersion:   0,
			TargetMetadataVersion: 1,
		},
		SplitPolicy:     proto.Clone(request.GetSplitPolicy()).(*table_grouppb.SplitPolicy),
		CapacityBudget:  proto.Clone(request.GetCapacityBudget()).(*table_grouppb.CapacityBudget),
		CapacityStatus:  &table_grouppb.CapacityStatus{State: table_grouppb.CapacityState_CAPACITY_STATE_UNKNOWN},
		PlacementIntent: proto.Clone(request.GetPlacementIntent()).(*table_grouppb.PlacementIntent),
	}
	if request.GetRegionBinding() != nil {
		group.RegionBinding = proto.Clone(request.GetRegionBinding()).(*table_grouppb.TableGroupRegionBinding)
	} else {
		group.Partitioning = proto.Clone(request.GetPartitioning()).(*table_grouppb.TableGroupPartitioning)
		group.FragmentBindings = cloneFragmentBindings(request.GetFragmentBindings())
	}
	record := &operationRecord{
		SchemaVersion:         operationRecordSchemaVersion,
		Kind:                  table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE,
		KeyspaceID:            request.GetKeyspaceId(),
		TableGroupID:          groupID,
		BaseMetadataVersion:   0,
		TargetMetadataVersion: 1,
		PrepareHash:           requestHash,
		Outcome:               operationOutcomeCreating,
	}
	recordValue, err := record.marshal()
	if err != nil {
		return nil, err
	}

	err = m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		if value, err := m.storage.LoadTableGroupOperation(txn, token); err != nil {
			return err
		} else if value != "" {
			return operationConflict(identity, request.GetOperationToken(), "operation token already exists")
		}
		if existing, err := m.storage.LoadTableGroup(txn, groupID); err != nil {
			return err
		} else if existing != nil {
			return alreadyExists("Table Group ID already exists", identity)
		}
		if indexedGroupID, ok, err := m.storage.LoadTableGroupKeyspaceIndex(txn, request.GetKeyspaceId()); err != nil {
			return err
		} else if ok {
			return alreadyExists(fmt.Sprintf("keyspace is already bound to Table Group %d", indexedGroupID), identity)
		}
		for _, region := range regions {
			if indexedGroupID, ok, err := m.storage.LoadTableGroupRegionIndex(txn, region.GetID()); err != nil {
				return err
			} else if ok {
				return alreadyExists(fmt.Sprintf("Region is already bound to Table Group %d", indexedGroupID), identity)
			}
		}
		if err := m.storage.SaveTableGroup(txn, group); err != nil {
			return err
		}
		if err := m.storage.SaveTableGroupKeyspaceIndex(txn, request.GetKeyspaceId(), groupID); err != nil {
			return err
		}
		for _, region := range regions {
			if err := m.storage.SaveTableGroupRegionIndex(txn, region.GetID(), groupID); err != nil {
				return err
			}
		}
		return m.storage.SaveTableGroupOperation(txn, token, recordValue)
	})
	if err != nil {
		tableGroupLifecycleCounter.WithLabelValues("create", "error").Inc()
		return nil, err
	}
	if err := m.index.upsert(group); err != nil {
		return nil, err
	}
	m.byKeyspace[request.GetKeyspaceId()] = groupID
	m.operations[token] = record
	tableGroupLifecycleCounter.WithLabelValues("create", "created").Inc()
	return cloneGroup(group), nil
}

func (m *Manager) validateCreateTargets(request *table_grouppb.CreateTableGroupRequest) ([]*core.RegionInfo, error) {
	if request.GetKeyspaceId() == keyspace.GetBootstrapKeyspaceID() {
		return nil, keyspaceMismatch(nil, "bootstrap keyspace cannot become a Milestone-1 Table Group")
	}
	meta, err := m.keyspaces.LoadKeyspaceByID(request.GetKeyspaceId())
	if err != nil {
		return nil, keyspaceMismatch(nil, "keyspace does not exist")
	}
	if meta.GetState() != keyspacepb.KeyspaceState_ENABLED {
		return nil, keyspaceMismatch(nil, "keyspace must be enabled")
	}
	if meta.GetConfig()[keyspace.RegionBoundType] != "txn" {
		return nil, keyspaceMismatch(nil, "Table Group keyspace must use transactional Region bounds")
	}
	bound := keyspace.MakeRegionBound(request.GetKeyspaceId())
	bindings, err := fragmentBindingsForCreate(request)
	if err != nil {
		return nil, err
	}
	regions := make([]*core.RegionInfo, 0, len(bindings))
	for index, fragment := range bindings {
		region := m.regions.GetRegion(fragment.binding.GetRegionId())
		if region == nil || region.GetID() != fragment.binding.GetRegionId() {
			return nil, regionMismatch(nil, "requested Table Group fragment Region is unavailable")
		}
		if byKey := m.regions.GetRegionByKey(region.GetStartKey()); byKey == nil || byKey.GetID() != region.GetID() {
			return nil, regionMismatch(nil, "requested Table Group fragment is not the current key-range owner")
		}
		if !proto.Equal(region.GetRegionEpoch(), fragment.binding.GetRegionEpoch()) {
			return nil, epochMismatch(nil, "requested Table Group fragment Region epoch is stale")
		}
		if len(region.GetPeers()) == 0 || region.GetLeader() == nil {
			return nil, regionMismatch(nil, "requested Table Group fragment Region is not eligible")
		}
		if region.GetMeta().GetTableGroup() != nil {
			return nil, regionMismatch(nil, "requested Table Group fragment Region already contains a mirror")
		}
		if index == 0 {
			if !bytes.Equal(region.GetStartKey(), bound.TxnLeftBound) {
				return nil, regionMismatch(nil, "Table Group fragments do not start at the transactional keyspace bound")
			}
		} else if !bytes.Equal(regions[index-1].GetEndKey(), region.GetStartKey()) {
			return nil, regionMismatch(nil, "Table Group fragment Regions are not contiguous")
		}
		regions = append(regions, region)
	}
	if !bytes.Equal(regions[len(regions)-1].GetEndKey(), bound.TxnRightBound) {
		return nil, regionMismatch(nil, "Table Group fragments do not end at the transactional keyspace bound")
	}
	return regions, nil
}

// Get returns the current snapshot for a complete identity.
func (m *Manager) Get(identity *table_grouppb.TableGroupIdentity) (*table_grouppb.TableGroup, error) {
	if err := validateIdentity(identity); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	group := m.index.groups[identity.GetTableGroupId()]
	if group == nil {
		return nil, notFound(identity)
	}
	if group.GetIdentity().GetKeyspaceId() != identity.GetKeyspaceId() {
		return nil, keyspaceMismatch(identity, "Table Group belongs to another keyspace")
	}
	return cloneGroup(group), nil
}

// GetByRegion returns the current snapshot bound to a Region in one keyspace.
func (m *Manager) GetByRegion(keyspaceID uint32, regionID uint64) (*table_grouppb.TableGroup, error) {
	if regionID == 0 {
		return nil, invalidArgument("Table Group Region ID must be non-zero")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	groupID, ok := m.index.regions[regionID]
	if !ok {
		return nil, notFound(nil)
	}
	group := m.index.groups[groupID]
	if group == nil {
		return nil, notFound(nil)
	}
	if group.GetIdentity().GetKeyspaceId() != keyspaceID {
		return nil, keyspaceMismatch(group.GetIdentity(), "Region belongs to a Table Group in another keyspace")
	}
	return cloneGroup(group), nil
}

// Reconcile returns the current result of an exact persisted operation token.
func (m *Manager) Reconcile(identity *table_grouppb.TableGroupIdentity, token []byte) (*table_grouppb.TableGroup, error) {
	if err := validateIdentity(identity); err != nil {
		return nil, err
	}
	if err := validateOperationToken(token); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	record := m.operations[tokenHex(token)]
	if record == nil {
		return nil, notFound(identity)
	}
	if record.KeyspaceID != identity.GetKeyspaceId() || record.TableGroupID != identity.GetTableGroupId() {
		return nil, operationConflict(identity, token, "operation token belongs to another Table Group")
	}
	group := m.index.groups[record.TableGroupID]
	if group == nil {
		return nil, notFound(identity)
	}
	return cloneGroup(group), nil
}

func (m *Manager) resolvePrepareReplay(
	record *operationRecord,
	kind table_grouppb.TableGroupOperationKind,
	requestHash string,
	token []byte,
) (*table_grouppb.TableGroup, error) {
	if record.Kind != kind || record.PrepareHash != requestHash {
		return nil, operationConflict(record.identity(), token, "operation token was reused with different content")
	}
	group := m.index.groups[record.TableGroupID]
	if group == nil {
		return nil, notFound(record.identity())
	}
	tableGroupLifecycleCounter.WithLabelValues(kind.String(), "replay").Inc()
	return cloneGroup(group), nil
}

func samePersistedGroup(persisted, expected *table_grouppb.TableGroup) error {
	if persisted == nil {
		return notFound(expected.GetIdentity())
	}
	if !proto.Equal(persisted, expected) {
		return staleMetadata(expected.GetIdentity(), expected.GetMetadataVersion(), persisted.GetMetadataVersion())
	}
	return nil
}

func sameOperationRecord(value string, expected *operationRecord) error {
	if value == "" {
		return operationConflict(expected.identity(), nil, "operation history is missing")
	}
	persisted, err := unmarshalOperationRecord(value)
	if err != nil {
		return err
	}
	if !equalOperationRecord(persisted, expected) {
		return operationConflict(expected.identity(), nil, "operation history changed concurrently")
	}
	return nil
}

var _ SplitPolicyProvider = (*Manager)(nil)

// EnsureTableGroupSplitAllowed implements SplitPolicyProvider.
func (m *Manager) EnsureTableGroupSplitAllowed(region *metapb.Region, source table_grouppb.SplitSource) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.index.ensureSplitAllowed(region, source)
}
