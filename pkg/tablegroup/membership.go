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

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/storage/kv"
)

// PrepareMembership stores a complete next membership without publishing it.
func (m *Manager) PrepareMembership(ctx context.Context, request *table_grouppb.PrepareMembershipRequest) (*table_grouppb.TableGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidArgument("missing prepare membership request")
	}
	if err := validateIdentity(request.GetIdentity()); err != nil {
		return nil, err
	}
	if err := validateOperationToken(request.GetOperationToken()); err != nil {
		return nil, err
	}
	if err := validateMembership(request.GetProposedMembership()); err != nil {
		return nil, err
	}
	requestHash, err := prepareRequestHash(request)
	if err != nil {
		return nil, err
	}
	token := tokenHex(request.GetOperationToken())

	m.mu.Lock()
	defer m.mu.Unlock()
	if record := m.operations[token]; record != nil {
		return m.resolvePrepareReplay(record, table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_UPDATE_MEMBERSHIP, requestHash, request.GetOperationToken())
	}
	current, err := m.getLocked(request.GetIdentity())
	if err != nil {
		return nil, err
	}
	if current.GetState() != table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE {
		return nil, invalidState(current.GetIdentity(), "Table Group is not active")
	}
	if request.GetExpectedMetadataVersion() != current.GetMetadataVersion() {
		return nil, staleMetadata(current.GetIdentity(), request.GetExpectedMetadataVersion(), current.GetMetadataVersion())
	}
	if request.GetProposedMembership().GetVersion() != current.GetActiveMembership().GetVersion()+1 {
		return nil, membershipConflict(current.GetIdentity(), "proposed membership version is not active version plus one")
	}

	next := cloneGroup(current)
	next.MetadataVersion++
	next.State = table_grouppb.TableGroupState_TABLE_GROUP_STATE_UPDATING_MEMBERSHIP
	next.PreparedMembership = proto.Clone(request.GetProposedMembership()).(*table_grouppb.TableGroupMembership)
	next.PendingOperation = &table_grouppb.TableGroupOperation{
		Token:                 append([]byte(nil), request.GetOperationToken()...),
		Kind:                  table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_UPDATE_MEMBERSHIP,
		BaseMetadataVersion:   current.GetMetadataVersion(),
		TargetMetadataVersion: next.GetMetadataVersion(),
	}
	record := &operationRecord{
		SchemaVersion:         operationRecordSchemaVersion,
		Kind:                  table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_UPDATE_MEMBERSHIP,
		KeyspaceID:            current.GetIdentity().GetKeyspaceId(),
		TableGroupID:          current.GetIdentity().GetTableGroupId(),
		BaseMetadataVersion:   current.GetMetadataVersion(),
		TargetMetadataVersion: next.GetMetadataVersion(),
		PrepareHash:           requestHash,
		Outcome:               operationOutcomePrepared,
	}
	if err := m.persistNewOperation(ctx, current, next, token, record); err != nil {
		return nil, err
	}
	if err := m.index.upsert(next); err != nil {
		return nil, err
	}
	m.operations[token] = record
	tableGroupLifecycleCounter.WithLabelValues("prepare_membership", "prepared").Inc()
	return cloneGroup(next), nil
}

// CommitMembership atomically publishes the exact prepared membership.
func (m *Manager) CommitMembership(ctx context.Context, request *table_grouppb.CommitMembershipRequest) (*table_grouppb.TableGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidArgument("missing commit membership request")
	}
	if err := validateIdentity(request.GetIdentity()); err != nil {
		return nil, err
	}
	if err := validateOperationToken(request.GetOperationToken()); err != nil {
		return nil, err
	}
	requestHash, err := commitRequestHash(request)
	if err != nil {
		return nil, err
	}
	return m.finalizeMembership(ctx, request.GetIdentity(), request.GetExpectedMetadataVersion(), request.GetMembershipVersion(),
		request.GetOperationToken(), finalizationCommit, requestHash)
}

// AbortMembership discards only the exact prepared membership.
func (m *Manager) AbortMembership(ctx context.Context, request *table_grouppb.AbortMembershipRequest) (*table_grouppb.TableGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidArgument("missing abort membership request")
	}
	if err := validateIdentity(request.GetIdentity()); err != nil {
		return nil, err
	}
	if err := validateOperationToken(request.GetOperationToken()); err != nil {
		return nil, err
	}
	requestHash, err := abortRequestHash(request)
	if err != nil {
		return nil, err
	}
	return m.finalizeMembership(ctx, request.GetIdentity(), request.GetExpectedMetadataVersion(), request.GetMembershipVersion(),
		request.GetOperationToken(), finalizationAbort, requestHash)
}

func (m *Manager) finalizeMembership(
	ctx context.Context,
	identity *table_grouppb.TableGroupIdentity,
	expectedMetadataVersion, membershipVersion uint64,
	tokenBytes []byte,
	kind finalizationKind,
	requestHash string,
) (*table_grouppb.TableGroup, error) {
	token := tokenHex(tokenBytes)
	m.mu.Lock()
	defer m.mu.Unlock()
	record := m.operations[token]
	if record == nil {
		return nil, operationConflict(identity, tokenBytes, "membership operation token was not prepared")
	}
	if record.Kind != table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_UPDATE_MEMBERSHIP ||
		record.KeyspaceID != identity.GetKeyspaceId() || record.TableGroupID != identity.GetTableGroupId() {
		return nil, operationConflict(identity, tokenBytes, "operation token belongs to another mutation")
	}
	if record.Finalization != finalizationNone {
		if record.Finalization == kind && record.FinalizationHash == requestHash {
			group := m.index.groups[record.TableGroupID]
			if group == nil {
				return nil, notFound(identity)
			}
			tableGroupLifecycleCounter.WithLabelValues(string(kind)+"_membership", "replay").Inc()
			return cloneGroup(group), nil
		}
		return nil, operationConflict(identity, tokenBytes, "membership operation already has another terminal result")
	}
	current, err := m.getLocked(identity)
	if err != nil {
		return nil, err
	}
	if current.GetState() != table_grouppb.TableGroupState_TABLE_GROUP_STATE_UPDATING_MEMBERSHIP {
		return nil, invalidState(identity, "Table Group has no prepared membership")
	}
	if current.GetMetadataVersion() != expectedMetadataVersion {
		return nil, staleMetadata(identity, expectedMetadataVersion, current.GetMetadataVersion())
	}
	if current.GetPreparedMembership().GetVersion() != membershipVersion {
		return nil, membershipConflict(identity, "membership version does not match prepared membership")
	}
	operation := current.GetPendingOperation()
	if operation == nil || !bytes.Equal(operation.GetToken(), tokenBytes) {
		return nil, operationConflict(identity, tokenBytes, "pending operation token does not match")
	}

	next := cloneGroup(current)
	next.MetadataVersion++
	next.State = table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE
	if kind == finalizationCommit {
		next.ActiveMembership = proto.Clone(current.GetPreparedMembership()).(*table_grouppb.TableGroupMembership)
	}
	next.PreparedMembership = nil
	next.PendingOperation = nil
	nextRecord := *record
	nextRecord.Finalization = kind
	nextRecord.FinalizationHash = requestHash
	if kind == finalizationCommit {
		nextRecord.Outcome = operationOutcomeCommitted
	} else {
		nextRecord.Outcome = operationOutcomeAborted
	}
	if err := m.persistExistingOperation(ctx, current, next, token, record, &nextRecord); err != nil {
		return nil, err
	}
	if err := m.index.upsert(next); err != nil {
		return nil, err
	}
	m.operations[token] = &nextRecord
	tableGroupLifecycleCounter.WithLabelValues(string(kind)+"_membership", string(nextRecord.Outcome)).Inc()
	return cloneGroup(next), nil
}

func (m *Manager) getLocked(identity *table_grouppb.TableGroupIdentity) (*table_grouppb.TableGroup, error) {
	group := m.index.groups[identity.GetTableGroupId()]
	if group == nil {
		return nil, notFound(identity)
	}
	if group.GetIdentity().GetKeyspaceId() != identity.GetKeyspaceId() {
		return nil, keyspaceMismatch(identity, "Table Group belongs to another keyspace")
	}
	return group, nil
}

func (m *Manager) persistNewOperation(
	ctx context.Context,
	current, next *table_grouppb.TableGroup,
	token string,
	record *operationRecord,
) error {
	recordValue, err := record.marshal()
	if err != nil {
		return err
	}
	return m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		persisted, err := m.storage.LoadTableGroup(txn, current.GetIdentity().GetTableGroupId())
		if err != nil {
			return err
		}
		if err := samePersistedGroup(persisted, current); err != nil {
			return err
		}
		if value, err := m.storage.LoadTableGroupOperation(txn, token); err != nil {
			return err
		} else if value != "" {
			return operationConflict(current.GetIdentity(), nil, "operation token already exists")
		}
		if err := m.storage.SaveTableGroup(txn, next); err != nil {
			return err
		}
		return m.storage.SaveTableGroupOperation(txn, token, recordValue)
	})
}

func (m *Manager) persistExistingOperation(
	ctx context.Context,
	current, next *table_grouppb.TableGroup,
	token string,
	currentRecord, nextRecord *operationRecord,
) error {
	nextRecordValue, err := nextRecord.marshal()
	if err != nil {
		return err
	}
	return m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		persisted, err := m.storage.LoadTableGroup(txn, current.GetIdentity().GetTableGroupId())
		if err != nil {
			return err
		}
		if err := samePersistedGroup(persisted, current); err != nil {
			return err
		}
		value, err := m.storage.LoadTableGroupOperation(txn, token)
		if err != nil {
			return err
		}
		if err := sameOperationRecord(value, currentRecord); err != nil {
			return err
		}
		if err := m.storage.SaveTableGroup(txn, next); err != nil {
			return err
		}
		return m.storage.SaveTableGroupOperation(txn, token, nextRecordValue)
	})
}
