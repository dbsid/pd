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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/table_grouppb"
)

const operationRecordSchemaVersion = 1

type operationOutcome string

const (
	operationOutcomeCreating  operationOutcome = "creating"
	operationOutcomeActive    operationOutcome = "active"
	operationOutcomePrepared  operationOutcome = "prepared"
	operationOutcomeCommitted operationOutcome = "committed"
	operationOutcomeAborted   operationOutcome = "aborted"
)

type finalizationKind string

const (
	finalizationNone   finalizationKind = ""
	finalizationCommit finalizationKind = "commit"
	finalizationAbort  finalizationKind = "abort"
)

type operationRecord struct {
	SchemaVersion         int                                   `json:"schema_version"`
	Kind                  table_grouppb.TableGroupOperationKind `json:"kind"`
	KeyspaceID            uint32                                `json:"keyspace_id"`
	TableGroupID          uint64                                `json:"table_group_id"`
	BaseMetadataVersion   uint64                                `json:"base_metadata_version"`
	TargetMetadataVersion uint64                                `json:"target_metadata_version"`
	PrepareHash           string                                `json:"prepare_hash"`
	Finalization          finalizationKind                      `json:"finalization,omitempty"`
	FinalizationHash      string                                `json:"finalization_hash,omitempty"`
	Outcome               operationOutcome                      `json:"outcome"`
}

func (r *operationRecord) identity() *table_grouppb.TableGroupIdentity {
	return &table_grouppb.TableGroupIdentity{
		KeyspaceId:   r.KeyspaceID,
		TableGroupId: r.TableGroupID,
	}
}

func (r *operationRecord) marshal() (string, error) {
	value, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func unmarshalOperationRecord(value string) (*operationRecord, error) {
	record := &operationRecord{}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(record); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, invalidArgument("multiple table group operation records")
		}
		return nil, err
	}
	if record.SchemaVersion != operationRecordSchemaVersion {
		return nil, invalidArgument("unsupported table group operation record schema")
	}
	if record.TableGroupID == 0 || !validSemanticHash(record.PrepareHash) {
		return nil, invalidArgument("incomplete table group operation record")
	}
	switch record.Kind {
	case table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE:
		if record.BaseMetadataVersion != 0 || record.TargetMetadataVersion != 1 ||
			record.Finalization != finalizationNone || record.FinalizationHash != "" ||
			(record.Outcome != operationOutcomeCreating && record.Outcome != operationOutcomeActive) {
			return nil, invalidArgument("inconsistent create operation record")
		}
	case table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_UPDATE_MEMBERSHIP:
		if record.BaseMetadataVersion == 0 || record.TargetMetadataVersion == 0 ||
			record.TargetMetadataVersion != record.BaseMetadataVersion+1 {
			return nil, invalidArgument("inconsistent membership operation versions")
		}
		switch record.Finalization {
		case finalizationNone:
			if record.FinalizationHash != "" || record.Outcome != operationOutcomePrepared {
				return nil, invalidArgument("inconsistent prepared membership operation record")
			}
		case finalizationCommit:
			if !validSemanticHash(record.FinalizationHash) || record.Outcome != operationOutcomeCommitted {
				return nil, invalidArgument("inconsistent committed membership operation record")
			}
		case finalizationAbort:
			if !validSemanticHash(record.FinalizationHash) || record.Outcome != operationOutcomeAborted {
				return nil, invalidArgument("inconsistent aborted membership operation record")
			}
		default:
			return nil, invalidArgument("unsupported membership finalization kind")
		}
	default:
		return nil, invalidArgument("unsupported table group operation kind")
	}
	return record, nil
}

func validSemanticHash(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func equalOperationRecord(left, right *operationRecord) bool {
	return left != nil && right != nil &&
		left.SchemaVersion == right.SchemaVersion &&
		left.Kind == right.Kind &&
		left.KeyspaceID == right.KeyspaceID &&
		left.TableGroupID == right.TableGroupID &&
		left.BaseMetadataVersion == right.BaseMetadataVersion &&
		left.TargetMetadataVersion == right.TargetMetadataVersion &&
		left.PrepareHash == right.PrepareHash &&
		left.Finalization == right.Finalization &&
		left.FinalizationHash == right.FinalizationHash &&
		left.Outcome == right.Outcome
}

func (m *Manager) validateOperationHistory() error {
	createRecords := make(map[uint64]int, len(m.index.groups))
	for token, record := range m.operations {
		group := m.index.groups[record.TableGroupID]
		if group == nil || group.GetIdentity().GetKeyspaceId() != record.KeyspaceID {
			return invalidArgument("persisted Table Group operation references unknown identity")
		}
		if err := validateOperationAgainstGroup(token, record, group); err != nil {
			return err
		}
		if record.Kind == table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE {
			createRecords[record.TableGroupID]++
		}
	}

	for groupID, group := range m.index.groups {
		if createRecords[groupID] != 1 {
			return invalidArgument("persisted Table Group requires exactly one create operation record")
		}
		pending := group.GetPendingOperation()
		if pending == nil {
			continue
		}
		record := m.operations[tokenHex(pending.GetToken())]
		if record == nil || record.Kind != pending.GetKind() ||
			record.BaseMetadataVersion != pending.GetBaseMetadataVersion() ||
			record.TargetMetadataVersion != pending.GetTargetMetadataVersion() {
			return invalidArgument("persisted Table Group pending operation conflicts with operation history")
		}
	}
	return nil
}

func validateOperationAgainstGroup(token string, record *operationRecord, group *table_grouppb.TableGroup) error {
	pending := group.GetPendingOperation()
	switch record.Kind {
	case table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE:
		if group.GetState() == table_grouppb.TableGroupState_TABLE_GROUP_STATE_CREATING {
			if record.Outcome != operationOutcomeCreating || pending == nil ||
				pending.GetKind() != record.Kind || tokenHex(pending.GetToken()) != token ||
				pending.GetBaseMetadataVersion() != record.BaseMetadataVersion ||
				pending.GetTargetMetadataVersion() != record.TargetMetadataVersion {
				return invalidArgument("persisted creating Table Group conflicts with create operation history")
			}
			return nil
		}
		if record.Outcome != operationOutcomeActive || group.GetMetadataVersion() < record.TargetMetadataVersion+1 {
			return invalidArgument("persisted Table Group conflicts with completed create operation history")
		}
	case table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_UPDATE_MEMBERSHIP:
		switch record.Outcome {
		case operationOutcomePrepared:
			if group.GetState() != table_grouppb.TableGroupState_TABLE_GROUP_STATE_UPDATING_MEMBERSHIP ||
				pending == nil || pending.GetKind() != record.Kind || tokenHex(pending.GetToken()) != token ||
				pending.GetBaseMetadataVersion() != record.BaseMetadataVersion ||
				pending.GetTargetMetadataVersion() != record.TargetMetadataVersion ||
				group.GetMetadataVersion() != record.TargetMetadataVersion {
				return invalidArgument("persisted updating Table Group conflicts with prepared operation history")
			}
		case operationOutcomeCommitted, operationOutcomeAborted:
			if group.GetMetadataVersion() < record.TargetMetadataVersion+1 ||
				(pending != nil && tokenHex(pending.GetToken()) == token) {
				return invalidArgument("persisted Table Group conflicts with finalized membership operation history")
			}
		default:
			return invalidArgument("persisted membership operation has invalid outcome")
		}
	default:
		return invalidArgument("persisted Table Group operation has invalid kind")
	}
	return nil
}

func tokenHex(token []byte) string {
	return hex.EncodeToString(token)
}

func semanticHash(message proto.Message) (string, error) {
	value, err := proto.Marshal(message)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(value)
	return hex.EncodeToString(hash[:]), nil
}

func createRequestHash(request *table_grouppb.CreateTableGroupRequest) (string, error) {
	clone := proto.Clone(request).(*table_grouppb.CreateTableGroupRequest)
	clone.Header = nil
	clone.OperationToken = nil
	return semanticHash(clone)
}

func prepareRequestHash(request *table_grouppb.PrepareMembershipRequest) (string, error) {
	clone := proto.Clone(request).(*table_grouppb.PrepareMembershipRequest)
	clone.Header = nil
	clone.OperationToken = nil
	return semanticHash(clone)
}

func commitRequestHash(request *table_grouppb.CommitMembershipRequest) (string, error) {
	clone := proto.Clone(request).(*table_grouppb.CommitMembershipRequest)
	clone.Header = nil
	clone.OperationToken = nil
	return semanticHash(clone)
}

func abortRequestHash(request *table_grouppb.AbortMembershipRequest) (string, error) {
	clone := proto.Clone(request).(*table_grouppb.AbortMembershipRequest)
	clone.Header = nil
	clone.OperationToken = nil
	return semanticHash(clone)
}
