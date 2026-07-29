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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/table_grouppb"
)

func TestTableGroupOperationRecordValidation(t *testing.T) {
	hash := strings.Repeat("01", 32)
	create := operationRecord{
		SchemaVersion:         operationRecordSchemaVersion,
		Kind:                  table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE,
		KeyspaceID:            1,
		TableGroupID:          2,
		TargetMetadataVersion: 1,
		PrepareHash:           hash,
		Outcome:               operationOutcomeCreating,
	}
	prepared := operationRecord{
		SchemaVersion:         operationRecordSchemaVersion,
		Kind:                  table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_UPDATE_MEMBERSHIP,
		KeyspaceID:            1,
		TableGroupID:          2,
		BaseMetadataVersion:   2,
		TargetMetadataVersion: 3,
		PrepareHash:           hash,
		Outcome:               operationOutcomePrepared,
	}
	committed := prepared
	committed.Finalization = finalizationCommit
	committed.FinalizationHash = hash
	committed.Outcome = operationOutcomeCommitted
	aborted := prepared
	aborted.Finalization = finalizationAbort
	aborted.FinalizationHash = hash
	aborted.Outcome = operationOutcomeAborted

	for _, record := range []*operationRecord{&create, &prepared, &committed, &aborted} {
		value, err := record.marshal()
		require.NoError(t, err)
		loaded, err := unmarshalOperationRecord(value)
		require.NoError(t, err)
		require.True(t, equalOperationRecord(record, loaded))
	}

	tests := []struct {
		name   string
		mutate func(*operationRecord)
	}{
		{name: "unknown kind", mutate: func(record *operationRecord) { record.Kind = 99 }},
		{name: "malformed prepare hash", mutate: func(record *operationRecord) { record.PrepareHash = "ABC" }},
		{name: "invalid create versions", mutate: func(record *operationRecord) { record.BaseMetadataVersion = 1 }},
		{name: "create finalization", mutate: func(record *operationRecord) { record.Finalization = finalizationCommit }},
		{name: "create membership outcome", mutate: func(record *operationRecord) { record.Outcome = operationOutcomeCommitted }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := create
			test.mutate(&record)
			value, err := record.marshal()
			require.NoError(t, err)
			_, err = unmarshalOperationRecord(value)
			require.Error(t, err)
		})
	}

	invalidMembership := prepared
	invalidMembership.Finalization = finalizationCommit
	invalidMembership.FinalizationHash = hash
	value, err := invalidMembership.marshal()
	require.NoError(t, err)
	_, err = unmarshalOperationRecord(value)
	require.Error(t, err)

	_, err = unmarshalOperationRecord(`{"schema_version":1,"unknown":true}`)
	require.Error(t, err)
	validJSON, err := create.marshal()
	require.NoError(t, err)
	_, err = unmarshalOperationRecord(validJSON + validJSON)
	require.Error(t, err)
}
