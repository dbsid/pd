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

package endpoint

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/storage/kv"
)

func TestTableGroupStorageRoundTripsRecordIndexesAndOperation(t *testing.T) {
	store := NewStorageEndpoint(kv.NewMemoryKV(), nil)
	group := &table_grouppb.TableGroup{
		Identity:        &table_grouppb.TableGroupIdentity{KeyspaceId: 1, TableGroupId: 2},
		MetadataVersion: 3,
		StatusVersion:   4,
	}
	err := store.RunInTxn(context.Background(), func(txn kv.Txn) error {
		if err := store.SaveTableGroup(txn, group); err != nil {
			return err
		}
		if err := store.SaveTableGroupKeyspaceIndex(txn, 1, 2); err != nil {
			return err
		}
		if err := store.SaveTableGroupRegionIndex(txn, 10, 2); err != nil {
			return err
		}
		return store.SaveTableGroupOperation(txn, "0102", `{"schema_version":1}`)
	})
	require.NoError(t, err)

	err = store.RunInTxn(context.Background(), func(txn kv.Txn) error {
		loaded, err := store.LoadTableGroup(txn, 2)
		if err != nil {
			return err
		}
		require.True(t, proto.Equal(group, loaded))
		groupID, ok, err := store.LoadTableGroupKeyspaceIndex(txn, 1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint64(2), groupID)
		groupID, ok, err = store.LoadTableGroupRegionIndex(txn, 10)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint64(2), groupID)
		operation, err := store.LoadTableGroupOperation(txn, "0102")
		require.NoError(t, err)
		require.Equal(t, `{"schema_version":1}`, operation)
		return nil
	})
	require.NoError(t, err)

	var groups, operations int
	require.NoError(t, store.LoadAllTableGroups(func(groupID uint64, loaded *table_grouppb.TableGroup) error {
		groups++
		require.Equal(t, uint64(2), groupID)
		require.True(t, proto.Equal(group, loaded))
		return nil
	}))
	require.NoError(t, store.LoadAllTableGroupOperations(func(tokenHex, value string) error {
		operations++
		require.Equal(t, "0102", tokenHex)
		require.Equal(t, `{"schema_version":1}`, value)
		return nil
	}))
	require.Equal(t, 1, groups)
	require.Equal(t, 1, operations)
}
