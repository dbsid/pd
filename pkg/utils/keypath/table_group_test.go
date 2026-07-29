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

package keypath

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableGroupPathsAreClusterScopedAndFixedWidth(t *testing.T) {
	SetClusterID(42)
	t.Cleanup(ResetClusterID)
	require.Equal(t, "/pd/42/table_groups/records/00000000000000000007", TableGroupRecordPath(7))
	require.Equal(t, "/pd/42/table_groups/records/", TableGroupRecordsPrefix())
	require.Equal(t, "/pd/42/table_groups/keyspaces/00000003", TableGroupKeyspaceIndexPath(3))
	require.Equal(t, "/pd/42/table_groups/regions/00000000000000000009", TableGroupRegionIndexPath(9))
	require.Equal(t, "/pd/42/table_groups/operations/0102", TableGroupOperationPath("0102"))
	groupID, err := ExtractTableGroupIDFromRecordPath(TableGroupRecordPath(7))
	require.NoError(t, err)
	require.Equal(t, uint64(7), groupID)
	_, err = ExtractTableGroupIDFromRecordPath("/wrong/7")
	require.Error(t, err)
}
