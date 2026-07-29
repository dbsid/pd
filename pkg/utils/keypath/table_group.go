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
	"fmt"
	"strconv"
	"strings"
)

// TableGroupRecordPath returns the path of an authoritative Table Group record.
func TableGroupRecordPath(groupID uint64) string {
	return fmt.Sprintf(tableGroupRecordPathFormat, ClusterID(), groupID)
}

// TableGroupRecordsPrefix returns the prefix of all authoritative Table Group records.
func TableGroupRecordsPrefix() string {
	return TableGroupRecordPath(0)[:len(TableGroupRecordPath(0))-20]
}

// ExtractTableGroupIDFromRecordPath extracts a Table Group ID from a record path.
func ExtractTableGroupIDFromRecordPath(recordPath string) (uint64, error) {
	prefix := TableGroupRecordsPrefix()
	if !strings.HasPrefix(recordPath, prefix) {
		return 0, fmt.Errorf("invalid table group record path %q", recordPath)
	}
	return strconv.ParseUint(strings.TrimPrefix(recordPath, prefix), 10, 64)
}

// TableGroupKeyspaceIndexPath returns the unique-index path for a keyspace.
func TableGroupKeyspaceIndexPath(keyspaceID uint32) string {
	return fmt.Sprintf(tableGroupKeyspaceIndexPathFormat, ClusterID(), keyspaceID)
}

// TableGroupRegionIndexPath returns the unique-index path for a Region.
func TableGroupRegionIndexPath(regionID uint64) string {
	return fmt.Sprintf(tableGroupRegionIndexPathFormat, ClusterID(), regionID)
}

// TableGroupOperationPath returns the path of a persistent idempotency record.
// tokenHex must be the lowercase hexadecimal encoding of the opaque operation token.
func TableGroupOperationPath(tokenHex string) string {
	return fmt.Sprintf(tableGroupOperationPathFormat, ClusterID(), tokenHex)
}

// TableGroupOperationsPrefix returns the prefix of all idempotency records.
func TableGroupOperationsPrefix() string {
	return TableGroupOperationPath("")
}
