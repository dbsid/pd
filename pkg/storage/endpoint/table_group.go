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
	"fmt"
	"strconv"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// TableGroupStorage defines persistence operations used by the Table Group authority.
type TableGroupStorage interface {
	SaveTableGroup(txn kv.Txn, group *table_grouppb.TableGroup) error
	LoadTableGroup(txn kv.Txn, groupID uint64) (*table_grouppb.TableGroup, error)
	LoadAllTableGroups(func(groupID uint64, group *table_grouppb.TableGroup) error) error

	SaveTableGroupKeyspaceIndex(txn kv.Txn, keyspaceID uint32, groupID uint64) error
	LoadTableGroupKeyspaceIndex(txn kv.Txn, keyspaceID uint32) (uint64, bool, error)
	SaveTableGroupRegionIndex(txn kv.Txn, regionID, groupID uint64) error
	LoadTableGroupRegionIndex(txn kv.Txn, regionID uint64) (uint64, bool, error)

	SaveTableGroupOperation(txn kv.Txn, tokenHex, value string) error
	LoadTableGroupOperation(txn kv.Txn, tokenHex string) (string, error)
	LoadAllTableGroupOperations(func(tokenHex, value string) error) error

	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

var _ TableGroupStorage = (*StorageEndpoint)(nil)

// SaveTableGroup saves an authoritative snapshot within a transaction.
func (*StorageEndpoint) SaveTableGroup(txn kv.Txn, group *table_grouppb.TableGroup) error {
	value, err := proto.Marshal(group)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return txn.Save(keypath.TableGroupRecordPath(group.GetIdentity().GetTableGroupId()), string(value))
}

// LoadTableGroup loads an authoritative snapshot within a transaction.
func (*StorageEndpoint) LoadTableGroup(txn kv.Txn, groupID uint64) (*table_grouppb.TableGroup, error) {
	value, err := txn.Load(keypath.TableGroupRecordPath(groupID))
	if err != nil || value == "" {
		return nil, err
	}
	group := &table_grouppb.TableGroup{}
	if err := proto.Unmarshal([]byte(value), group); err != nil {
		return nil, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return group, nil
}

// LoadAllTableGroups loads every authoritative snapshot.
func (se *StorageEndpoint) LoadAllTableGroups(f func(groupID uint64, group *table_grouppb.TableGroup) error) error {
	var callbackErr error
	err := se.loadRangeByPrefix(keypath.TableGroupRecordsPrefix(), func(recordID, value string) {
		if callbackErr != nil {
			return
		}
		groupID, err := strconv.ParseUint(recordID, 10, 64)
		if err != nil || recordID != fmt.Sprintf("%020d", groupID) {
			callbackErr = fmt.Errorf("invalid Table Group record ID %q", recordID)
			return
		}
		group := &table_grouppb.TableGroup{}
		if err := proto.Unmarshal([]byte(value), group); err != nil {
			callbackErr = errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByCause()
			return
		}
		callbackErr = f(groupID, group)
	})
	if err != nil {
		return err
	}
	return callbackErr
}

// SaveTableGroupKeyspaceIndex saves the unique keyspace-to-group mapping.
func (*StorageEndpoint) SaveTableGroupKeyspaceIndex(txn kv.Txn, keyspaceID uint32, groupID uint64) error {
	return txn.Save(keypath.TableGroupKeyspaceIndexPath(keyspaceID), strconv.FormatUint(groupID, 10))
}

// LoadTableGroupKeyspaceIndex loads the unique keyspace-to-group mapping.
func (*StorageEndpoint) LoadTableGroupKeyspaceIndex(txn kv.Txn, keyspaceID uint32) (uint64, bool, error) {
	return loadTableGroupIndex(txn, keypath.TableGroupKeyspaceIndexPath(keyspaceID))
}

// SaveTableGroupRegionIndex saves the unique Region-to-group mapping.
func (*StorageEndpoint) SaveTableGroupRegionIndex(txn kv.Txn, regionID, groupID uint64) error {
	return txn.Save(keypath.TableGroupRegionIndexPath(regionID), strconv.FormatUint(groupID, 10))
}

// LoadTableGroupRegionIndex loads the unique Region-to-group mapping.
func (*StorageEndpoint) LoadTableGroupRegionIndex(txn kv.Txn, regionID uint64) (uint64, bool, error) {
	return loadTableGroupIndex(txn, keypath.TableGroupRegionIndexPath(regionID))
}

func loadTableGroupIndex(txn kv.Txn, indexPath string) (uint64, bool, error) {
	value, err := txn.Load(indexPath)
	if err != nil || value == "" {
		return 0, false, err
	}
	groupID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return groupID, true, nil
}

// SaveTableGroupOperation saves a raw, versioned operation-history value.
func (*StorageEndpoint) SaveTableGroupOperation(txn kv.Txn, tokenHex, value string) error {
	return txn.Save(keypath.TableGroupOperationPath(tokenHex), value)
}

// LoadTableGroupOperation loads a raw operation-history value.
func (*StorageEndpoint) LoadTableGroupOperation(txn kv.Txn, tokenHex string) (string, error) {
	return txn.Load(keypath.TableGroupOperationPath(tokenHex))
}

// LoadAllTableGroupOperations loads every raw operation-history value.
func (se *StorageEndpoint) LoadAllTableGroupOperations(f func(tokenHex, value string) error) error {
	var callbackErr error
	err := se.loadRangeByPrefix(keypath.TableGroupOperationsPrefix(), func(tokenHex, value string) {
		if callbackErr == nil {
			callbackErr = f(tokenHex, value)
		}
	})
	if err != nil {
		return err
	}
	return callbackErr
}
