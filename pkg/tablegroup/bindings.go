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
	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/table_grouppb"
)

const maxTableGroupFragments = 1024

type fragmentBinding struct {
	fragmentID uint32
	binding    *table_grouppb.TableGroupRegionBinding
}

func fragmentBindingsForCreate(request *table_grouppb.CreateTableGroupRequest) ([]fragmentBinding, error) {
	if request == nil {
		return nil, invalidArgument("missing create table group request")
	}
	return normalizeFragmentBindings(
		request.GetPartitioning(),
		request.GetFragmentBindings(),
		request.GetRegionBinding(),
		0,
		true,
	)
}

func fragmentBindingsForGroup(group *table_grouppb.TableGroup) ([]fragmentBinding, error) {
	if group == nil {
		return nil, invalidArgument("stored Table Group is nil")
	}
	return normalizeFragmentBindings(
		group.GetPartitioning(),
		group.GetFragmentBindings(),
		group.GetRegionBinding(),
		group.GetMetadataVersion(),
		false,
	)
}

func normalizeFragmentBindings(
	partitioning *table_grouppb.TableGroupPartitioning,
	fragments []*table_grouppb.TableGroupFragmentBinding,
	legacy *table_grouppb.TableGroupRegionBinding,
	metadataVersion uint64,
	create bool,
) ([]fragmentBinding, error) {
	if partitioning == nil {
		if len(fragments) != 0 {
			return nil, invalidArgument("legacy Table Group cannot contain fragment bindings")
		}
		if err := validateRegionBinding(legacy, metadataVersion, create); err != nil {
			return nil, err
		}
		return []fragmentBinding{{fragmentID: 0, binding: legacy}}, nil
	}
	if legacy != nil {
		return nil, invalidArgument("new-format Table Group cannot contain legacy region binding")
	}
	count := partitioning.GetPartitionCount()
	if count == 0 || count > maxTableGroupFragments {
		return nil, invalidArgument("Table Group partition count must contain 1 to 1024 fragments")
	}
	switch partitioning.GetMethod() {
	case table_grouppb.TableGroupPartitionMethod_TABLE_GROUP_PARTITION_METHOD_SINGLE:
		if count != 1 || partitioning.GetHashAlgorithm() != table_grouppb.TableGroupHashAlgorithm_TABLE_GROUP_HASH_ALGORITHM_UNSPECIFIED {
			return nil, invalidArgument("single Table Group partitioning requires one fragment and no hash algorithm")
		}
	case table_grouppb.TableGroupPartitionMethod_TABLE_GROUP_PARTITION_METHOD_HASH:
		if count < 2 || partitioning.GetHashAlgorithm() != table_grouppb.TableGroupHashAlgorithm_TABLE_GROUP_HASH_ALGORITHM_MODULO_U64_V1 {
			return nil, invalidArgument("hash Table Group partitioning requires at least two fragments and modulo-u64-v1")
		}
	default:
		return nil, invalidArgument("Table Group partition method is unspecified or unknown")
	}
	if len(fragments) != int(count) {
		return nil, invalidArgument("Table Group fragment binding count does not match partition count")
	}

	bindings := make([]fragmentBinding, 0, count)
	regionIDs := make(map[uint64]struct{}, count)
	shardIDs := make(map[uint64]struct{}, count)
	for index, fragment := range fragments {
		if fragment == nil || fragment.GetFragmentId() != uint32(index) {
			return nil, invalidArgument("Table Group fragment IDs must be zero-based, contiguous, and ordered")
		}
		binding := fragment.GetRegionBinding()
		if err := validateRegionBinding(binding, metadataVersion, create); err != nil {
			return nil, err
		}
		if _, exists := regionIDs[binding.GetRegionId()]; exists {
			return nil, invalidArgument("Table Group fragment Region IDs must be unique")
		}
		if _, exists := shardIDs[binding.GetShardId()]; exists {
			return nil, invalidArgument("Table Group fragment Shard IDs must be unique")
		}
		regionIDs[binding.GetRegionId()] = struct{}{}
		shardIDs[binding.GetShardId()] = struct{}{}
		bindings = append(bindings, fragmentBinding{fragmentID: fragment.GetFragmentId(), binding: binding})
	}
	return bindings, nil
}

func validateRegionBinding(
	binding *table_grouppb.TableGroupRegionBinding,
	metadataVersion uint64,
	create bool,
) error {
	if binding == nil || binding.GetRegionId() == 0 || binding.GetShardId() == 0 {
		return invalidArgument("region binding requires non-zero Region and Shard IDs")
	}
	if binding.GetRegionEpoch() == nil || binding.GetRegionEpoch().GetVersion() == 0 || binding.GetRegionEpoch().GetConfVer() == 0 {
		return invalidArgument("region binding requires a complete non-zero epoch")
	}
	if create && binding.GetAppliedMetadataVersion() != 0 {
		return invalidArgument("create region binding must not claim an applied metadata version")
	}
	if !create && binding.GetAppliedMetadataVersion() > metadataVersion {
		return invalidArgument("stored applied metadata version exceeds authority")
	}
	return nil
}

func findFragmentBinding(group *table_grouppb.TableGroup, regionID uint64) (fragmentBinding, bool) {
	bindings, err := fragmentBindingsForGroup(group)
	if err != nil {
		return fragmentBinding{}, false
	}
	for _, candidate := range bindings {
		if candidate.binding.GetRegionId() == regionID {
			return candidate, true
		}
	}
	return fragmentBinding{}, false
}

func cloneFragmentBindings(bindings []*table_grouppb.TableGroupFragmentBinding) []*table_grouppb.TableGroupFragmentBinding {
	clones := make([]*table_grouppb.TableGroupFragmentBinding, len(bindings))
	for index, binding := range bindings {
		clones[index] = proto.Clone(binding).(*table_grouppb.TableGroupFragmentBinding)
	}
	return clones
}
