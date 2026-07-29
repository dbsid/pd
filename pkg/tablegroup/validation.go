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
	"fmt"
	"strings"

	"github.com/pingcap/kvproto/pkg/table_grouppb"
)

const (
	maxOperationTokenBytes  = 64
	maxMembershipMembers    = 1024
	maxMembershipIndexes    = 4096
	maxLocationLabels       = 64
	maxPlacementConstraints = 64
	maxConstraintValues     = 16
	maxCapacityDimensions   = 6
	maxLabelBytes           = 128
)

func validateOperationToken(token []byte) error {
	if len(token) == 0 || len(token) > maxOperationTokenBytes {
		return invalidArgument("operation token must contain 1 to 64 bytes")
	}
	return nil
}

func validateIdentity(identity *table_grouppb.TableGroupIdentity) error {
	if identity == nil || identity.GetTableGroupId() == 0 {
		return invalidArgument("table group identity requires a non-zero group ID")
	}
	return nil
}

func validateCreateRequest(request *table_grouppb.CreateTableGroupRequest) error {
	if request == nil {
		return invalidArgument("missing create table group request")
	}
	if err := validateOperationToken(request.GetOperationToken()); err != nil {
		return err
	}
	binding := request.GetRegionBinding()
	if binding == nil || binding.GetRegionId() == 0 || binding.GetShardId() == 0 {
		return invalidArgument("region binding requires non-zero Region and Shard IDs")
	}
	if binding.GetRegionEpoch() == nil || binding.GetRegionEpoch().GetVersion() == 0 || binding.GetRegionEpoch().GetConfVer() == 0 {
		return invalidArgument("region binding requires a complete non-zero epoch")
	}
	if binding.GetAppliedMetadataVersion() != 0 {
		return invalidArgument("create region binding must not claim an applied metadata version")
	}
	if request.GetSplitPolicy().GetMode() != table_grouppb.SplitPolicyMode_SPLIT_POLICY_MODE_FORBID {
		return invalidArgument("Milestone-1 Table Groups require the forbid-split policy")
	}
	if err := validateCapacityBudget(request.GetCapacityBudget()); err != nil {
		return err
	}
	return validatePlacementIntent(request.GetPlacementIntent())
}

func validateCapacityBudget(budget *table_grouppb.CapacityBudget) error {
	if budget == nil || budget.GetMaxDataBytes() == 0 || budget.GetMaxWriteBytesPerSecond() == 0 ||
		budget.GetMaxRequestsPerSecond() == 0 || budget.GetMaxRaftLogBytes() == 0 ||
		budget.GetMaxSnapshotBytes() == 0 || budget.GetMaxRecoverySeconds() == 0 {
		return invalidArgument("capacity budget requires every finite limit")
	}
	switch budget.GetLimitAction() {
	case table_grouppb.CapacityLimitAction_CAPACITY_LIMIT_ACTION_REJECT,
		table_grouppb.CapacityLimitAction_CAPACITY_LIMIT_ACTION_THROTTLE:
		return nil
	default:
		return invalidArgument("capacity budget requires reject or throttle action")
	}
}

func validatePlacementIntent(intent *table_grouppb.PlacementIntent) error {
	if intent == nil || intent.GetReplicaCount() == 0 {
		return invalidArgument("placement intent requires a non-zero replica count")
	}
	if len(intent.GetLocationLabels()) > maxLocationLabels {
		return invalidArgument("placement intent exceeds 64 location labels")
	}
	replicaConstraints := intent.GetReplicaConstraints()
	leaderConstraints := intent.GetLeaderConstraints()
	if len(replicaConstraints) > maxPlacementConstraints ||
		len(leaderConstraints) > maxPlacementConstraints-len(replicaConstraints) {
		return invalidArgument("placement intent exceeds 64 constraints")
	}
	labels := make(map[string]struct{}, len(intent.GetLocationLabels()))
	for _, label := range intent.GetLocationLabels() {
		if err := validateLabel(label); err != nil {
			return err
		}
		key := strings.ToLower(label)
		if _, ok := labels[key]; ok {
			return invalidArgument("placement location labels must be unique")
		}
		labels[key] = struct{}{}
	}
	for _, constraint := range replicaConstraints {
		if err := validatePlacementConstraint(constraint); err != nil {
			return err
		}
	}
	for _, constraint := range leaderConstraints {
		if err := validatePlacementConstraint(constraint); err != nil {
			return err
		}
	}
	return nil
}

func validatePlacementConstraint(constraint *table_grouppb.PlacementConstraint) error {
	if constraint == nil {
		return invalidArgument("placement constraint is nil")
	}
	if err := validateLabel(constraint.GetKey()); err != nil {
		return err
	}
	if len(constraint.GetValues()) > maxConstraintValues {
		return invalidArgument("placement constraint exceeds 16 values")
	}
	seen := make(map[string]struct{}, len(constraint.GetValues()))
	for _, value := range constraint.GetValues() {
		if err := validateLabel(value); err != nil {
			return err
		}
		if _, ok := seen[value]; ok {
			return invalidArgument("placement constraint values must be unique")
		}
		seen[value] = struct{}{}
	}
	switch constraint.GetOperator() {
	case table_grouppb.PlacementConstraintOperator_PLACEMENT_CONSTRAINT_OPERATOR_IN,
		table_grouppb.PlacementConstraintOperator_PLACEMENT_CONSTRAINT_OPERATOR_NOT_IN:
		if len(constraint.GetValues()) == 0 {
			return invalidArgument("IN and NOT_IN constraints require values")
		}
	case table_grouppb.PlacementConstraintOperator_PLACEMENT_CONSTRAINT_OPERATOR_EXISTS,
		table_grouppb.PlacementConstraintOperator_PLACEMENT_CONSTRAINT_OPERATOR_NOT_EXISTS:
		if len(constraint.GetValues()) != 0 {
			return invalidArgument("EXISTS and NOT_EXISTS constraints forbid values")
		}
	default:
		return invalidArgument("placement constraint operator is unspecified or unknown")
	}
	return nil
}

func validateCapacityStatus(status *table_grouppb.CapacityStatus) error {
	if status == nil {
		return invalidArgument("stored Table Group requires capacity status")
	}
	switch status.GetState() {
	case table_grouppb.CapacityState_CAPACITY_STATE_HEALTHY,
		table_grouppb.CapacityState_CAPACITY_STATE_APPROACHING_LIMIT,
		table_grouppb.CapacityState_CAPACITY_STATE_THROTTLED,
		table_grouppb.CapacityState_CAPACITY_STATE_EXHAUSTED,
		table_grouppb.CapacityState_CAPACITY_STATE_UNKNOWN:
	default:
		return invalidArgument("stored Table Group has unspecified or unknown capacity state")
	}
	dimensions := status.GetLimitingDimensions()
	if len(dimensions) > maxCapacityDimensions {
		return invalidArgument("capacity status exceeds six limiting dimensions")
	}
	var previous table_grouppb.CapacityDimension
	for i, dimension := range dimensions {
		if dimension < table_grouppb.CapacityDimension_CAPACITY_DIMENSION_DATA_BYTES ||
			dimension > table_grouppb.CapacityDimension_CAPACITY_DIMENSION_RECOVERY_SECONDS ||
			(i > 0 && dimension <= previous) {
			return invalidArgument("capacity limiting dimensions must be known, unique, and strictly ordered")
		}
		previous = dimension
	}
	return nil
}

func validateLabel(value string) error {
	if len(value) == 0 || len(value) > maxLabelBytes {
		return invalidArgument("placement label key/value must contain 1 to 128 bytes")
	}
	return nil
}

func validateMembership(membership *table_grouppb.TableGroupMembership) error {
	if membership == nil || membership.GetVersion() == 0 {
		return invalidArgument("membership requires a non-zero version")
	}
	if len(membership.GetMembers()) > maxMembershipMembers {
		return invalidArgument("membership exceeds 1024 members")
	}
	totalIndexes := 0
	var previousTableID, previousPartitionID uint64
	for i, member := range membership.GetMembers() {
		if member == nil || member.GetTableId() == 0 {
			return invalidArgument("membership member requires a non-zero table ID")
		}
		if i > 0 && (member.GetTableId() < previousTableID ||
			(member.GetTableId() == previousTableID && member.GetPartitionId() <= previousPartitionID)) {
			return invalidArgument("membership members must be unique and strictly ordered")
		}
		previousTableID = member.GetTableId()
		previousPartitionID = member.GetPartitionId()
		var previousIndexID uint64
		for j, indexID := range member.GetIndexIds() {
			if indexID == 0 || (j > 0 && indexID <= previousIndexID) {
				return invalidArgument("membership index IDs must be non-zero, unique, and strictly ordered")
			}
			previousIndexID = indexID
			totalIndexes++
			if totalIndexes > maxMembershipIndexes {
				return invalidArgument("membership exceeds 4096 index IDs")
			}
		}
	}
	return nil
}

func validateStoredGroup(group *table_grouppb.TableGroup) error {
	if group == nil {
		return invalidArgument("stored Table Group is nil")
	}
	if err := validateIdentity(group.GetIdentity()); err != nil {
		return err
	}
	if group.GetMetadataVersion() == 0 || group.GetStatusVersion() == 0 {
		return invalidArgument("stored Table Group requires non-zero metadata and status versions")
	}
	binding := group.GetRegionBinding()
	if binding == nil || binding.GetRegionId() == 0 || binding.GetShardId() == 0 || binding.GetRegionEpoch() == nil ||
		binding.GetRegionEpoch().GetVersion() == 0 || binding.GetRegionEpoch().GetConfVer() == 0 {
		return invalidArgument("stored Table Group has an incomplete Region binding")
	}
	if binding.GetAppliedMetadataVersion() > group.GetMetadataVersion() {
		return invalidArgument("stored applied metadata version exceeds authority")
	}
	if group.GetSplitPolicy().GetMode() != table_grouppb.SplitPolicyMode_SPLIT_POLICY_MODE_FORBID {
		return invalidArgument("stored Table Group does not forbid split")
	}
	if err := validateCapacityBudget(group.GetCapacityBudget()); err != nil {
		return err
	}
	if err := validateCapacityStatus(group.GetCapacityStatus()); err != nil {
		return err
	}
	if err := validatePlacementIntent(group.GetPlacementIntent()); err != nil {
		return err
	}
	if err := validateMembership(group.GetActiveMembership()); err != nil {
		return err
	}
	switch group.GetState() {
	case table_grouppb.TableGroupState_TABLE_GROUP_STATE_CREATING:
		if group.GetPendingOperation().GetKind() != table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_CREATE || group.GetPreparedMembership() != nil {
			return invalidArgument("creating Table Group has inconsistent pending state")
		}
	case table_grouppb.TableGroupState_TABLE_GROUP_STATE_ACTIVE:
		if group.GetPendingOperation() != nil || group.GetPreparedMembership() != nil {
			return invalidArgument("active Table Group cannot contain pending state")
		}
	case table_grouppb.TableGroupState_TABLE_GROUP_STATE_UPDATING_MEMBERSHIP:
		if group.GetPendingOperation().GetKind() != table_grouppb.TableGroupOperationKind_TABLE_GROUP_OPERATION_KIND_UPDATE_MEMBERSHIP {
			return invalidArgument("updating Table Group requires a membership operation")
		}
		if err := validateMembership(group.GetPreparedMembership()); err != nil {
			return err
		}
		if group.GetPreparedMembership().GetVersion() != group.GetActiveMembership().GetVersion()+1 {
			return membershipConflict(group.GetIdentity(), "prepared membership version is not active version plus one")
		}
	default:
		return invalidArgument(fmt.Sprintf("unsupported stored Table Group state %s", group.GetState()))
	}
	return validatePendingOperation(group)
}

func validatePendingOperation(group *table_grouppb.TableGroup) error {
	operation := group.GetPendingOperation()
	if operation == nil {
		return nil
	}
	if err := validateOperationToken(operation.GetToken()); err != nil {
		return err
	}
	if operation.GetTargetMetadataVersion() != group.GetMetadataVersion() ||
		operation.GetBaseMetadataVersion()+1 != operation.GetTargetMetadataVersion() {
		return invalidArgument("pending operation metadata versions are inconsistent")
	}
	return nil
}
