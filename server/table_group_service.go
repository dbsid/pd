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

package server

import (
	"context"

	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/tablegroup"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// TableGroupServer serves the leader-only Table Group lifecycle API.
type TableGroupServer struct {
	table_grouppb.UnimplementedTableGroupServiceServer
	*GrpcServer
}

func (s *TableGroupServer) validateRequest(header *table_grouppb.RequestHeader) error {
	if s.IsClosed() {
		return errs.ErrNotStarted
	}
	if !s.member.IsServing() {
		return errs.ErrNotLeader
	}
	clusterID := keypath.ClusterID()
	if header.GetClusterId() != clusterID {
		return errs.ErrMismatchClusterID(clusterID, header.GetClusterId())
	}
	return nil
}

func (s *TableGroupServer) manager() (*tablegroup.Manager, error) {
	cluster := s.GetRaftCluster()
	if cluster == nil || cluster.GetTableGroupManager() == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return cluster.GetTableGroupManager(), nil
}

func tableGroupHeader(err error) *table_grouppb.ResponseHeader {
	header := &table_grouppb.ResponseHeader{ClusterId: keypath.ClusterID()}
	if err == nil {
		return header
	}
	if detail := tablegroup.ErrorDetail(err); detail != nil {
		header.Error = detail
		return header
	}
	header.Error = &table_grouppb.TableGroupError{
		Code:    table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INTERNAL,
		Message: err.Error(),
	}
	return header
}

// CreateTableGroup implements table_grouppb.TableGroupServiceServer.
func (s *TableGroupServer) CreateTableGroup(
	ctx context.Context,
	request *table_grouppb.CreateTableGroupRequest,
) (*table_grouppb.CreateTableGroupResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	manager, err := s.manager()
	if err != nil {
		return &table_grouppb.CreateTableGroupResponse{Header: tableGroupHeader(err)}, nil
	}
	group, err := manager.Create(ctx, request)
	return &table_grouppb.CreateTableGroupResponse{Header: tableGroupHeader(err), TableGroup: group}, nil
}

// GetTableGroup implements table_grouppb.TableGroupServiceServer.
func (s *TableGroupServer) GetTableGroup(
	_ context.Context,
	request *table_grouppb.GetTableGroupRequest,
) (*table_grouppb.GetTableGroupResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	manager, err := s.manager()
	if err != nil {
		return &table_grouppb.GetTableGroupResponse{Header: tableGroupHeader(err)}, nil
	}
	group, err := manager.Get(request.GetIdentity())
	return &table_grouppb.GetTableGroupResponse{Header: tableGroupHeader(err), TableGroup: group}, nil
}

// GetTableGroupByRegion implements table_grouppb.TableGroupServiceServer.
func (s *TableGroupServer) GetTableGroupByRegion(
	_ context.Context,
	request *table_grouppb.GetTableGroupByRegionRequest,
) (*table_grouppb.GetTableGroupByRegionResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	manager, err := s.manager()
	if err != nil {
		return &table_grouppb.GetTableGroupByRegionResponse{Header: tableGroupHeader(err)}, nil
	}
	group, err := manager.GetByRegion(request.GetKeyspaceId(), request.GetRegionId())
	return &table_grouppb.GetTableGroupByRegionResponse{Header: tableGroupHeader(err), TableGroup: group}, nil
}

// PrepareMembership implements table_grouppb.TableGroupServiceServer.
func (s *TableGroupServer) PrepareMembership(
	ctx context.Context,
	request *table_grouppb.PrepareMembershipRequest,
) (*table_grouppb.PrepareMembershipResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	manager, err := s.manager()
	if err != nil {
		return &table_grouppb.PrepareMembershipResponse{Header: tableGroupHeader(err)}, nil
	}
	group, err := manager.PrepareMembership(ctx, request)
	return &table_grouppb.PrepareMembershipResponse{Header: tableGroupHeader(err), TableGroup: group}, nil
}

// CommitMembership implements table_grouppb.TableGroupServiceServer.
func (s *TableGroupServer) CommitMembership(
	ctx context.Context,
	request *table_grouppb.CommitMembershipRequest,
) (*table_grouppb.CommitMembershipResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	manager, err := s.manager()
	if err != nil {
		return &table_grouppb.CommitMembershipResponse{Header: tableGroupHeader(err)}, nil
	}
	group, err := manager.CommitMembership(ctx, request)
	return &table_grouppb.CommitMembershipResponse{Header: tableGroupHeader(err), TableGroup: group}, nil
}

// AbortMembership implements table_grouppb.TableGroupServiceServer.
func (s *TableGroupServer) AbortMembership(
	ctx context.Context,
	request *table_grouppb.AbortMembershipRequest,
) (*table_grouppb.AbortMembershipResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	manager, err := s.manager()
	if err != nil {
		return &table_grouppb.AbortMembershipResponse{Header: tableGroupHeader(err)}, nil
	}
	group, err := manager.AbortMembership(ctx, request)
	return &table_grouppb.AbortMembershipResponse{Header: tableGroupHeader(err), TableGroup: group}, nil
}
