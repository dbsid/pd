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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/tablegroup"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestTableGroupServiceRejectsRequestsWhileServerIsClosed(t *testing.T) {
	service := &TableGroupServer{GrpcServer: &GrpcServer{Server: &Server{}}}
	response, err := service.GetTableGroup(context.Background(), &table_grouppb.GetTableGroupRequest{
		Header: &table_grouppb.RequestHeader{ClusterId: 1},
	})
	require.Error(t, err)
	require.Nil(t, response)

	byRegionResponse, err := service.GetTableGroupByRegion(context.Background(), &table_grouppb.GetTableGroupByRegionRequest{
		Header:   &table_grouppb.RequestHeader{ClusterId: 1},
		RegionId: 1,
	})
	require.Error(t, err)
	require.Nil(t, byRegionResponse)

	routeResponse, err := service.GetTableGroupRoute(context.Background(), &table_grouppb.GetTableGroupRouteRequest{
		Header: &table_grouppb.RequestHeader{ClusterId: 1},
	})
	require.Error(t, err)
	require.Nil(t, routeResponse)
}

func TestTableGroupResponseHeaderPreservesStructuredPolicyError(t *testing.T) {
	oldClusterID := keypath.ClusterID()
	keypath.SetClusterID(42)
	defer keypath.SetClusterID(oldClusterID)
	region := &metapb.Region{TableGroup: &metapb.TableGroupRegionMeta{
		KeyspaceId: 1, TableGroupId: 101, AppliedMetadataVersion: 1,
	}}
	err := tablegroup.EnsureSplitAllowed(nil, region,
		table_grouppb.SplitSource_SPLIT_SOURCE_MANUAL_REQUEST)

	header := tableGroupHeader(err)
	require.Equal(t, uint64(42), header.GetClusterId())
	require.Equal(t, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_SPLIT_FORBIDDEN,
		header.GetError().GetCode())
	require.Equal(t, table_grouppb.SplitSource_SPLIT_SOURCE_MANUAL_REQUEST,
		header.GetError().GetSplitSource())

	header = tableGroupHeader(errors.New("internal test error"))
	require.Equal(t, table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INTERNAL,
		header.GetError().GetCode())
}
