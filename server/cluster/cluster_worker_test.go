// Copyright 2016 TiKV Project Authors.
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

package cluster

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/table_grouppb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/tablegroup"
)

type splitCountingAllocator struct {
	calls atomic.Int64
	next  atomic.Uint64
}

func (a *splitCountingAllocator) Alloc(count uint32) (uint64, uint32, error) {
	a.calls.Add(1)
	return a.next.Add(uint64(count)), count, nil
}

func (a *splitCountingAllocator) AllocAt(id uint64) (bool, error) {
	for {
		base := a.next.Load()
		if id <= base {
			return false, nil
		}
		if a.next.CompareAndSwap(base, id) {
			return true, nil
		}
	}
}

func (a *splitCountingAllocator) SetBase(base uint64) error {
	a.next.Store(base)
	return nil
}

func (*splitCountingAllocator) Rebase() error { return nil }

func TestReportSplit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	left := &metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	right := &metapb.Region{Id: 2, StartKey: []byte("b"), EndKey: []byte("c")}
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: left, Right: right})
	re.NoError(err)
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: right, Right: left})
	re.Error(err)
}

func TestReportBatchSplit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	regions := []*metapb.Region{
		{Id: 1, StartKey: []byte(""), EndKey: []byte("a")},
		{Id: 2, StartKey: []byte("a"), EndKey: []byte("b")},
		{Id: 3, StartKey: []byte("b"), EndKey: []byte("c")},
		{Id: 3, StartKey: []byte("c"), EndKey: []byte("")},
	}
	_, err = cluster.HandleBatchReportSplit(&pdpb.ReportBatchSplitRequest{Regions: regions})
	re.NoError(err)
}

func TestTableGroupSplitAllocationAndReportsAreRejectedBeforeSideEffects(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	allocator := &splitCountingAllocator{}
	cluster := newTestRaftCluster(ctx, allocator, opt, storage.NewStorageWithMemoryBackend())
	peer := &metapb.Peer{Id: 11, StoreId: 1}
	protected := &metapb.Region{
		Id:          10,
		StartKey:    []byte("a"),
		EndKey:      []byte("z"),
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		Peers:       []*metapb.Peer{peer},
		TableGroup: &metapb.TableGroupRegionMeta{
			KeyspaceId: 1, TableGroupId: 101, AppliedMetadataVersion: 1,
		},
	}
	re.NoError(cluster.putRegion(core.NewRegionInfo(protected, peer)))

	_, err = cluster.HandleAskSplit(&pdpb.AskSplitRequest{Region: protected})
	assertTableGroupSplitError(re, err, table_grouppb.SplitSource_SPLIT_SOURCE_AUTOMATIC_SIZE)
	_, err = cluster.HandleAskBatchSplit(&pdpb.AskBatchSplitRequest{
		Region: protected, SplitCount: 2, Reason: pdpb.SplitReason_LOAD,
	})
	assertTableGroupSplitError(re, err, table_grouppb.SplitSource_SPLIT_SOURCE_AUTOMATIC_LOAD)
	re.Zero(allocator.calls.Load(), "protected split requests must not allocate IDs")

	left := &metapb.Region{
		Id: 11, StartKey: []byte("a"), EndKey: []byte("m"), TableGroup: protected.GetTableGroup(),
	}
	right := &metapb.Region{Id: 12, StartKey: []byte("m"), EndKey: []byte("z")}
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: left, Right: right})
	assertTableGroupSplitError(re, err, table_grouppb.SplitSource_SPLIT_SOURCE_RECOVERY_REPLAY)
	_, err = cluster.HandleBatchReportSplit(&pdpb.ReportBatchSplitRequest{Regions: []*metapb.Region{left, right}})
	assertTableGroupSplitError(re, err, table_grouppb.SplitSource_SPLIT_SOURCE_RECOVERY_REPLAY)
}

func assertTableGroupSplitError(re *require.Assertions, err error, source table_grouppb.SplitSource) {
	re.Error(err)
	detail := tablegroup.ErrorDetail(err)
	re.NotNil(detail)
	re.Equal(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_SPLIT_FORBIDDEN, detail.GetCode())
	re.Equal(source, detail.GetSplitSource())
}
