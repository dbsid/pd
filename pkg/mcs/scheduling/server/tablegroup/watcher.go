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

// Package tablegroup watches the PD-owned Table Group authority for scheduling.
package tablegroup

import (
	"context"
	"sync"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/table_grouppb"
	"github.com/pingcap/log"

	pdtablegroup "github.com/tikv/pd/pkg/tablegroup"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// Watcher maintains the scheduling service's read-only Table Group policy registry.
type Watcher struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	watcher *etcdutil.LoopWatcher
	policy  *pdtablegroup.SplitPolicyRegistry
}

// NewWatcher starts a prefix watcher and waits for the initial authority snapshot.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	policy *pdtablegroup.SplitPolicyRegistry,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := &Watcher{cancel: cancel, policy: policy}
	putFn := func(kv *mvccpb.KeyValue) error {
		group := &table_grouppb.TableGroup{}
		if err := proto.Unmarshal(kv.Value, group); err != nil {
			log.Warn("failed to decode watched Table Group", zap.String("key", string(kv.Key)), zap.Error(err))
			return err
		}
		return w.policy.Sync(group)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		groupID, err := keypath.ExtractTableGroupIDFromRecordPath(string(kv.Key))
		if err != nil {
			return err
		}
		w.policy.Remove(groupID)
		return nil
	}
	w.watcher = etcdutil.NewLoopWatcher(
		ctx,
		&w.wg,
		etcdClient,
		"scheduling-table-group-watcher",
		keypath.TableGroupRecordsPrefix(),
		func([]*clientv3.Event) error { return nil },
		putFn,
		deleteFn,
		func([]*clientv3.Event) error { return nil },
		true,
	)
	w.watcher.StartWatchLoop()
	if err := w.watcher.WaitLoad(); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

// Close stops the watcher and waits for its goroutine.
func (w *Watcher) Close() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}
