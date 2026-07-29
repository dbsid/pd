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

import "github.com/prometheus/client_golang/prometheus"

var (
	tableGroupLifecycleCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "table_group",
			Name:      "lifecycle_total",
			Help:      "Number of Table Group lifecycle operations by bounded operation and result.",
		},
		[]string{"operation", "result"},
	)
	tableGroupSplitRejectedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "table_group",
			Name:      "split_rejected_total",
			Help:      "Number of protected Region split attempts rejected by source and reason.",
		},
		[]string{"source", "reason"},
	)
)

func init() {
	prometheus.MustRegister(tableGroupLifecycleCounter, tableGroupSplitRejectedCounter)
}
