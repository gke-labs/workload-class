/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package metrics defines the custom Prometheus metrics of the workload-class manager. They are registered
// with the controller-runtime metrics registry and served on the manager's metrics endpoint.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// InitialResourcesSkipClassNotValidated is the skip reason recorded when the WorkloadClass that governs a
	// Pod sets initial resources, but its ResourceManagementValidated condition is missing, failing, or was
	// observed for an older generation.
	InitialResourcesSkipClassNotValidated = "class-not-validated"
)

var (
	// InitialResourcesApplied counts the Pods on which the Pod webhook filled in a kind of value from the
	// initial resources of a WorkloadClass. A Pod is counted once for each kind of value that was filled in
	// on at least one of its containers.
	InitialResourcesApplied = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "workloadclass_initial_resources_applied_total",
			Help: "Number of Pods on which initial resources filled in a kind of value " +
				"(cpu-request, memory-request, cpu-limit or memory-limit) at admission.",
		},
		[]string{"resource"},
	)

	// InitialResourcesSkipped counts the Pods governed by a WorkloadClass with initial resources that the Pod
	// webhook admitted without applying them.
	InitialResourcesSkipped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "workloadclass_initial_resources_skipped_total",
			Help: "Number of Pods governed by a WorkloadClass with initial resources that were admitted " +
				"without applying them, by reason.",
		},
		[]string{"reason"},
	)
)

func init() {
	ctrlmetrics.Registry.MustRegister(InitialResourcesApplied, InitialResourcesSkipped)
}
