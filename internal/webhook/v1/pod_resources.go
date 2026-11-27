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

package v1

import (
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/metrics"
	"github.com/gke-labs/workload-class/internal/utils"
)

// defaultInitialResources fills in the requests and limits that the containers of the Pod do not set, from
// the initial resources of the WorkloadClass (see utils.ApplyInitialResources for the fill rules).
//
// Initial resources are only applied once the controller has confirmed that the resource management
// configuration is valid against guardrails for the current generation of the WorkloadClass
// (ResourceManagementValidated). A WorkloadClass that is only flagged for an overlapping PodSelector
// (SelectorOverlap) is still valid, because each Pod resolves to a single best match.
//
// The result is recorded on the Pod in the workloads.gke.io/initial-resources annotation and in the
// workloadclass_initial_resources_* metrics. Like placement, initial resources fail open: a Pod is never
// rejected because they could not be applied.
func (d *PodDefaulter) defaultInitialResources(log logr.Logger, pod *corev1.Pod, wlc *workloadsv1.WorkloadClass) {
	if !d.EnableInitialResources {
		return
	}
	rm := wlc.Spec.ResourceManagement
	if rm == nil || rm.InitialResources == nil {
		return
	}

	if !utils.IsResourceManagementReady(wlc) {
		cond := utils.ResourceManagementValidatedCondition(wlc)
		if cond == nil {
			log.Info("WorkloadClass condition is not set; initial resources will not be applied",
				"condition", workloadsv1.ConditionTypeResourceManagementValidated)
		} else {
			log.Info("WorkloadClass condition is not True for the current generation; initial resources will not be applied",
				"condition", cond.Type, "status", cond.Status, "reason", cond.Reason, "message", cond.Message,
				"observedGeneration", cond.ObservedGeneration, "generation", wlc.Generation)
		}
		metrics.InitialResourcesSkipped.WithLabelValues(metrics.InitialResourcesSkipClassNotValidated).Inc()
		return
	}

	filled, skipReason := utils.ApplyInitialResources(pod, rm.InitialResources)
	if value := utils.InitialResourcesAnnotationValue(filled, skipReason); value != "" {
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[workloadsv1.InitialResourcesAnnotation] = value
	}

	if skipReason != "" {
		log.Info("Skipped initial resources for Pod", "reason", skipReason)
		metrics.InitialResourcesSkipped.WithLabelValues(skipReason).Inc()
		return
	}
	for _, kind := range filled {
		metrics.InitialResourcesApplied.WithLabelValues(kind).Inc()
	}
	if len(filled) > 0 {
		log.Info("Applied initial resources to Pod", "filled", filled)
	}
}
