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

package utils

import (
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	// InitialResourcesFilledCPURequest records that a cpu request was filled in on at least one container.
	InitialResourcesFilledCPURequest = "cpu-request"
	// InitialResourcesFilledMemoryRequest records that a memory request was filled in on at least one container.
	InitialResourcesFilledMemoryRequest = "memory-request"
	// InitialResourcesFilledCPULimit records that a cpu limit was filled in on at least one container.
	InitialResourcesFilledCPULimit = "cpu-limit"
	// InitialResourcesFilledMemoryLimit records that a memory limit was filled in on at least one container.
	InitialResourcesFilledMemoryLimit = "memory-limit"

	// InitialResourcesSkipPodLevelResources is the reason for skipping a Pod that sets Pod-level resources
	// (spec.resources). Kubernetes validates container requests and limits against them, so filling in
	// container values could make the Pod invalid.
	InitialResourcesSkipPodLevelResources = "pod-level-resources"

	initialResourcesSkippedPrefix = "skipped: "
)

// initialResourcesFillOrder is the order in which filled values are reported.
var initialResourcesFillOrder = []string{
	InitialResourcesFilledCPURequest,
	InitialResourcesFilledMemoryRequest,
	InitialResourcesFilledCPULimit,
	InitialResourcesFilledMemoryLimit,
}

// ApplyInitialResources fills in the cpu and memory requests and limits that the containers of the Pod do not
// set, from the initial resources of the WorkloadClass that governs the Pod. It changes the Pod in memory and
// never changes a value that a container already sets.
//
// For each container in spec.containers that is not listed in excludeContainers:
//   - A missing cpu or memory request is set to the initial request. Each resource is filled in separately.
//   - A resource that already has a limit is left alone. The API server copies a limit into a missing
//     request before admission, so such a container has a request by the time a webhook sees it. Leaving
//     the resource alone also guarantees that a request is never written above an existing limit.
//   - A missing limit is then set to the container's request (after filling), for memory only with the
//     MemoryEqualsRequest limit policy, for cpu and memory with EqualsRequest, and never with None.
//
// Init containers (including native sidecars) and ephemeral containers are not changed.
//
// It returns the kinds of values it filled in on at least one container (for example "cpu-request"), in a
// stable order, or the reason why the whole Pod was skipped.
func ApplyInitialResources(pod *corev1.Pod, ir *workloadsv1.InitialResources) (filled []string, skipReason string) {
	if pod == nil || ir == nil {
		return nil, ""
	}
	if pod.Spec.Resources != nil {
		return nil, InitialResourcesSkipPodLevelResources
	}

	limitResources := initialLimitResources(EffectiveLimitPolicy(ir))
	seen := map[string]bool{}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if slices.Contains(ir.ExcludeContainers, c.Name) {
			continue
		}
		for _, r := range managedResources {
			if fillInitialRequest(c, r, ir.Requests) {
				seen[string(r)+"-request"] = true
			}
		}
		for _, r := range limitResources {
			if fillInitialLimit(c, r) {
				seen[string(r)+"-limit"] = true
			}
		}
	}

	for _, kind := range initialResourcesFillOrder {
		if seen[kind] {
			filled = append(filled, kind)
		}
	}
	return filled, ""
}

// initialLimitResources returns the resources whose missing limits the limit policy sets.
func initialLimitResources(policy workloadsv1.LimitPolicy) []corev1.ResourceName {
	switch policy {
	case workloadsv1.LimitPolicyMemoryEqualsRequest:
		return []corev1.ResourceName{corev1.ResourceMemory}
	case workloadsv1.LimitPolicyEqualsRequest:
		return managedResources
	default:
		return nil
	}
}

// fillInitialRequest sets the container's request for the resource to the initial request if the container
// sets neither a request nor a limit for it. It reports whether it changed the container.
func fillInitialRequest(c *corev1.Container, name corev1.ResourceName, requests corev1.ResourceList) bool {
	if _, ok := c.Resources.Requests[name]; ok {
		return false
	}
	if _, ok := c.Resources.Limits[name]; ok {
		return false
	}
	q, ok := requests[name]
	if !ok || q.Sign() <= 0 {
		// Never write a request that would make the Pod invalid. The WorkloadClass controller reports such
		// values on the ResourceManagementValidated condition.
		return false
	}
	if c.Resources.Requests == nil {
		c.Resources.Requests = corev1.ResourceList{}
	}
	c.Resources.Requests[name] = q.DeepCopy()
	return true
}

// fillInitialLimit sets the container's limit for the resource to its request if the container sets a request
// but no limit for it. It reports whether it changed the container.
func fillInitialLimit(c *corev1.Container, name corev1.ResourceName) bool {
	if _, ok := c.Resources.Limits[name]; ok {
		return false
	}
	q, ok := c.Resources.Requests[name]
	if !ok {
		return false
	}
	if c.Resources.Limits == nil {
		c.Resources.Limits = corev1.ResourceList{}
	}
	c.Resources.Limits[name] = q.DeepCopy()
	return true
}

// InitialResourcesAnnotationValue returns the value of the workloads.gke.io/initial-resources annotation that
// records the result of ApplyInitialResources: a comma-separated list of the kinds of values that were filled
// in, or "skipped: <reason>" if the Pod was skipped. It returns an empty string if there is nothing to record.
func InitialResourcesAnnotationValue(filled []string, skipReason string) string {
	if skipReason != "" {
		return initialResourcesSkippedPrefix + skipReason
	}
	return strings.Join(filled, ",")
}
