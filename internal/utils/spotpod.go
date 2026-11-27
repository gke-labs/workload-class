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

	corev1 "k8s.io/api/core/v1"
)

const (
	// SpotLabelKey is the GKE node label indicating a Spot VM node.
	SpotLabelKey = "cloud.google.com/gke-spot"
	// SpotLabelValue is the value for SpotLabelKey on Spot VM nodes.
	SpotLabelValue = "true"
	// ProvisioningLabelKey is the GKE node label indicating provisioning model ("spot" or "standard").
	ProvisioningLabelKey = "cloud.google.com/gke-provisioning"
	// ProvisioningSpotValue is the value for ProvisioningLabelKey on Spot VM nodes.
	ProvisioningSpotValue = "spot"
)

// IsSpotNode reports whether the node carries the GKE Spot label.
func IsSpotNode(node *corev1.Node) bool {
	return node != nil && node.Labels[SpotLabelKey] == SpotLabelValue
}

const (
	// CountComputeClassAsTargetAnnotation, set to "true" on a GKESpotPlacementPolicy with a non-Spot
	// customComputeClass, makes nodes of that compute class count as "on target" (like Spot nodes) for
	// spotRatio accounting and fallback detection. This lets the Spot plugin split a workload between a
	// preferred compute class and default capacity. Experimental (workload-class demo).
	CountComputeClassAsTargetAnnotation = "workloads.gke.io/count-compute-class-as-target"
	// ComputeClassNodeLabelKey is the GKE node label carrying the node's compute class.
	ComputeClassNodeLabelKey = "cloud.google.com/compute-class"
)

// IsOnTargetNode reports whether a Spot-targeted Pod on node is on target: the node is a Spot node, or
// targetCCC is set and the node belongs to that compute class.
func IsOnTargetNode(node *corev1.Node, targetCCC string) bool {
	if IsSpotNode(node) {
		return true
	}
	return node != nil && targetCCC != "" && node.Labels[ComputeClassNodeLabelKey] == targetCCC
}

// IsPodTargetedForSpot reports whether the Pod has been directed at Spot capacity (by the Pod placement
// webhook), via the Spot nodeSelector, the Spot toleration, or a required nodeAffinity on
// cloud.google.com/gke-provisioning In [spot]. Pods placed on On-Demand on purpose carry none of these.
func IsPodTargetedForSpot(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.Spec.NodeSelector[SpotLabelKey] == SpotLabelValue ||
		slices.ContainsFunc(pod.Spec.Tolerations, IsSpotToleration) {
		return true
	}
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Key == ProvisioningLabelKey && expr.Operator == corev1.NodeSelectorOpIn && slices.Contains(expr.Values, ProvisioningSpotValue) {
				return true
			}
		}
	}
	return false
}

// IsSpotToleration reports whether t tolerates the GKE Spot NoSchedule taint (gke-spot=true).
func IsSpotToleration(t corev1.Toleration) bool {
	return t.Key == SpotLabelKey &&
		(t.Operator == corev1.TolerationOpEqual || t.Operator == "") &&
		t.Value == SpotLabelValue &&
		t.Effect == corev1.TaintEffectNoSchedule
}

// IsPodActive reports whether the Pod is neither terminating nor in a terminal phase.
func IsPodActive(pod *corev1.Pod) bool {
	return pod != nil &&
		pod.DeletionTimestamp == nil &&
		pod.Status.Phase != corev1.PodSucceeded &&
		pod.Status.Phase != corev1.PodFailed
}
