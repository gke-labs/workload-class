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
)

// IsSpotNode reports whether the node carries the GKE Spot label.
func IsSpotNode(node *corev1.Node) bool {
	return node != nil && node.Labels[SpotLabelKey] == SpotLabelValue
}

// IsPodTargetedForSpot reports whether the Pod has been directed at Spot capacity (by the Pod placement
// webhook), either via the Spot nodeSelector or the Spot toleration. Pods placed on On-Demand on purpose
// carry neither.
func IsPodTargetedForSpot(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return pod.Spec.NodeSelector[SpotLabelKey] == SpotLabelValue ||
		slices.ContainsFunc(pod.Spec.Tolerations, IsSpotToleration)
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
