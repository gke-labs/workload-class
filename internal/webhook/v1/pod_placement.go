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
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	// SpotLabelKey is the GKE node label indicating a Spot VM node.
	SpotLabelKey = "cloud.google.com/gke-spot"
	// SpotLabelValue is the value for SpotLabelKey on Spot VM nodes.
	SpotLabelValue = "true"
	// ProvisioningLabelKey is the GKE node label indicating provisioning model ("spot" or "standard").
	ProvisioningLabelKey = "cloud.google.com/gke-provisioning"
	// MachineFamilyLabelKey is the GKE node label indicating the GCE machine family (e.g., "n2d", "t2d").
	MachineFamilyLabelKey = "cloud.google.com/machine-family"
	// ComputeClassLabelKey is the GKE node label / nodeSelector key used to target a Custom Compute Class.
	ComputeClassLabelKey = "cloud.google.com/compute-class"
)

// shouldUseSpot determines if a Pod should be placed on Spot based on the existing Spot count, total existing pods, and the target spot ratio
func shouldUseSpot(spotExisting, totalExisting, spotRatio int) bool {
	return spotExisting*100 < (totalExisting+1)*spotRatio
}

// isWorkloadInFallback reports whether the WorkloadClass is currently running Pods on On-Demand fallback.
// It honors the InFallback condition when set and otherwise inspects existing Pods: a Pod that was
// targeted for Spot but is running on a non-Spot node indicates the workload has fallen back.
func (d *PodPlacementDefaulter) isWorkloadInFallback(ctx context.Context, incomingPod *corev1.Pod, wc *workloadsv1.WorkloadClass) bool {
	if meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeInFallback) {
		return true
	}

	pods, err := d.listWorkloadClassPods(ctx, incomingPod, wc)
	if err != nil {
		podlog.Error(err, "Failed to list Pods to determine fallback state", "workloadClass", wc.Name, "namespace", incomingPod.Namespace)
		return false
	}

	for _, existing := range pods {
		if existing.Spec.NodeName == "" || !isPodTargetedForSpot(existing) {
			continue
		}
		if isSpot, known := d.isNodeSpot(ctx, existing.Spec.NodeName); known && !isSpot {
			return true
		}
	}
	return false
}

// countExistingWorkloadClassPods returns how many active Pods selected by the WorkloadClass are
// (or are targeted to be) on Spot, and the total number of active selected Pods.
func (d *PodPlacementDefaulter) countExistingWorkloadClassPods(ctx context.Context, incomingPod *corev1.Pod, wc *workloadsv1.WorkloadClass) (spotCount, totalCount int) {
	pods, err := d.listWorkloadClassPods(ctx, incomingPod, wc)
	if err != nil {
		podlog.Error(err, "Failed to list Pods to compute Spot ratio", "workloadClass", wc.Name, "namespace", incomingPod.Namespace)
		return 0, 0
	}

	for _, existing := range pods {
		totalCount++
		if d.isPodEffectiveSpot(ctx, existing) {
			spotCount++
		}
	}
	return spotCount, totalCount
}

// listWorkloadClassPods lists active (non-terminating, non-terminal) Pods in the incoming Pod's namespace
// that are selected by the WorkloadClass, excluding the incoming Pod itself. A nil PodSelector selects all
// Pods in the namespace (e.g., a namespace-default WorkloadClass).
func (d *PodPlacementDefaulter) listWorkloadClassPods(ctx context.Context, incomingPod *corev1.Pod, wc *workloadsv1.WorkloadClass) ([]*corev1.Pod, error) {
	selector := labels.Everything()
	if wc.Spec.PodSelector != nil {
		var err error
		selector, err = metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid podSelector: %w", err)
		}
	}

	podList := &corev1.PodList{}
	if err := d.Client.List(ctx, podList, client.InNamespace(incomingPod.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, err
	}

	var pods []*corev1.Pod
	for i := range podList.Items {
		existing := &podList.Items[i]
		if existing.DeletionTimestamp != nil ||
			existing.Status.Phase == corev1.PodSucceeded ||
			existing.Status.Phase == corev1.PodFailed {
			continue
		}
		if incomingPod.Name != "" && existing.Name == incomingPod.Name {
			continue
		}
		pods = append(pods, existing)
	}
	return pods, nil
}

func (d *PodPlacementDefaulter) isPodEffectiveSpot(ctx context.Context, pod *corev1.Pod) bool {
	if pod.Spec.NodeName != "" {
		if isSpot, known := d.isNodeSpot(ctx, pod.Spec.NodeName); known {
			return isSpot
		}
	}
	return isPodTargetedForSpot(pod)
}

func (d *PodPlacementDefaulter) isNodeSpot(ctx context.Context, nodeName string) (isSpot, known bool) {
	if nodeName == "" {
		return false, false
	}
	node := &corev1.Node{}
	if err := d.Client.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return false, false
	}
	return node.Labels[SpotLabelKey] == SpotLabelValue, true
}

func isPodTargetedForSpot(pod *corev1.Pod) bool {
	return pod.Spec.NodeSelector[SpotLabelKey] == SpotLabelValue ||
		slices.ContainsFunc(pod.Spec.Tolerations, isSpotToleration)
}

func isSpotToleration(t corev1.Toleration) bool {
	return t.Key == SpotLabelKey &&
		(t.Operator == corev1.TolerationOpEqual || t.Operator == "") &&
		t.Value == SpotLabelValue &&
		t.Effect == corev1.TaintEffectNoSchedule
}

// mutateForOnDemand pins the Pod to On-Demand (non-Spot) nodes.
func mutateForOnDemand(pod *corev1.Pod) {
	delete(pod.Spec.NodeSelector, SpotLabelKey)
	removeSpotToleration(pod)

	requireNodeSelectorTerms(pod, []corev1.NodeSelectorTerm{
		{MatchExpressions: []corev1.NodeSelectorRequirement{spotDoesNotExist()}},
	})
}

// mutateForSpot targets the Pod at Spot nodes.
//   - Fail: the Pod may only schedule onto Spot nodes and stays Pending during a stockout.
//   - FallbackToOnDemand: the Pod prefers Spot nodes but may fall back to On-Demand nodes. If the
//     GKESpotPlacementPolicy restricts fallback machine families, On-Demand fallback is limited to them.
//
// If a GKESpotPlacementPolicy is provided and specifies a CustomComputeClass, the Pod is also targeted at
// that class unless it already selects a compute class. spotPolicy may be nil, in which case no
// GKE-specific refinements are applied.
func mutateForSpot(pod *corev1.Pod, fallbackAction workloadsv1.FallbackAction, spotPolicy *workloadsv1.GKESpotPlacementPolicy) {
	var ccc string
	var families []string
	if spotPolicy != nil {
		ccc = spotPolicy.Spec.CustomComputeClass
		families = spotPolicy.Spec.FallbackAllowedMachineFamilies
	}

	ensureSpotToleration(pod)

	if ccc != "" {
		if _, ok := pod.Spec.NodeSelector[ComputeClassLabelKey]; !ok {
			setNodeSelector(pod, ComputeClassLabelKey, ccc)
		}
	}

	if fallbackAction == workloadsv1.FallbackActionFail {
		setNodeSelector(pod, SpotLabelKey, SpotLabelValue)
		return
	}

	// FallbackToOnDemand: prefer Spot capacity, allow On-Demand fallback.
	addPreferredSchedulingTerm(pod, corev1.PreferredSchedulingTerm{
		Weight:     100,
		Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{spotIn()}},
	})

	if len(families) > 0 {
		// Spot nodes of any family, OR On-Demand nodes of an allowed family.
		requireNodeSelectorTerms(pod, []corev1.NodeSelectorTerm{
			{MatchExpressions: []corev1.NodeSelectorRequirement{spotIn()}},
			{MatchExpressions: []corev1.NodeSelectorRequirement{
				spotDoesNotExist(),
				{Key: MachineFamilyLabelKey, Operator: corev1.NodeSelectorOpIn, Values: families},
			}},
		})
	}
}

func spotIn() corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: SpotLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{SpotLabelValue}}
}

func spotDoesNotExist() corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: SpotLabelKey, Operator: corev1.NodeSelectorOpDoesNotExist}
}

func setNodeSelector(pod *corev1.Pod, key, value string) {
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}
	pod.Spec.NodeSelector[key] = value
}

func ensureSpotToleration(pod *corev1.Pod) {
	if slices.ContainsFunc(pod.Spec.Tolerations, isSpotToleration) {
		return
	}
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
		Key:      SpotLabelKey,
		Operator: corev1.TolerationOpEqual,
		Value:    SpotLabelValue,
		Effect:   corev1.TaintEffectNoSchedule,
	})
}

func removeSpotToleration(pod *corev1.Pod) {
	if len(pod.Spec.Tolerations) == 0 {
		return
	}
	filtered := pod.Spec.Tolerations[:0]
	for _, t := range pod.Spec.Tolerations {
		if isSpotToleration(t) {
			continue
		}
		filtered = append(filtered, t)
	}
	pod.Spec.Tolerations = filtered
}

func ensureNodeAffinity(pod *corev1.Pod) *corev1.NodeAffinity {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	return pod.Spec.Affinity.NodeAffinity
}

// requireNodeSelectorTerms ANDs the given terms (which are ORed among themselves) with any
// existing required node affinity on the Pod.
//
// NodeSelectorTerms are ORed by Kubernetes, so simply appending terms would loosen the Pod's
// existing constraints. Instead, each existing term is combined with each new term (cross product),
// which preserves the Pod's existing requirements while adding the placement requirements.
func requireNodeSelectorTerms(pod *corev1.Pod, terms []corev1.NodeSelectorTerm) {
	na := ensureNodeAffinity(pod)
	if na.RequiredDuringSchedulingIgnoredDuringExecution == nil || len(na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
		na.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{NodeSelectorTerms: terms}
		return
	}

	existing := na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	combined := make([]corev1.NodeSelectorTerm, 0, len(existing)*len(terms))
	for _, e := range existing {
		for _, t := range terms {
			merged := corev1.NodeSelectorTerm{
				MatchExpressions: concatRequirements(e.MatchExpressions, t.MatchExpressions),
				MatchFields:      concatRequirements(e.MatchFields, t.MatchFields),
			}
			combined = append(combined, merged)
		}
	}
	na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = combined
}

// concatRequirements returns a new slice containing a followed by b, or nil if both are empty.
func concatRequirements(a, b []corev1.NodeSelectorRequirement) []corev1.NodeSelectorRequirement {
	if len(a)+len(b) == 0 {
		return nil
	}
	out := make([]corev1.NodeSelectorRequirement, 0, len(a)+len(b))
	out = append(out, a...)
	return append(out, b...)
}

func addPreferredSchedulingTerm(pod *corev1.Pod, term corev1.PreferredSchedulingTerm) {
	na := ensureNodeAffinity(pod)
	na.PreferredDuringSchedulingIgnoredDuringExecution = append(
		na.PreferredDuringSchedulingIgnoredDuringExecution,
		term,
	)
}

// parsePercentage parses an integer percentage string between "0%" and "100%".
func parsePercentage(s string) (int, error) {
	if !strings.HasSuffix(s, "%") {
		return 0, fmt.Errorf("must end with %%")
	}
	v, err := strconv.Atoi(strings.TrimSuffix(s, "%"))
	if err != nil {
		return 0, err
	}
	if v < 0 || v > 100 {
		return 0, fmt.Errorf("must be between 0%% and 100%%")
	}
	return v, nil
}
