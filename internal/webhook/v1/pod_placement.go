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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	// SpotLabelKey is the GKE node label indicating a Spot VM node.
	SpotLabelKey = utils.SpotLabelKey
	// SpotLabelValue is the value for SpotLabelKey on Spot VM nodes.
	SpotLabelValue = utils.SpotLabelValue
	// ProvisioningLabelKey is the GKE node label indicating provisioning model ("spot" or "standard").
	ProvisioningLabelKey = "cloud.google.com/gke-provisioning"
	// MachineFamilyLabelKey is the GKE node label indicating the GCE machine family (e.g., "n2d", "t2d").
	MachineFamilyLabelKey = "cloud.google.com/machine-family"
	// ComputeClassLabelKey is the GKE node label / nodeSelector key used to target a Custom Compute Class.
	ComputeClassLabelKey = "cloud.google.com/compute-class"
)

// existingPodCounts holds placement counts for the active Pods currently selected by a WorkloadClass
// (excluding the incoming Pod being admitted).
type existingPodCounts struct {
	// total is the number of active Pods selected by the WorkloadClass.
	total int
	// effectiveSpot is the number of Pods counted as Spot for SpotRatio purposes (scheduled on a Spot node, or
	// targeted for Spot when unscheduled or the node cannot be fetched).
	effectiveSpot int
	// fallback is the number of Spot-targeted Pods scheduled on a known non-Spot node.
	fallback int
}

// isWorkloadInFallback reports whether the WorkloadClass is currently running Pods on On-Demand fallback.
// It honors the InFallback condition when set and otherwise inspects existing Pods: a Pod that was
// targeted for Spot but is running on a non-Spot node indicates the workload has fallen back.
func (d *PodPlacementDefaulter) isWorkloadInFallback(ctx context.Context, incomingPod *corev1.Pod, wc *workloadsv1.WorkloadClass) bool {
	if meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeInFallback) {
		return true
	}

	counts, err := d.countWorkloadClassPods(ctx, incomingPod, wc)
	if err != nil {
		podlog.Error(err, "Failed to list Pods to determine fallback state", "workloadClass", wc.Name, "namespace", incomingPod.Namespace)
		return false
	}
	return counts.fallback > 0
}

// countExistingWorkloadClassPods returns how many active Pods selected by the WorkloadClass are
// (or are targeted to be) on Spot, and the total number of active selected Pods.
func (d *PodPlacementDefaulter) countExistingWorkloadClassPods(ctx context.Context, incomingPod *corev1.Pod, wc *workloadsv1.WorkloadClass) (spotCount, totalCount int) {
	counts, err := d.countWorkloadClassPods(ctx, incomingPod, wc)
	if err != nil {
		podlog.Error(err, "Failed to list Pods to compute Spot ratio", "workloadClass", wc.Name, "namespace", incomingPod.Namespace)
		return 0, 0
	}
	return counts.effectiveSpot, counts.total
}

// countWorkloadClassPods classifies the active Pods selected by the WorkloadClass (excluding the incoming Pod)
// in a single pass, caching node lookups so each distinct node is fetched at most once.
func (d *PodPlacementDefaulter) countWorkloadClassPods(ctx context.Context, incomingPod *corev1.Pod, wc *workloadsv1.WorkloadClass) (existingPodCounts, error) {
	pods, err := d.listWorkloadClassPods(ctx, incomingPod, wc)
	if err != nil {
		return existingPodCounts{}, err
	}

	type nodeSpot struct {
		isSpot bool
		known  bool
	}
	nodes := make(map[string]nodeSpot)
	lookupNode := func(nodeName string) (bool, bool) {
		if n, ok := nodes[nodeName]; ok {
			return n.isSpot, n.known
		}
		isSpot, known := d.isNodeSpot(ctx, nodeName)
		nodes[nodeName] = nodeSpot{isSpot: isSpot, known: known}
		return isSpot, known
	}

	var counts existingPodCounts
	for _, existing := range pods {
		counts.total++
		targeted := utils.IsPodTargetedForSpot(existing)
		if existing.Spec.NodeName == "" {
			if targeted {
				counts.effectiveSpot++
			}
			continue
		}
		isSpot, known := lookupNode(existing.Spec.NodeName)
		switch {
		case known && isSpot:
			counts.effectiveSpot++
		case known && !isSpot:
			if targeted {
				counts.fallback++
			}
		case targeted:
			counts.effectiveSpot++
		}
	}
	return counts, nil
}

// listWorkloadClassPods lists active (non-terminating, non-terminal) Pods in the incoming Pod's namespace
// that the WorkloadClass applies to, excluding the incoming Pod itself (see utils.WorkloadClassPodSelector: a
// nil PodSelector, or the WorkloadClass being the namespace's enforced WorkloadClass, selects all Pods in the namespace). When
// multiple WorkloadClasses exist in the namespace, each Pod is only counted for its best-matching WorkloadClass.
func (d *PodPlacementDefaulter) listWorkloadClassPods(ctx context.Context, incomingPod *corev1.Pod, wc *workloadsv1.WorkloadClass) ([]*corev1.Pod, error) {
	selector, err := utils.WorkloadClassPodSelector(ctx, d.Client, wc)
	if err != nil {
		return nil, err
	}

	podList := &corev1.PodList{}
	if err := d.Client.List(ctx, podList, client.InNamespace(incomingPod.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, err
	}

	wcs := &workloadsv1.WorkloadClassList{}
	if err := d.Client.List(ctx, wcs, client.InNamespace(incomingPod.Namespace)); err != nil {
		return nil, err
	}
	checkBestMatch := len(wcs.Items) > 1

	var pods []*corev1.Pod
	for i := range podList.Items {
		existing := &podList.Items[i]
		if !utils.IsPodActive(existing) {
			continue
		}
		if incomingPod.Name != "" && existing.Name == incomingPod.Name {
			continue
		}
		if checkBestMatch {
			bestMatch, err := utils.FindBestMatchWorkloadClass(ctx, d.Client, nil, existing)
			if err != nil {
				return nil, fmt.Errorf("failed to find best match WorkloadClass for Pod %s: %w", existing.Name, err)
			}
			if bestMatch == nil || bestMatch.Name != wc.Name {
				continue
			}
		}
		pods = append(pods, existing)
	}
	return pods, nil
}

// isNodeSpot reports whether the named node carries the GKE Spot label. known is false when the
// node name is empty or the node can't be fetched, so callers can fall back to other signals.
func (d *PodPlacementDefaulter) isNodeSpot(ctx context.Context, nodeName string) (isSpot, known bool) {
	if nodeName == "" {
		return false, false
	}
	node := &corev1.Node{}
	if err := d.Client.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return false, false
	}
	return utils.IsSpotNode(node), true
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

// spotIn returns a node selector requirement matching Spot nodes (gke-spot In [true]).
func spotIn() corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: SpotLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{SpotLabelValue}}
}

// spotDoesNotExist returns a node selector requirement matching non-Spot nodes (gke-spot DoesNotExist).
func spotDoesNotExist() corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: SpotLabelKey, Operator: corev1.NodeSelectorOpDoesNotExist}
}

// setNodeSelector sets key=value in the Pod's nodeSelector, initializing the map if needed.
func setNodeSelector(pod *corev1.Pod, key, value string) {
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}
	pod.Spec.NodeSelector[key] = value
}

// ensureSpotToleration adds the GKE Spot toleration to the Pod if it isn't already present.
func ensureSpotToleration(pod *corev1.Pod) {
	if slices.ContainsFunc(pod.Spec.Tolerations, utils.IsSpotToleration) {
		return
	}
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
		Key:      SpotLabelKey,
		Operator: corev1.TolerationOpEqual,
		Value:    SpotLabelValue,
		Effect:   corev1.TaintEffectNoSchedule,
	})
}

// removeSpotToleration removes any GKE Spot tolerations from the Pod, leaving other tolerations intact.
func removeSpotToleration(pod *corev1.Pod) {
	if len(pod.Spec.Tolerations) == 0 {
		return
	}
	filtered := pod.Spec.Tolerations[:0]
	for _, t := range pod.Spec.Tolerations {
		if utils.IsSpotToleration(t) {
			continue
		}
		filtered = append(filtered, t)
	}
	pod.Spec.Tolerations = filtered
}

// ensureNodeAffinity returns the Pod's NodeAffinity, initializing Affinity and NodeAffinity if nil.
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

// addPreferredSchedulingTerm appends a preferred (soft) node affinity term to the Pod.
func addPreferredSchedulingTerm(pod *corev1.Pod, term corev1.PreferredSchedulingTerm) {
	na := ensureNodeAffinity(pod)
	na.PreferredDuringSchedulingIgnoredDuringExecution = append(
		na.PreferredDuringSchedulingIgnoredDuringExecution,
		term,
	)
}
