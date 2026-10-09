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

package controller

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	fallbackActiveMsg = "One or more Spot-targeted Pods are running on On-Demand fallback nodes"
	spotTargetMetMsg  = "No Spot-targeted Pods are running on On-Demand fallback nodes"
	scaledToZeroMsg   = "The workload has no active Pods"
)

// tracksFallback reports whether the InFallback condition applies to the WorkloadClass: it targets Spot and
// allows falling back to On-Demand capacity.
func tracksFallback(wc *workloadsv1.WorkloadClass) bool {
	return targetsSpot(wc) && wc.Spec.CapacityStrategy.FallbackAction == workloadsv1.FallbackActionFallbackToOnDemand
}

// reconcileFallbackState updates the InFallback condition on the WorkloadClass status in memory and reports
// whether the status changed. The caller is responsible for persisting the status.
//
// The workload is in fallback when any Spot-targeted Pod (one carrying the Spot nodeSelector or toleration) is
// running on a non-Spot node. Pods placed on On-Demand on purpose (Type OnDemand, or to meet a SpotRatio below
// 100%) carry no Spot targeting, and unscheduled Pods are ignored, so scale-ups and rollouts don't register as
// fallback.
//
//   - Not applicable (no Spot targeting or FallbackAction is not FallbackToOnDemand): the condition is removed.
//   - Not validated: the condition is left untouched, so a transient validation failure doesn't release a
//     latched None fallback.
//   - In fallback: InFallback=True.
//   - Not in fallback: InFallback=False, except with reversion None, where InFallback=True is latched (so the
//     Pod placement webhook keeps new Pods on On-Demand) until the workload scales down to zero Pods.
func (r *WorkloadClassReconciler) reconcileFallbackState(ctx context.Context, wc *workloadsv1.WorkloadClass, validated bool) (bool, error) {
	if !tracksFallback(wc) {
		return meta.RemoveStatusCondition(&wc.Status.Conditions, workloadsv1.ConditionTypeInFallback), nil
	}
	if !validated {
		return false, nil
	}

	reversionAction, err := r.effectiveReversionAction(ctx, wc)
	if err != nil {
		return false, err
	}

	spotNodes, err := r.listSpotNodeNames(ctx)
	if err != nil {
		return false, err
	}
	fallbackPods, activePods, err := r.countFallbackPods(ctx, wc, spotNodes)
	if err != nil {
		return false, err
	}

	cond := metav1.Condition{
		Type:               workloadsv1.ConditionTypeInFallback,
		ObservedGeneration: wc.Generation,
	}
	inFallback := meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeInFallback)

	switch {
	case fallbackPods > 0:
		cond.Status, cond.Reason, cond.Message = metav1.ConditionTrue, workloadsv1.ReasonFallbackActive, fallbackActiveMsg
	case inFallback && reversionAction == workloadsv1.NoneReversionAction && activePods > 0:
		// Latched: keep new Pods on On-Demand after fallback.
		return false, nil
	case activePods == 0:
		cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, workloadsv1.ReasonSpotTargetMet, scaledToZeroMsg
	default:
		cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, workloadsv1.ReasonSpotTargetMet, spotTargetMetMsg
	}

	changed := meta.SetStatusCondition(&wc.Status.Conditions, cond)
	if changed {
		logf.FromContext(ctx).Info("Updated Spot fallback state", "inFallback", cond.Status, "fallbackPods", fallbackPods,
			"activePods", activePods, "reversionAction", reversionAction)
	}
	return changed, nil
}

// effectiveReversionAction returns the reversion action from the WorkloadClass's attached GKESpotPlacementPolicy,
// defaulting to None when the policy leaves it unset. Without an attached GKESpotPlacementPolicy there is no
// reversion configuration, and reversion is treated as Lazy (matching the Pod placement webhook).
func (r *WorkloadClassReconciler) effectiveReversionAction(ctx context.Context, wc *workloadsv1.WorkloadClass) (workloadsv1.ReversionAction, error) {
	ref := wc.Spec.InfrastructureProfileRef
	if !referencesSpotPolicy(ref) ||
		!meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached) {
		return workloadsv1.LazyReversionAction, nil
	}

	policy := &workloadsv1.GKESpotPlacementPolicy{}
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name}, policy); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return workloadsv1.LazyReversionAction, nil
		}
		return "", fmt.Errorf("failed to get GKESpotPlacementPolicy %s: %w", ref.Name, err)
	}
	if policy.Spec.Reversion.Action == "" {
		return workloadsv1.NoneReversionAction, nil
	}
	return policy.Spec.Reversion.Action, nil
}

// countFallbackPods counts the active (non-terminating, non-terminal) Pods selected by the WorkloadClass, and how
// many of them are Spot-targeted Pods scheduled on non-Spot nodes. A nil PodSelector selects all Pods in the
// namespace.
func (r *WorkloadClassReconciler) countFallbackPods(ctx context.Context, wc *workloadsv1.WorkloadClass, spotNodes map[string]bool) (fallbackPods, activePods int, err error) {
	selector := labels.Everything()
	if wc.Spec.PodSelector != nil {
		selector, err = metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid podSelector: %w", err)
		}
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return 0, 0, fmt.Errorf("failed to list Pods: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !isPodActive(pod) {
			continue
		}
		activePods++
		if pod.Spec.NodeName != "" && !spotNodes[pod.Spec.NodeName] && isPodTargetedForSpot(pod) {
			fallbackPods++
		}
	}
	return fallbackPods, activePods, nil
}

// isPodActive reports whether the Pod is neither terminating nor in a terminal phase.
func isPodActive(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
}

// isPodTargetedForSpot reports whether the Pod was directed at Spot capacity (by the Pod placement webhook),
// either via the Spot nodeSelector or the Spot toleration.
func isPodTargetedForSpot(pod *corev1.Pod) bool {
	return pod.Spec.NodeSelector[spotNodeLabelKey] == spotNodeLabelValue ||
		slices.ContainsFunc(pod.Spec.Tolerations, func(t corev1.Toleration) bool {
			return t.Key == spotNodeLabelKey &&
				(t.Operator == corev1.TolerationOpEqual || t.Operator == "") &&
				t.Value == spotNodeLabelValue &&
				t.Effect == corev1.TaintEffectNoSchedule
		})
}

// podPlacementChangePredicate passes Pod events that can change a workload's fallback state: Pods being bound
// to a node, starting to terminate, reaching a terminal phase, or being deleted.
func podPlacementChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			pod, ok := e.Object.(*corev1.Pod)
			return ok && pod.Spec.NodeName != ""
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, okOld := e.ObjectOld.(*corev1.Pod)
			newPod, okNew := e.ObjectNew.(*corev1.Pod)
			if !okOld || !okNew {
				return false
			}
			return oldPod.Spec.NodeName != newPod.Spec.NodeName ||
				isPodActive(oldPod) != isPodActive(newPod)
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// findWorkloadClassForPod enqueues the Pod's best-matching WorkloadClass if it tracks fallback state.
func (r *WorkloadClassReconciler) findWorkloadClassForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	wc, err := utils.FindBestMatchWorkloadClass(ctx, r.Client, nil, pod)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to find WorkloadClass for Pod", "pod", client.ObjectKeyFromObject(pod))
		return nil
	}
	if wc == nil || !tracksFallback(wc) {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(wc)}}
}
