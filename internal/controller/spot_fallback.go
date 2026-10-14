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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
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
	return utils.TargetsSpot(wc) && wc.Spec.CapacityStrategy.FallbackAction == workloadsv1.FallbackActionFallbackToOnDemand
}

// workloadPods is the classification of a WorkloadClass's active Pods by placement.
type workloadPods struct {
	// active is the number of active (non-terminating, non-terminal) Pods selected by the WorkloadClass,
	// including unscheduled ones.
	active int
	// fallback is the number of Spot-targeted Pods scheduled on non-Spot nodes.
	fallback int
}

// desiredFallbackCondition computes the InFallback condition the WorkloadClass should have. It returns nil when
// the condition should be absent, and (nil, false) in the second value when the current condition should be
// left untouched.
//
// The workload is in fallback when any Spot-targeted Pod (one carrying the Spot nodeSelector or toleration) is
// running on a non-Spot node. Pods placed on On-Demand on purpose (Type OnDemand, or to meet a SpotRatio below
// 100%) carry no Spot targeting, and unscheduled Pods are ignored, so scale-ups and rollouts don't register as
// fallback.
//
//   - Not applicable (no Spot targeting or FallbackAction is not FallbackToOnDemand): absent.
//   - In fallback: InFallback=True.
//   - Not in fallback: InFallback=False, except with reversion None, where InFallback=True is latched (so the
//     Pod placement webhook keeps new Pods on On-Demand) until the workload scales down to zero Pods.
func desiredFallbackCondition(wc *workloadsv1.WorkloadClass, pods workloadPods, reversion workloadsv1.ReversionAction) (cond *metav1.Condition, keep bool) {
	if !tracksFallback(wc) {
		return nil, false
	}

	c := &metav1.Condition{Type: workloadsv1.ConditionTypeInFallback, ObservedGeneration: wc.Generation}
	latched := meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeInFallback)

	switch {
	case pods.fallback > 0:
		c.Status, c.Reason, c.Message = metav1.ConditionTrue, workloadsv1.ReasonFallbackActive, fallbackActiveMsg
	case latched && reversion == workloadsv1.NoneReversionAction && pods.active > 0:
		return nil, true
	case pods.active == 0:
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, workloadsv1.ReasonSpotTargetMet, scaledToZeroMsg
	default:
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, workloadsv1.ReasonSpotTargetMet, spotTargetMetMsg
	}
	return c, false
}

// reconcileFallbackState computes and persists the InFallback condition for the WorkloadClass. It returns
// the classification of the WorkloadClass's Pods so later steps (reversion) can reuse it.
func (r *GKESpotPlacementReconciler) reconcileFallbackState(ctx context.Context, wc *workloadsv1.WorkloadClass) (workloadPods, error) {
	var pods workloadPods
	if tracksFallback(wc) {
		spotNodes, err := r.listSpotNodeNames(ctx)
		if err != nil {
			return pods, err
		}
		pods, err = r.classifyWorkloadPods(ctx, wc, spotNodes)
		if err != nil {
			return pods, err
		}
	}

	reversion, err := r.effectiveReversionAction(ctx, wc)
	if err != nil {
		return pods, err
	}

	desired, keep := desiredFallbackCondition(wc, pods, reversion)
	if keep {
		return pods, nil
	}
	if err := r.setFallbackCondition(ctx, wc, desired); err != nil {
		return pods, err
	}
	return pods, nil
}

// clearFallbackState removes the InFallback condition, used when the WorkloadClass leaves the plugin's scope
// or is no longer validated (the Pod placement webhook doesn't act on it, so the condition would be stale).
func (r *GKESpotPlacementReconciler) clearFallbackState(ctx context.Context, wc *workloadsv1.WorkloadClass) error {
	return r.setFallbackCondition(ctx, wc, nil)
}

// setFallbackCondition persists the InFallback condition (or removes it when desired is nil) on the latest
// WorkloadClass, retrying on conflicts and touching no other status fields. wc is updated in place on success.
func (r *GKESpotPlacementReconciler) setFallbackCondition(ctx context.Context, wc *workloadsv1.WorkloadClass, desired *metav1.Condition) error {
	key := client.ObjectKeyFromObject(wc)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &workloadsv1.WorkloadClass{}
		if err := r.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}

		var changed bool
		if desired == nil {
			changed = meta.RemoveStatusCondition(&latest.Status.Conditions, workloadsv1.ConditionTypeInFallback)
		} else {
			changed = meta.SetStatusCondition(&latest.Status.Conditions, *desired)
		}
		if !changed {
			wc.Status.Conditions = latest.Status.Conditions
			return nil
		}

		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		wc.Status.Conditions = latest.Status.Conditions
		logf.FromContext(ctx).Info("Updated Spot fallback state", "inFallback", conditionStatus(desired))
		return nil
	})
}

func conditionStatus(c *metav1.Condition) string {
	if c == nil {
		return "<removed>"
	}
	return string(c.Status)
}

// effectiveReversionAction returns the reversion action from the WorkloadClass's attached GKESpotPlacementPolicy,
// defaulting to None when the policy leaves it unset. Without an attached GKESpotPlacementPolicy there is no
// reversion configuration, and reversion is treated as Lazy (matching the Pod placement webhook).
func (r *GKESpotPlacementReconciler) effectiveReversionAction(ctx context.Context, wc *workloadsv1.WorkloadClass) (workloadsv1.ReversionAction, error) {
	ref := wc.Spec.InfrastructureProfileRef
	if !utils.ReferencesGKESpotPlacementPolicy(ref) ||
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

// listSpotNodeNames returns the names of all nodes labeled as GKE Spot nodes.
func (r *GKESpotPlacementReconciler) listSpotNodeNames(ctx context.Context) (map[string]bool, error) {
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, client.MatchingLabels{utils.SpotLabelKey: utils.SpotLabelValue}); err != nil {
		return nil, fmt.Errorf("failed to list Spot nodes: %w", err)
	}

	spotNodes := make(map[string]bool, len(nodes.Items))
	for i := range nodes.Items {
		spotNodes[nodes.Items[i].Name] = true
	}
	return spotNodes, nil
}

// classifyWorkloadPods counts the active Pods selected by the WorkloadClass and how many of them are
// Spot-targeted Pods scheduled on non-Spot nodes. A nil PodSelector selects all Pods in the namespace.
func (r *GKESpotPlacementReconciler) classifyWorkloadPods(ctx context.Context, wc *workloadsv1.WorkloadClass, spotNodes map[string]bool) (workloadPods, error) {
	var result workloadPods

	selector := labels.Everything()
	if wc.Spec.PodSelector != nil {
		var err error
		selector, err = metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			return result, fmt.Errorf("invalid podSelector: %w", err)
		}
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return result, fmt.Errorf("failed to list Pods: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !utils.IsPodActive(pod) {
			continue
		}
		result.active++
		if pod.Spec.NodeName != "" && !spotNodes[pod.Spec.NodeName] && utils.IsPodTargetedForSpot(pod) {
			result.fallback++
		}
	}
	return result, nil
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
				utils.IsPodActive(oldPod) != utils.IsPodActive(newPod)
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// findWorkloadClassForPod enqueues the Pod's best-matching WorkloadClass if the plugin tracks its fallback
// state. Namespaces without any WorkloadClass are skipped before the (more expensive) best-match lookup, so
// Pod churn in unrelated namespaces costs a single cached List.
func (r *GKESpotPlacementReconciler) findWorkloadClassForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	wcs := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, wcs, client.InNamespace(pod.Namespace), client.Limit(1)); err != nil || len(wcs.Items) == 0 {
		return nil
	}

	wc, err := utils.FindBestMatchWorkloadClass(ctx, r.Client, nil, pod)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to find WorkloadClass for Pod", "pod", client.ObjectKeyFromObject(pod))
		return nil
	}
	if wc == nil || !utils.IsGKESpotManaged(wc) || !tracksFallback(wc) {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(wc)}}
}
