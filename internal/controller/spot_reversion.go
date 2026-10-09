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
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// evictionRetryDelay is how long to wait before retrying evictions that were rejected (e.g. by a
// PodDisruptionBudget) while the disruption window is open.
const evictionRetryDelay = 10 * time.Second

// reconcileSpotReversion performs Active reversion: when Spot capacity is available, it evicts Pods of the
// WorkloadClass that are running on On-Demand (fallback) nodes so their controllers recreate them, at which
// point the Pod placement webhook targets Spot again.
//
// It only acts when the WorkloadClass references an attached GKESpotPlacementPolicy whose reversion action is
// Active. Lazy and None reversion never evict; without a policy, reversion is treated as Lazy. Evictions stop
// once the Spot share of the workload reaches its SpotRatio. Pods younger than MinDurationOnFallback, and Pods
// without a controller owner (which would not be recreated), are skipped.
//
// Evictions go through the Eviction API, so they respect PodDisruptionBudgets and the disruption webhook. If
// an eviction is rejected while the disruption window is closed, evictions stop until the next window;
// otherwise they are retried after evictionRetryDelay.
//
// It returns how long to wait before reconciling again (0 if no requeue is needed).
func (r *WorkloadClassReconciler) reconcileSpotReversion(ctx context.Context, wc *workloadsv1.WorkloadClass, now time.Time) (time.Duration, error) {
	log := logf.FromContext(ctx)

	policy, err := r.activeReversionPolicy(ctx, wc)
	if err != nil || policy == nil {
		return 0, err
	}

	spotRatio := 100
	if wc.Spec.CapacityStrategy.SpotRatio != "" {
		parsed, err := parsePercentage(wc.Spec.CapacityStrategy.SpotRatio)
		if err != nil {
			return 0, fmt.Errorf("invalid spotRatio %q: %w", wc.Spec.CapacityStrategy.SpotRatio, err)
		}
		spotRatio = parsed
	}
	if spotRatio <= 0 {
		return 0, nil
	}

	spotNodes, err := r.listSpotNodeNames(ctx)
	if err != nil {
		return 0, err
	}

	spotPods, onDemandPods, totalActivePods, err := r.classifyWorkloadClassPods(ctx, wc, spotNodes)
	if err != nil {
		return 0, err
	}

	var minDuration time.Duration
	if policy.Spec.Reversion.MinDurationOnFallback != nil {
		minDuration = policy.Spec.Reversion.MinDurationOnFallback.Duration
	}

	var requeue time.Duration
	for i := range onDemandPods {
		if spotPods*100 >= totalActivePods*spotRatio {
			break
		}

		pod := &onDemandPods[i]
		if metav1.GetControllerOf(pod) == nil {
			log.V(1).Info("Skipping Spot reversion of Pod without a controller owner", "pod", pod.Name)
			continue
		}

		if minDuration > 0 {
			if elapsed := now.Sub(podFallbackStartTime(pod)); elapsed < minDuration {
				requeue = minPositiveDuration(requeue, minDuration-elapsed)
				continue
			}
		}

		eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}
		if err := r.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
			switch {
			case apierrors.IsNotFound(err):
				continue
			case apierrors.IsTooManyRequests(err) || apierrors.IsForbidden(err):
				// Rejected by a PodDisruptionBudget (429) or the disruption webhook (403).
				if inWindow, nextWindow := utils.IsTimeInWindows(ctx, now, wc.Spec.DisruptionPolicy.AllowedDisruptionWindows); !inWindow {
					log.Info("Spot reversion eviction blocked outside the disruption window; waiting for the next window",
						"pod", pod.Name, "nextWindow", nextWindow)
					return minPositiveDuration(requeue, nextWindow), nil
				}
				log.Info("Spot reversion eviction rejected; will retry", "pod", pod.Name, "reason", err.Error())
				requeue = minPositiveDuration(requeue, evictionRetryDelay)
				continue
			default:
				return 0, fmt.Errorf("failed to evict Pod %s/%s for Spot reversion: %w", pod.Namespace, pod.Name, err)
			}
		}

		log.Info("Evicted Pod from On-Demand fallback node for Spot reversion", "pod", pod.Name, "node", pod.Spec.NodeName)
		spotPods++
	}

	return requeue, nil
}

// activeReversionPolicy returns the WorkloadClass's GKESpotPlacementPolicy if Active reversion applies: the
// WorkloadClass targets Spot, references a GKESpotPlacementPolicy that has attached
// (PlacementPluginAttached=True), and the policy's reversion action is Active. Otherwise it returns nil.
func (r *WorkloadClassReconciler) activeReversionPolicy(ctx context.Context, wc *workloadsv1.WorkloadClass) (*workloadsv1.GKESpotPlacementPolicy, error) {
	ref := wc.Spec.InfrastructureProfileRef
	if !targetsSpot(wc) || !referencesSpotPolicy(ref) ||
		!meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached) {
		return nil, nil
	}

	policy := &workloadsv1.GKESpotPlacementPolicy{}
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name}, policy); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if policy.Spec.Reversion.Action != workloadsv1.ActiveReversionAction {
		return nil, nil
	}
	return policy, nil
}

// listSpotNodeNames returns the names of all nodes labeled as GKE Spot nodes.
func (r *WorkloadClassReconciler) listSpotNodeNames(ctx context.Context) (map[string]bool, error) {
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, client.MatchingLabels{spotNodeLabelKey: spotNodeLabelValue}); err != nil {
		return nil, fmt.Errorf("failed to list Spot nodes: %w", err)
	}

	spotNodes := make(map[string]bool, len(nodes.Items))
	for i := range nodes.Items {
		spotNodes[nodes.Items[i].Name] = true
	}
	return spotNodes, nil
}

// classifyWorkloadClassPods splits the active (non-terminating, non-terminal) Pods selected by the
// WorkloadClass into those scheduled on Spot nodes and those scheduled on On-Demand nodes. Unscheduled Pods
// count towards totalActivePods only. A nil PodSelector selects all Pods in the namespace.
func (r *WorkloadClassReconciler) classifyWorkloadClassPods(ctx context.Context, wc *workloadsv1.WorkloadClass, spotNodes map[string]bool) (spotPods int, onDemandPods []corev1.Pod, totalActivePods int, err error) {
	selector := labels.Everything()
	if wc.Spec.PodSelector != nil {
		selector, err = metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			return 0, nil, 0, fmt.Errorf("invalid podSelector: %w", err)
		}
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return 0, nil, 0, fmt.Errorf("failed to list Pods: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !isPodActive(pod) {
			continue
		}

		totalActivePods++
		if pod.Spec.NodeName == "" {
			continue
		}
		if spotNodes[pod.Spec.NodeName] {
			spotPods++
		} else {
			onDemandPods = append(onDemandPods, *pod)
		}
	}
	return spotPods, onDemandPods, totalActivePods, nil
}

// podFallbackStartTime approximates when the Pod started running on its (On-Demand) node: its start time,
// else when it was scheduled, else its creation time.
func podFallbackStartTime(pod *corev1.Pod) time.Time {
	if pod.Status.StartTime != nil && !pod.Status.StartTime.IsZero() {
		return pod.Status.StartTime.Time
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionTrue && !cond.LastTransitionTime.IsZero() {
			return cond.LastTransitionTime.Time
		}
	}
	return pod.CreationTimestamp.Time
}

// minPositiveDuration returns the smaller of a and b, ignoring non-positive values.
func minPositiveDuration(a, b time.Duration) time.Duration {
	switch {
	case a <= 0:
		return b
	case b <= 0:
		return a
	default:
		return min(a, b)
	}
}
