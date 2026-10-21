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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	// defaultMinDurationOnFallback is the minimum time a Pod must have run on an On-Demand node before Active
	// reversion evicts it, when the GKESpotPlacementPolicy does not set reversion.minDurationOnFallback.
	//
	// Spot capacity signals can be wrong at the moment of eviction (the autoscaler clears Backoff before a node
	// is actually provisioned, or the Ready-node fallback is true during a stockout). Without a floor, an
	// evicted Pod whose replacement lands on On-Demand again is evicted again as soon as it is bound, in a
	// loop paced only by the PodDisruptionBudget. The floor bounds that to one eviction per Pod per period and
	// gives the autoscaler a full scale-up attempt between tries. An explicit value, including 0s, is honored.
	defaultMinDurationOnFallback = 5 * time.Minute

	// evictionRetryDelay is how long to wait before retrying evictions rejected by a PodDisruptionBudget while
	// the disruption window is open.
	evictionRetryDelay = 10 * time.Second

	// tooNewRetryDelay is how long to wait before retrying when the disruption webhook denies an eviction
	// because the Pod is younger than the WorkloadClass's minInitialRunDurationDays. That condition lasts
	// hours or days, so retrying every few seconds would be wasteful.
	tooNewRetryDelay = 30 * time.Minute
)

// reconcileSpotReversion performs Active reversion: when Spot capacity is available, it evicts fallback Pods
// (Spot-targeted Pods running on non-Spot nodes) so their controllers recreate them, at which point the Pod
// placement webhook targets Spot again.
//
// It only acts when the WorkloadClass allows falling back to On-Demand and its attached GKESpotPlacementPolicy
// has reversion action Active. Lazy and None never evict; without a policy, reversion is treated as Lazy.
// Evictions stop once the Spot share of the workload (Pods on Spot nodes plus Pending Spot-targeted Pods)
// satisfies its SpotRatio under the same rule the webhook applies at admission (utils.SpotShareMet). Pods
// younger than minDurationOnFallback (or defaultMinDurationOnFallback when unset) are skipped, as are Pods
// without a controller owner, which would not be recreated.
//
// Evictions go through the Eviction API, so they respect PodDisruptionBudgets and the disruption webhook. A
// rejection outside the disruption window stops evictions until the next window; inside the window it is
// retried after evictionRetryDelay, or tooNewRetryDelay when the Pod is below minInitialRunDurationDays.
//
// It returns how long to wait before reconciling again (0 if no requeue is needed).
func (r *GKESpotPlacementReconciler) reconcileSpotReversion(ctx context.Context, wc *workloadsv1.WorkloadClass, policy *workloadsv1.GKESpotPlacementPolicy, pods workloadPods, now time.Time) (time.Duration, error) {
	log := logf.FromContext(ctx)

	if !tracksFallback(wc) || reversionActionFor(policy) != workloadsv1.ActiveReversionAction || len(pods.fallback) == 0 {
		return 0, nil
	}

	spotRatio, err := utils.SpotRatioPercent(wc.Spec.CapacityStrategy.SpotRatio)
	if err != nil {
		return 0, err
	}
	if spotRatio <= 0 {
		return 0, nil
	}

	minDuration := defaultMinDurationOnFallback
	if policy.Spec.Reversion.MinDurationOnFallback != nil {
		minDuration = policy.Spec.Reversion.MinDurationOnFallback.Duration
	}

	spotPods := pods.effectiveSpot()
	var requeue time.Duration
	for i := range pods.fallback {
		if utils.SpotShareMet(spotPods, pods.active, spotRatio) {
			break
		}

		pod := &pods.fallback[i]
		if metav1.GetControllerOf(pod) == nil {
			log.V(1).Info("Skipping Spot reversion of Pod without a controller owner", "pod", pod.Name)
			continue
		}
		if elapsed := now.Sub(podFallbackStartTime(pod)); elapsed < minDuration {
			requeue = minPositiveDuration(requeue, minDuration-elapsed)
			continue
		}

		eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}
		if err := r.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
			switch {
			case apierrors.IsNotFound(err):
				continue
			case apierrors.IsTooManyRequests(err) || apierrors.IsForbidden(err):
				// Rejected by a PodDisruptionBudget (429) or the disruption webhook (403).
				if disruptionWindowEnforced(wc) {
					if inWindow, nextWindow := utils.IsTimeInWindows(ctx, now, wc.Spec.DisruptionPolicy.AllowedDisruptionWindows); !inWindow {
						log.Info("Spot reversion eviction blocked outside the disruption window; waiting for the next window",
							"pod", pod.Name, "nextWindow", nextWindow)
						return minPositiveDuration(requeue, nextWindow), nil
					}
				}
				delay := evictionRetryDelay
				if isTooNewDenial(err) {
					delay = tooNewRetryDelay
				}
				log.Info("Spot reversion eviction rejected; will retry", "pod", pod.Name, "retryIn", delay, "reason", err.Error())
				requeue = minPositiveDuration(requeue, delay)
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

// disruptionWindowEnforced reports whether the WorkloadClass's temporal disruption windows are currently
// enforced on evictions. EmergencyOverride and MaintenanceReadiness=Overdue bypass temporal windows in both
// the disruption webhook and the WorkloadClass-managed PDB.
func disruptionWindowEnforced(wc *workloadsv1.WorkloadClass) bool {
	return !wc.Spec.DisruptionPolicy.EmergencyOverride &&
		wc.Status.MaintenanceReadiness != workloadsv1.ReadinessOverdue
}

// isTooNewDenial reports whether an eviction error is the disruption webhook denying because the Pod is
// younger than the WorkloadClass's minInitialRunDurationDays.
func isTooNewDenial(err error) bool {
	return apierrors.IsForbidden(err) && strings.Contains(err.Error(), "pod is too new")
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
