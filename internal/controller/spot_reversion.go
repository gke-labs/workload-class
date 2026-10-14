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
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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

// reconcileSpotReversion evicts fallback Pods (Spot-targeted Pods running on non-Spot nodes) when Spot
// capacity is available, so their controllers recreate them and the Pod placement webhook targets Spot again.
// It only acts for WorkloadClasses that allow falling back to On-Demand, and evicts a fallback Pod in two cases:
//
//   - Forced reversion: a guardrail sets reversion.maxFallbackDuration and the Pod has been on fallback for at
//     least that long. This applies under any reversion action (Lazy included) and ignores SpotRatio and
//     minDurationOnFallback: the guardrail bounds how long any Pod may run on fallback. (Validation rejects
//     None together with maxFallbackDuration, so None WorkloadClasses never reach this path validated.)
//   - Active reversion: the policy's reversion action is Active, the workload's Spot share (Pods on Spot
//     nodes plus Pending Spot-targeted Pods) is still below its SpotRatio (utils.SpotShareMet), and the Pod
//     has been on fallback for at least minDurationOnFallback (or defaultMinDurationOnFallback when unset).
//
// Pods without a controller owner are never evicted, since nothing would recreate them. Fallback Pods are
// visited oldest first. Evictions go through the Eviction API, so they respect PodDisruptionBudgets and the
// disruption webhook. A rejection outside the disruption window stops evictions until the next window; inside
// the window it is retried after evictionRetryDelay, or tooNewRetryDelay when the Pod is below
// minInitialRunDurationDays.
//
// It returns how long to wait before reconciling again: the time until the next Pod crosses
// minDurationOnFallback or maxFallbackDuration, or the retry delay (0 if no requeue is needed).
func (r *GKESpotPlacementReconciler) reconcileSpotReversion(ctx context.Context, wc *workloadsv1.WorkloadClass, policy *workloadsv1.GKESpotPlacementPolicy, pods workloadPods, now time.Time) (time.Duration, error) {
	log := logf.FromContext(ctx)

	if !tracksFallback(wc) || len(pods.fallback) == 0 {
		return 0, nil
	}

	maxDuration, err := r.guardrailMaxFallbackDuration(ctx)
	if err != nil {
		return 0, err
	}
	active := reversionActionFor(policy) == workloadsv1.ActiveReversionAction
	if !active && maxDuration == nil {
		return 0, nil
	}

	spotRatio, err := utils.SpotRatioPercent(wc.Spec.CapacityStrategy.SpotRatio)
	if err != nil {
		return 0, err
	}

	spotCapacityAvailable, err := r.checkSpotCapacity(ctx)
	if err != nil {
		log.Error(err, "Failed to check Spot capacity")
		return 0, nil
	}
	log.V(1).Info("Checked Spot capacity", "spotCapacityAvailable", spotCapacityAvailable)
	if !spotCapacityAvailable {
		return 0, nil
	}

	minDuration := defaultMinDurationOnFallback
	if policy != nil && policy.Spec.Reversion.MinDurationOnFallback != nil {
		minDuration = policy.Spec.Reversion.MinDurationOnFallback.Duration
	}

	spotPods := pods.effectiveSpot()
	var requeue time.Duration
	for i := range pods.fallback {
		pod := &pods.fallback[i]
		if metav1.GetControllerOf(pod) == nil {
			log.V(1).Info("Skipping Spot reversion of Pod without a controller owner", "pod", pod.Name)
			continue
		}
		elapsed := now.Sub(podFallbackStartTime(pod))

		var reason string
		switch {
		case maxDuration != nil && elapsed >= *maxDuration:
			reason = "maxFallbackDuration exceeded"
		case !active:
			// Lazy under the limit: come back when this Pod crosses maxFallbackDuration.
			requeue = minPositiveDuration(requeue, *maxDuration-elapsed)
			continue
		case spotRatio <= 0 || utils.SpotShareMet(spotPods, pods.active, spotRatio):
			// Spot share satisfied; only forced evictions remain, which the first case handles.
			if maxDuration != nil {
				requeue = minPositiveDuration(requeue, *maxDuration-elapsed)
			}
			continue
		case elapsed < minDuration:
			requeue = minPositiveDuration(requeue, minDuration-elapsed)
			if maxDuration != nil {
				requeue = minPositiveDuration(requeue, *maxDuration-elapsed)
			}
			continue
		default:
			reason = "Active reversion"
		}

		evicted, delay, err := r.evictForReversion(ctx, wc, pod, now)
		if err != nil {
			return 0, err
		}
		if !evicted {
			if delay < 0 {
				// Blocked outside the disruption window: stop until the next window opens.
				return minPositiveDuration(requeue, -delay), nil
			}
			requeue = minPositiveDuration(requeue, delay)
			continue
		}
		log.Info("Evicted Pod from On-Demand fallback node", "pod", pod.Name, "node", pod.Spec.NodeName, "reason", reason, "onFallbackFor", elapsed.Round(time.Second))
		spotPods++
	}

	return requeue, nil
}

// evictForReversion evicts the Pod through the Eviction API. It returns whether the Pod was evicted and a
// requeue delay: positive to retry later, negative when evictions are blocked outside the disruption window
// (the magnitude is the time until the next window), zero otherwise.
func (r *GKESpotPlacementReconciler) evictForReversion(ctx context.Context, wc *workloadsv1.WorkloadClass, pod *corev1.Pod, now time.Time) (evicted bool, delay time.Duration, err error) {
	log := logf.FromContext(ctx)
	eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}
	err = r.SubResource("eviction").Create(ctx, pod, eviction)
	switch {
	case err == nil:
		return true, 0, nil
	case apierrors.IsNotFound(err):
		return false, 0, nil
	case apierrors.IsTooManyRequests(err) || apierrors.IsForbidden(err):
		// Rejected by a PodDisruptionBudget (429) or the disruption webhook (403).
		if disruptionWindowEnforced(wc) {
			if inWindow, nextWindow := utils.IsTimeInWindows(ctx, now, wc.Spec.DisruptionPolicy.AllowedDisruptionWindows); !inWindow {
				log.Info("Spot reversion eviction blocked outside the disruption window; waiting for the next window",
					"pod", pod.Name, "nextWindow", nextWindow)
				return false, -nextWindow, nil
			}
		}
		d := evictionRetryDelay
		if isTooNewDenial(err) {
			d = tooNewRetryDelay
		}
		log.Info("Spot reversion eviction rejected; will retry", "pod", pod.Name, "retryIn", d, "reason", err.Error())
		return false, d, nil
	default:
		return false, 0, fmt.Errorf("failed to evict Pod %s/%s for Spot reversion: %w", pod.Namespace, pod.Name, err)
	}
}

// disruptionWindowEnforced reports whether the WorkloadClass's temporal disruption windows are currently
// enforced on evictions. EmergencyOverride and MaintenanceReadiness=Overdue bypass temporal windows in both
// the disruption webhook and the WorkloadClass-managed PDB.
func disruptionWindowEnforced(wc *workloadsv1.WorkloadClass) bool {
	return !wc.Spec.DisruptionPolicy.EmergencyOverride &&
		wc.Status.MaintenanceReadiness != workloadsv1.ReadinessOverdue
}

// guardrailMaxFallbackDuration returns the strictest (smallest) reversion.maxFallbackDuration across all
// gke-spot-placement guardrail constraints, or nil when none sets it. Guardrails with unparsable parameters
// are skipped; the WorkloadClassGuardrail controller reports those.
func (r *GKESpotPlacementReconciler) guardrailMaxFallbackDuration(ctx context.Context) (*time.Duration, error) {
	guardrails := &workloadsv1.WorkloadClassGuardrailList{}
	if err := r.List(ctx, guardrails); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClassGuardrails: %w", err)
	}

	var strictest *time.Duration
	for i := range guardrails.Items {
		g := &guardrails.Items[i]
		if g.Spec.PluginConstraints == nil {
			continue
		}
		for _, c := range g.Spec.PluginConstraints.Placement {
			if c.Name != workloadsv1.PluginNameGKESpotPlacement || c.Parameters == nil || len(c.Parameters.Raw) == 0 {
				continue
			}
			params, err := utils.ParseGKESpotGuardrailParameters(c.Parameters.Raw)
			if err != nil {
				logf.FromContext(ctx).Error(err, "Skipping guardrail with invalid gke-spot-placement parameters", "guardrail", g.Name)
				continue
			}
			if params.Reversion == nil || params.Reversion.MaxFallbackDuration == nil || params.Reversion.MaxFallbackDuration.Duration < 0 {
				continue
			}
			d := params.Reversion.MaxFallbackDuration.Duration
			if strictest == nil || d < *strictest {
				strictest = &d
			}
		}
	}
	return strictest, nil
}

// findWorkloadClassesForGuardrail maps a WorkloadClassGuardrail event to reconcile requests for every
// GKE-managed WorkloadClass that tracks Spot fallback, so changes to reversion.maxFallbackDuration take
// effect without waiting for another event.
func (r *GKESpotPlacementReconciler) findWorkloadClassesForGuardrail(ctx context.Context, obj client.Object) []reconcile.Request {
	if _, ok := obj.(*workloadsv1.WorkloadClassGuardrail); !ok {
		return nil
	}
	return r.enqueueSpotTargetingWorkloadClasses(ctx)
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
