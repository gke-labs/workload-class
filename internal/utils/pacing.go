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
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// DefaultDisruptionStuckThreshold is the default duration an in-window eviction may remain blocked
// by pacing before break-glass permits it to proceed.
const DefaultDisruptionStuckThreshold = 120 * time.Second

// HasPacingPolicy reports whether the DisruptionPolicy configures concurrent disruption pacing.
func HasPacingPolicy(dp workloadsv1.DisruptionPolicy) bool {
	return dp.MaxUnavailable != nil || dp.MinAvailable != nil
}

// StuckThreshold returns the configured stuck-eviction break-glass threshold, defaulting to
// DefaultDisruptionStuckThreshold when unset or non-positive.
func StuckThreshold(dp workloadsv1.DisruptionPolicy) time.Duration {
	if dp.DisruptionStuckThresholdSeconds <= 0 {
		return DefaultDisruptionStuckThreshold
	}
	return time.Duration(dp.DisruptionStuckThresholdSeconds) * time.Second
}

// IsPodReady reports whether pod is active, Running, and has PodReady=True.
func IsPodReady(pod *corev1.Pod) bool {
	if !IsPodActive(pod) || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// CalculateDisruptionBudget computes how many additional governed Pods may be disrupted concurrently
// under dp.MaxUnavailable or dp.MinAvailable, matching Kubernetes PodDisruptionBudget rounding semantics.
//
// Returns (0, nil) when totalPods <= 0. Returns an error if both fields are set or an IntOrString is invalid.
func CalculateDisruptionBudget(dp workloadsv1.DisruptionPolicy, totalPods, readyPods int) (int, error) {
	if dp.MaxUnavailable != nil && dp.MinAvailable != nil {
		return 0, fmt.Errorf("maxUnavailable and minAvailable are mutually exclusive")
	}
	if !HasPacingPolicy(dp) || totalPods <= 0 {
		return totalPods, nil
	}

	var desiredHealthy int
	if dp.MaxUnavailable != nil {
		if dp.MaxUnavailable.Type == intstr.Int && dp.MaxUnavailable.IntVal < 0 {
			return 0, fmt.Errorf("maxUnavailable must be non-negative, got %d", dp.MaxUnavailable.IntVal)
		}
		maxUnavail, err := intstr.GetScaledValueFromIntOrPercent(dp.MaxUnavailable, totalPods, true)
		if err != nil {
			return 0, fmt.Errorf("invalid maxUnavailable %q: %w", dp.MaxUnavailable.String(), err)
		}
		desiredHealthy = max(0, totalPods-maxUnavail)
	} else {
		if dp.MinAvailable.Type == intstr.Int && dp.MinAvailable.IntVal < 0 {
			return 0, fmt.Errorf("minAvailable must be non-negative, got %d", dp.MinAvailable.IntVal)
		}
		minAvail, err := intstr.GetScaledValueFromIntOrPercent(dp.MinAvailable, totalPods, true)
		if err != nil {
			return 0, fmt.Errorf("invalid minAvailable %q: %w", dp.MinAvailable.String(), err)
		}
		desiredHealthy = min(totalPods, max(0, minAvail))
	}

	return max(0, readyPods-desiredHealthy), nil
}

// BlockedPodTracker tracks when a Pod's eviction was first blocked by in-window pacing so the
// disruption webhook can apply break-glass after StuckThreshold without mutating the Pod.
type BlockedPodTracker struct {
	mu        sync.Mutex
	blockedAt map[types.UID]time.Time
}

// NewBlockedPodTracker constructs an empty BlockedPodTracker.
func NewBlockedPodTracker() *BlockedPodTracker {
	return &BlockedPodTracker{
		blockedAt: map[types.UID]time.Time{},
	}
}

// RecordOrElapsed records now as the first-blocked timestamp for uid if not already present,
// and returns the elapsed duration since uid was first blocked.
func (t *BlockedPodTracker) RecordOrElapsed(uid types.UID, now time.Time) time.Duration {
	if t == nil || uid == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	first, ok := t.blockedAt[uid]
	if !ok {
		t.blockedAt[uid] = now
		return 0
	}
	return max(0, now.Sub(first))
}

// Clear removes any tracked first-blocked timestamp for uid.
func (t *BlockedPodTracker) Clear(uid types.UID) {
	if t == nil || uid == "" {
		return
	}
	t.mu.Lock()
	delete(t.blockedAt, uid)
	t.mu.Unlock()
}

// Prune removes entries whose first-blocked timestamp is strictly before cutoff.
func (t *BlockedPodTracker) Prune(cutoff time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	for uid, at := range t.blockedAt {
		if at.Before(cutoff) {
			delete(t.blockedAt, uid)
		}
	}
	t.mu.Unlock()
}
