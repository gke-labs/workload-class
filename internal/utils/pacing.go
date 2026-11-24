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
// by pacing before break-glass permits it to proceed when DisruptionStuckThresholdSeconds is omitted.
const DefaultDisruptionStuckThreshold = 120 * time.Second

// HasPacingPolicy reports whether the DisruptionPolicy configures concurrent disruption pacing.
func HasPacingPolicy(dp workloadsv1.DisruptionPolicy) bool {
	return dp.MaxUnavailable != nil || dp.MinAvailable != nil
}

// StuckThreshold returns the configured stuck-eviction break-glass threshold and whether break-glass
// is enabled. When DisruptionStuckThresholdSeconds is nil, it defaults to (DefaultDisruptionStuckThreshold, true).
// When explicitly set to 0 (or less), break-glass is disabled and (0, false) is returned.
func StuckThreshold(dp workloadsv1.DisruptionPolicy) (time.Duration, bool) {
	if dp.DisruptionStuckThresholdSeconds == nil {
		return DefaultDisruptionStuckThreshold, true
	}
	if *dp.DisruptionStuckThresholdSeconds <= 0 {
		return 0, false
	}
	return time.Duration(*dp.DisruptionStuckThresholdSeconds) * time.Second, true
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

type blockedPodEntry struct {
	firstBlocked time.Time
	lastSeen     time.Time
}

// BlockedPodTracker tracks when Pods are continuously blocked by in-window pacing and rate-limits
// break-glass evictions per WorkloadClass so concurrent drains cannot release all blocked Pods at once.
//
// Note: state is kept in memory per webhook replica and is not shared across manager replicas or
// persisted across restarts. Running multiple webhook replicas causes each instance to track its
// own independent timer and cooldown state.
type BlockedPodTracker struct {
	mu             sync.Mutex
	blocked        map[types.UID]blockedPodEntry
	lastBreakGlass map[types.NamespacedName]time.Time
}

// NewBlockedPodTracker constructs an empty BlockedPodTracker.
func NewBlockedPodTracker() *BlockedPodTracker {
	return &BlockedPodTracker{
		blocked:        map[types.UID]blockedPodEntry{},
		lastBreakGlass: map[types.NamespacedName]time.Time{},
	}
}

// RecordOrElapsed tracks blocked eviction attempts for uid and returns how long uid has been
// continuously blocked. If resetGap > 0 and the time since uid was last seen exceeds resetGap,
// the timer resets to now (so a cancelled drain hours earlier does not immediately break glass).
func (t *BlockedPodTracker) RecordOrElapsed(uid types.UID, now time.Time, resetGap time.Duration) time.Duration {
	if t == nil || uid == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.blocked[uid]
	if !ok || (resetGap > 0 && now.Sub(entry.lastSeen) > resetGap) {
		t.blocked[uid] = blockedPodEntry{firstBlocked: now, lastSeen: now}
		return 0
	}
	if now.After(entry.lastSeen) {
		entry.lastSeen = now
		t.blocked[uid] = entry
	}
	return max(0, now.Sub(entry.firstBlocked))
}

// TryBreakGlass checks whether wcKey may perform a break-glass eviction at now. At most one
// break-glass eviction per WorkloadClass is allowed within cooldown. On success, it records now
// as wcKey's latest break-glass timestamp, clears uid's blocked entry, and returns true.
func (t *BlockedPodTracker) TryBreakGlass(wcKey types.NamespacedName, uid types.UID, now time.Time, cooldown time.Duration) bool {
	if t == nil {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if last, ok := t.lastBreakGlass[wcKey]; ok && cooldown > 0 && now.Sub(last) < cooldown {
		return false
	}
	t.lastBreakGlass[wcKey] = now
	if uid != "" {
		delete(t.blocked, uid)
	}
	return true
}

// Clear removes any tracked blocked entry for uid.
func (t *BlockedPodTracker) Clear(uid types.UID) {
	if t == nil || uid == "" {
		return
	}
	t.mu.Lock()
	delete(t.blocked, uid)
	t.mu.Unlock()
}

// Prune removes blocked Pod entries whose last-seen timestamp is strictly before cutoff,
// and per-WorkloadClass break-glass records strictly before cutoff.
func (t *BlockedPodTracker) Prune(cutoff time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	for uid, entry := range t.blocked {
		if entry.lastSeen.Before(cutoff) {
			delete(t.blocked, uid)
		}
	}
	for wcKey, at := range t.lastBreakGlass {
		if at.Before(cutoff) {
			delete(t.lastBreakGlass, wcKey)
		}
	}
	t.mu.Unlock()
}
