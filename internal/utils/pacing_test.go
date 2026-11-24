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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestCalculateDisruptionBudget(t *testing.T) {
	intVal := func(v int32) *intstr.IntOrString { val := intstr.FromInt32(v); return &val }
	strVal := func(s string) *intstr.IntOrString { val := intstr.FromString(s); return &val }

	testCases := []struct {
		name         string
		dp           workloadsv1.DisruptionPolicy
		total, ready int
		want         int
		wantErr      bool
	}{
		{name: "no_pacing_policy", dp: workloadsv1.DisruptionPolicy{}, total: 4, ready: 4, want: 4},
		{name: "maxUnavailable_int_all_ready", dp: workloadsv1.DisruptionPolicy{MaxUnavailable: intVal(1)}, total: 4, ready: 4, want: 1},
		{name: "maxUnavailable_int_one_unready_blocks", dp: workloadsv1.DisruptionPolicy{MaxUnavailable: intVal(1)}, total: 4, ready: 3, want: 0},
		{name: "maxUnavailable_percent_rounds_up", dp: workloadsv1.DisruptionPolicy{MaxUnavailable: strVal("25%")}, total: 3, ready: 3, want: 1},
		{name: "maxUnavailable_percent_exhausted", dp: workloadsv1.DisruptionPolicy{MaxUnavailable: strVal("25%")}, total: 4, ready: 3, want: 0},
		{name: "minAvailable_int_allows_surplus", dp: workloadsv1.DisruptionPolicy{MinAvailable: intVal(2)}, total: 4, ready: 3, want: 1},
		{name: "minAvailable_int_at_floor_blocks", dp: workloadsv1.DisruptionPolicy{MinAvailable: intVal(2)}, total: 4, ready: 2, want: 0},
		{name: "minAvailable_percent_rounds_up", dp: workloadsv1.DisruptionPolicy{MinAvailable: strVal("75%")}, total: 3, ready: 3, want: 0},
		{name: "minAvailable_percent_allows_one", dp: workloadsv1.DisruptionPolicy{MinAvailable: strVal("75%")}, total: 4, ready: 4, want: 1},
		{name: "both_set_returns_error", dp: workloadsv1.DisruptionPolicy{MaxUnavailable: intVal(1), MinAvailable: intVal(2)}, total: 4, ready: 4, wantErr: true},
		{name: "negative_maxUnavailable_errors", dp: workloadsv1.DisruptionPolicy{MaxUnavailable: intVal(-1)}, total: 4, ready: 4, wantErr: true},
		{name: "invalid_percent_errors", dp: workloadsv1.DisruptionPolicy{MaxUnavailable: strVal("abc%")}, total: 4, ready: 4, wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CalculateDisruptionBudget(tc.dp, tc.total, tc.ready)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("CalculateDisruptionBudget() = (%d, %v), want (%d, err=%v)", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestStuckThresholdAndIsPodReady(t *testing.T) {
	zero, custom := int32(0), int32(45)
	if got, ok := StuckThreshold(workloadsv1.DisruptionPolicy{}); !ok || got != DefaultDisruptionStuckThreshold {
		t.Errorf("default StuckThreshold = (%v, %v), want (%v, true)", got, ok, DefaultDisruptionStuckThreshold)
	}
	if got, ok := StuckThreshold(workloadsv1.DisruptionPolicy{DisruptionStuckThresholdSeconds: &zero}); ok || got != 0 {
		t.Errorf("disabled StuckThreshold(0) = (%v, %v), want (0, false)", got, ok)
	}
	if got, ok := StuckThreshold(workloadsv1.DisruptionPolicy{DisruptionStuckThresholdSeconds: &custom}); !ok || got != 45*time.Second {
		t.Errorf("custom StuckThreshold = (%v, %v), want (45s, true)", got, ok)
	}

	readyPod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	if !IsPodReady(readyPod) {
		t.Error("expected Running+Ready pod to be ready")
	}
	unreadyPod := readyPod.DeepCopy()
	unreadyPod.Status.Conditions[0].Status = corev1.ConditionFalse
	if IsPodReady(unreadyPod) {
		t.Error("expected unready pod not to be ready")
	}
	now := metav1.Now()
	termPod := readyPod.DeepCopy()
	termPod.DeletionTimestamp = &now
	if IsPodReady(termPod) {
		t.Error("expected terminating pod not to be ready")
	}
}

func TestBlockedPodTracker(t *testing.T) {
	var nilTracker *BlockedPodTracker
	wcKey := types.NamespacedName{Namespace: "default", Name: "web"}
	if got := nilTracker.RecordOrElapsed("uid-1", time.Now(), time.Minute); got != 0 {
		t.Errorf("nil tracker returned %v, want 0", got)
	}
	if !nilTracker.TryBreakGlass(wcKey, "uid-1", time.Now(), time.Minute) {
		t.Error("nil tracker TryBreakGlass want true")
	}
	nilTracker.Clear("uid-1")
	nilTracker.Prune(time.Now())

	tracker := NewBlockedPodTracker()
	t0 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	gap := 2 * time.Minute

	// Continuous retries advance elapsed since firstBlocked.
	if got := tracker.RecordOrElapsed("pod-1", t0, gap); got != 0 {
		t.Fatalf("first RecordOrElapsed = %v, want 0", got)
	}
	if got := tracker.RecordOrElapsed("pod-1", t0.Add(90*time.Second), gap); got != 90*time.Second {
		t.Fatalf("second RecordOrElapsed = %v, want 90s", got)
	}
	// A quiet gap larger than resetGap resets the timer (cancelled drain later retried).
	if got := tracker.RecordOrElapsed("pod-1", t0.Add(5*time.Minute), gap); got != 0 {
		t.Fatalf("after gap RecordOrElapsed = %v, want 0 (timer reset)", got)
	}

	// Rate-limiting per WorkloadClass: parallel pods only break glass one at a time per cooldown.
	tracker.RecordOrElapsed("p-a", t0, gap)
	tracker.RecordOrElapsed("p-b", t0, gap)
	if !tracker.TryBreakGlass(wcKey, "p-a", t0.Add(gap), gap) {
		t.Fatal("first pod break-glass want true")
	}
	if tracker.TryBreakGlass(wcKey, "p-b", t0.Add(gap), gap) {
		t.Fatal("second concurrent pod break-glass within cooldown want false")
	}
	if !tracker.TryBreakGlass(wcKey, "p-b", t0.Add(2*gap), gap) {
		t.Fatal("second pod break-glass after cooldown want true")
	}

	// Prune drops based on lastSeen (not firstBlocked).
	tracker.RecordOrElapsed("active-pod", t0, gap)
	tracker.RecordOrElapsed("active-pod", t0.Add(90*time.Second), gap)
	tracker.Prune(t0.Add(60 * time.Second))
	if got := tracker.RecordOrElapsed("active-pod", t0.Add(150*time.Second), gap); got != 150*time.Second {
		t.Errorf("active-pod pruned prematurely, elapsed = %v, want 150s", got)
	}
	tracker.Prune(t0.Add(10 * time.Minute))
	if got := tracker.RecordOrElapsed("active-pod", t0.Add(10*time.Minute), gap); got != 0 {
		t.Errorf("expected stale active-pod pruned, got %v", got)
	}
}
