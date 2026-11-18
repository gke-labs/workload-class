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
	intVal := func(v int32) *intstr.IntOrString {
		val := intstr.FromInt32(v)
		return &val
	}
	strVal := func(s string) *intstr.IntOrString {
		val := intstr.FromString(s)
		return &val
	}

	testCases := []struct {
		name    string
		dp      workloadsv1.DisruptionPolicy
		total   int
		ready   int
		want    int
		wantErr bool
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
			if (err != nil) != tc.wantErr {
				t.Fatalf("CalculateDisruptionBudget() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("CalculateDisruptionBudget() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestStuckThresholdAndIsPodReady(t *testing.T) {
	if got := StuckThreshold(workloadsv1.DisruptionPolicy{}); got != DefaultDisruptionStuckThreshold {
		t.Errorf("default StuckThreshold = %v, want %v", got, DefaultDisruptionStuckThreshold)
	}
	if got := StuckThreshold(workloadsv1.DisruptionPolicy{DisruptionStuckThresholdSeconds: 45}); got != 45*time.Second {
		t.Errorf("custom StuckThreshold = %v, want 45s", got)
	}

	readyPod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if !IsPodReady(readyPod) {
		t.Error("expected Running+Ready pod to be ready")
	}
	unreadyPod := readyPod.DeepCopy()
	unreadyPod.Status.Conditions[0].Status = corev1.ConditionFalse
	if IsPodReady(unreadyPod) {
		t.Error("expected unready pod not to be ready")
	}
	now := metav1.Now()
	terminatingPod := readyPod.DeepCopy()
	terminatingPod.DeletionTimestamp = &now
	if IsPodReady(terminatingPod) {
		t.Error("expected terminating pod not to be ready")
	}
}

func TestBlockedPodTracker(t *testing.T) {
	var nilTracker *BlockedPodTracker
	if got := nilTracker.RecordOrElapsed("uid-1", time.Now()); got != 0 {
		t.Errorf("nil tracker returned %v, want 0", got)
	}
	nilTracker.Clear("uid-1")
	nilTracker.Prune(time.Now())

	tracker := NewBlockedPodTracker()
	t0 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	if got := tracker.RecordOrElapsed(types.UID("pod-1"), t0); got != 0 {
		t.Errorf("first RecordOrElapsed = %v, want 0", got)
	}
	if got := tracker.RecordOrElapsed(types.UID("pod-1"), t0.Add(90*time.Second)); got != 90*time.Second {
		t.Errorf("second RecordOrElapsed = %v, want 90s", got)
	}
	tracker.Clear(types.UID("pod-1"))
	if got := tracker.RecordOrElapsed(types.UID("pod-1"), t0.Add(150*time.Second)); got != 0 {
		t.Errorf("RecordOrElapsed after Clear = %v, want 0", got)
	}

	tracker.RecordOrElapsed(types.UID("old-pod"), t0)
	tracker.Prune(t0.Add(time.Minute))
	if got := tracker.RecordOrElapsed(types.UID("old-pod"), t0.Add(2*time.Minute)); got != 0 {
		t.Errorf("expected old-pod to be pruned, got elapsed %v", got)
	}
}
