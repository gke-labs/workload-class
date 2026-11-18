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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestDisruptionTracker_RecordAndDrain(t *testing.T) {
	tracker := NewDisruptionTracker(0)
	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "batch", Namespace: "team-a"},
	}
	key := types.NamespacedName{Namespace: "team-a", Name: "batch"}

	t1 := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)
	t0 := t1.Add(-time.Minute)

	if !tracker.Record(wc, t1) {
		t.Fatalf("Record(t1) = false, want true")
	}
	if !tracker.Record(wc, t2) {
		t.Fatalf("Record(t2) = false, want true")
	}
	// An older timestamp must not overwrite a newer recorded disruption.
	if !tracker.Record(wc, t0) {
		t.Fatalf("Record(t0) = false, want true")
	}

	got, ok := tracker.Drain(key)
	if !ok {
		t.Fatalf("Drain() ok = false, want true")
	}
	if !got.Equal(t2) {
		t.Errorf("Drain() = %v, want %v", got, t2)
	}

	// Second drain must be empty.
	if _, ok := tracker.Drain(key); ok {
		t.Errorf("second Drain() ok = true, want false")
	}

	select {
	case ev := <-tracker.Events():
		if ev.Object.GetName() != wc.Name || ev.Object.GetNamespace() != wc.Namespace {
			t.Errorf("event object = %s/%s, want %s/%s", ev.Object.GetNamespace(), ev.Object.GetName(), wc.Namespace, wc.Name)
		}
	default:
		t.Errorf("expected wake-up event on channel")
	}
}

func TestDisruptionTracker_FullBufferRetainsTimestamp(t *testing.T) {
	tracker := NewDisruptionTracker(1)
	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "batch", Namespace: "team-a"},
	}
	key := types.NamespacedName{Namespace: "team-a", Name: "batch"}

	t1 := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)

	if !tracker.Record(wc, t1) {
		t.Fatalf("first Record() = false, want true")
	}
	if tracker.Record(wc, t2) {
		t.Fatalf("second Record() with full channel = true, want false")
	}

	got, ok := tracker.Drain(key)
	if !ok || !got.Equal(t2) {
		t.Errorf("Drain() = (%v, %v), want (%v, true)", got, ok, t2)
	}
}

func TestDisruptionTracker_NilSafety(t *testing.T) {
	var nilTracker *DisruptionTracker
	wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "batch", Namespace: "team-a"}}
	key := types.NamespacedName{Namespace: "team-a", Name: "batch"}

	if nilTracker.Record(wc, time.Now()) {
		t.Errorf("nil tracker Record() = true, want false")
	}
	if _, ok := nilTracker.Drain(key); ok {
		t.Errorf("nil tracker Drain() ok = true, want false")
	}

	tracker := NewDisruptionTracker(1)
	if tracker.Record(nil, time.Now()) {
		t.Errorf("Record(nil) = true, want false")
	}
}
