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
	"reflect"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestPendingLeaseTracker(t *testing.T) {
	wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "team-a"}}
	otherWC := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "other-wc", Namespace: "team-a"}}
	wcKey := types.NamespacedName{Namespace: "team-a", Name: "wc"}
	otherWCKey := types.NamespacedName{Namespace: "team-a", Name: "other-wc"}

	newPod := func(name string, uid types.UID) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: uid}}
	}
	subjectA := workloadsv1.Subject{Kind: "User", Name: "user-a"}
	subjectB := workloadsv1.Subject{Kind: "ServiceAccount", Name: "cluster-autoscaler", Namespace: "kube-system"}

	t.Run("enqueue_signals_and_drain_returns_sorted_leases", func(t *testing.T) {
		tracker := NewPendingLeaseTracker(10)
		if !tracker.Enqueue(wc, newPod("pod-b", "uid-b"), subjectA) {
			t.Fatal("Enqueue() = false, want true")
		}
		if !tracker.Enqueue(wc, newPod("pod-a", "uid-a"), subjectB) {
			t.Fatal("Enqueue() = false, want true")
		}

		for range 2 {
			select {
			case ev := <-tracker.Events():
				if ev.Object.GetName() != wc.Name || ev.Object.GetNamespace() != wc.Namespace {
					t.Errorf("Events() = %s/%s, want %s/%s", ev.Object.GetNamespace(), ev.Object.GetName(), wc.Namespace, wc.Name)
				}
			default:
				t.Fatal("Events() did not receive a signal")
			}
		}

		want := []PendingLease{
			{PodName: "pod-a", PodUID: "uid-a", Subject: subjectB},
			{PodName: "pod-b", PodUID: "uid-b", Subject: subjectA},
		}
		if got := tracker.Drain(wcKey); !reflect.DeepEqual(got, want) {
			t.Errorf("Drain() = %+v, want %+v", got, want)
		}
		if got := tracker.Drain(wcKey); got != nil {
			t.Errorf("Drain() after drain = %+v, want nil", got)
		}
	})

	t.Run("first_subject_wins_for_same_pod_instance", func(t *testing.T) {
		tracker := NewPendingLeaseTracker(10)
		tracker.Enqueue(wc, newPod("pod-a", "uid-a"), subjectA)
		tracker.Enqueue(wc, newPod("pod-a", "uid-a"), subjectB)

		want := []PendingLease{{PodName: "pod-a", PodUID: "uid-a", Subject: subjectA}}
		if got := tracker.Drain(wcKey); !reflect.DeepEqual(got, want) {
			t.Errorf("Drain() = %+v, want %+v", got, want)
		}
	})

	t.Run("new_pod_instance_with_same_name_replaces_stale_lease", func(t *testing.T) {
		tracker := NewPendingLeaseTracker(10)
		tracker.Enqueue(wc, newPod("pod-a", "old-uid"), subjectA)
		tracker.Enqueue(wc, newPod("pod-a", "new-uid"), subjectB)

		want := []PendingLease{{PodName: "pod-a", PodUID: "new-uid", Subject: subjectB}}
		if got := tracker.Drain(wcKey); !reflect.DeepEqual(got, want) {
			t.Errorf("Drain() = %+v, want %+v", got, want)
		}
	})

	t.Run("leases_are_isolated_per_workload_class", func(t *testing.T) {
		tracker := NewPendingLeaseTracker(10)
		tracker.Enqueue(wc, newPod("pod-a", "uid-a"), subjectA)
		tracker.Enqueue(otherWC, newPod("pod-b", "uid-b"), subjectB)

		if got := tracker.Drain(wcKey); len(got) != 1 || got[0].PodName != "pod-a" {
			t.Errorf("Drain(wc) = %+v, want only pod-a", got)
		}
		if got := tracker.Drain(otherWCKey); len(got) != 1 || got[0].PodName != "pod-b" {
			t.Errorf("Drain(otherWC) = %+v, want only pod-b", got)
		}
	})

	t.Run("full_channel_still_records_lease", func(t *testing.T) {
		tracker := NewPendingLeaseTracker(1)
		if !tracker.Enqueue(wc, newPod("pod-a", "uid-a"), subjectA) {
			t.Fatal("first Enqueue() = false, want true")
		}
		if tracker.Enqueue(wc, newPod("pod-b", "uid-b"), subjectA) {
			t.Error("Enqueue() with full channel = true, want false")
		}
		if got := tracker.Drain(wcKey); len(got) != 2 {
			t.Errorf("Drain() = %+v, want 2 leases recorded despite full channel", got)
		}
	})

	t.Run("nil_inputs_are_ignored", func(t *testing.T) {
		var nilTracker *PendingLeaseTracker
		if nilTracker.Enqueue(wc, newPod("pod-a", "uid-a"), subjectA) {
			t.Error("nil tracker Enqueue() = true, want false")
		}
		if got := nilTracker.Drain(wcKey); got != nil {
			t.Errorf("nil tracker Drain() = %+v, want nil", got)
		}

		tracker := NewPendingLeaseTracker(0)
		if tracker.Enqueue(nil, newPod("pod-a", "uid-a"), subjectA) || tracker.Enqueue(wc, nil, subjectA) {
			t.Error("Enqueue() with nil WorkloadClass or Pod = true, want false")
		}
		if cap(tracker.events) != DefaultPendingLeaseBufferSize {
			t.Errorf("NewPendingLeaseTracker(0) buffer = %d, want %d", cap(tracker.events), DefaultPendingLeaseBufferSize)
		}
	})

	t.Run("concurrent_enqueue_and_drain", func(t *testing.T) {
		tracker := NewPendingLeaseTracker(1)
		var wg sync.WaitGroup
		for i := range 50 {
			wg.Go(func() {
				tracker.Enqueue(wc, newPod("pod", types.UID(rune('a'+i%26))), subjectA)
				tracker.Drain(wcKey)
			})
		}
		wg.Wait()
	})
}
