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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestReconcileSafeToEvictAnnotations(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := workloadsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add workloadsv1: %v", err)
	}

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	newWC := func(manage bool) *workloadsv1.WorkloadClass {
		return &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector:      &metav1.LabelSelector{MatchLabels: map[string]string{"app": "db"}},
				DisruptionPolicy: workloadsv1.DisruptionPolicy{ManageSafeToEvictAnnotation: manage},
			},
		}
	}
	newPod := func(name string, anns map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   "default",
				Labels:      map[string]string{"app": "db"},
				Annotations: anns,
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
	}

	t.Run("suspends_when_ready_and_restores_when_not_ready_leaving_unannotated_pods_untouched", func(t *testing.T) {
		wc := newWC(true)
		protectedPod := newPod("db-0", map[string]string{SafeToEvictAnnotation: annotationValueFalse, "keep": "yes"})
		safeTruePod := newPod("db-1", map[string]string{SafeToEvictAnnotation: annotationValueTrue})
		plainPod := newPod("db-2", nil)

		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, wc, protectedPod, safeTruePod, plainPod).Build()
		r := &WorkloadClassReconciler{Client: fc, Scheme: scheme}

		// Window opens (Ready): db-0 suspended, db-1 & db-2 untouched.
		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessReady, true); err != nil {
			t.Fatalf("Ready reconcile error = %v", err)
		}

		got := &corev1.Pod{}
		if err := fc.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "db-0"}, got); err != nil {
			t.Fatalf("get db-0: %v", err)
		}
		if _, exists := got.Annotations[SafeToEvictAnnotation]; exists {
			t.Errorf("expected %s to be removed during Ready, got %v", SafeToEvictAnnotation, got.Annotations)
		}
		if got.Annotations[SafeToEvictSuspendedAnnotation] != annotationValueTrue || got.Annotations["keep"] != "yes" {
			t.Errorf("unexpected annotations on db-0: %v", got.Annotations)
		}

		if err := fc.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "db-1"}, got); err != nil {
			t.Fatalf("get db-1: %v", err)
		}
		if got.Annotations[SafeToEvictAnnotation] != annotationValueTrue || got.Annotations[SafeToEvictSuspendedAnnotation] != "" {
			t.Errorf("db-1 should be untouched, got %v", got.Annotations)
		}

		if err := fc.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "db-2"}, got); err != nil {
			t.Fatalf("get db-2: %v", err)
		}
		if len(got.Annotations) != 0 {
			t.Errorf("db-2 should be untouched, got %v", got.Annotations)
		}

		// Window closes (NotReady): db-0 restored, db-1 & db-2 still untouched.
		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessNotReady, true); err != nil {
			t.Fatalf("NotReady reconcile error = %v", err)
		}
		if err := fc.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "db-0"}, got); err != nil {
			t.Fatalf("get db-0: %v", err)
		}
		if got.Annotations[SafeToEvictAnnotation] != annotationValueFalse {
			t.Errorf("expected %s=false to be restored, got %v", SafeToEvictAnnotation, got.Annotations)
		}
		if _, exists := got.Annotations[SafeToEvictSuspendedAnnotation]; exists {
			t.Errorf("expected %s to be removed on restore, got %v", SafeToEvictSuspendedAnnotation, got.Annotations)
		}
	})

	t.Run("restores_when_option_disabled_or_validation_fails", func(t *testing.T) {
		wc := newWC(false)
		suspendedPod := newPod("db-0", map[string]string{SafeToEvictSuspendedAnnotation: annotationValueTrue})
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, wc, suspendedPod).Build()
		r := &WorkloadClassReconciler{Client: fc, Scheme: scheme}

		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessReady, true); err != nil {
			t.Fatalf("disabled reconcile error = %v", err)
		}
		got := &corev1.Pod{}
		if err := fc.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "db-0"}, got); err != nil {
			t.Fatalf("get db-0: %v", err)
		}
		if got.Annotations[SafeToEvictAnnotation] != annotationValueFalse {
			t.Errorf("expected safe-to-evict=false restored when disabled, got %v", got.Annotations)
		}

		// Also verify Overdue suspends, and invalid restores.
		wc.Spec.DisruptionPolicy.ManageSafeToEvictAnnotation = true
		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessOverdue, true); err != nil {
			t.Fatalf("Overdue reconcile error = %v", err)
		}
		if err := fc.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "db-0"}, got); err != nil {
			t.Fatalf("get db-0: %v", err)
		}
		if got.Annotations[SafeToEvictSuspendedAnnotation] != annotationValueTrue {
			t.Errorf("expected Overdue to suspend safe-to-evict, got %v", got.Annotations)
		}

		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessReady, false); err != nil {
			t.Fatalf("invalid reconcile error = %v", err)
		}
		if err := fc.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "db-0"}, got); err != nil {
			t.Fatalf("get db-0: %v", err)
		}
		if got.Annotations[SafeToEvictAnnotation] != annotationValueFalse {
			t.Errorf("expected invalid WorkloadClass to restore safe-to-evict=false, got %v", got.Annotations)
		}
	})
}
