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
	"errors"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func safeToEvictTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, policyv1.AddToScheme, workloadsv1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatalf("add scheme: %v", err)
		}
	}
	return s
}

func makeSTEWC(name string, manage bool, match map[string]string) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector:      &metav1.LabelSelector{MatchLabels: match},
			DisruptionPolicy: workloadsv1.DisruptionPolicy{ManageSafeToEvictAnnotation: manage},
		},
	}
}

func makeSTEPod(name string, lbls, anns map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: lbls, Annotations: anns},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func mustGetPod(t *testing.T, c client.Client, name string) *corev1.Pod {
	t.Helper()
	p := &corev1.Pod{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: name}, p); err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	return p
}

func assertPodRestored(t *testing.T, c client.Client, name string) {
	t.Helper()
	p := mustGetPod(t, c, name)
	if _, ok := p.Annotations[SafeToEvictSuspendedAnnotation]; ok || p.Annotations[SafeToEvictAnnotation] != annotationValueFalse {
		t.Errorf("%s: want restored safe-to-evict=false without marker, got %v", name, p.Annotations)
	}
}

func TestReconcileSafeToEvictAnnotations(t *testing.T) {
	scheme := safeToEvictTestScheme(t)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	db := map[string]string{"app": "db"}

	t.Run("suspend_restore_and_finalizer", func(t *testing.T) {
		wc := makeSTEWC("wc", true, db)
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, wc,
			makeSTEPod("db-0", db, map[string]string{SafeToEvictAnnotation: annotationValueFalse, "keep": "yes"}),
			makeSTEPod("db-1", db, map[string]string{SafeToEvictAnnotation: "true"}),
			makeSTEPod("db-2", db, nil),
		).Build()
		r := &WorkloadClassReconciler{Client: fc, Scheme: scheme}
		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessReady, true); err != nil {
			t.Fatalf("ready: %v", err)
		}
		if got := mustGetPod(t, fc, "db-0").Annotations; got[SafeToEvictAnnotation] != "" || got[SafeToEvictSuspendedAnnotation] != "wc" || got["keep"] != "yes" {
			t.Errorf("db-0 annotations = %v", got)
		}
		if !controllerutil.ContainsFinalizer(wc, SafeToEvictFinalizer) {
			t.Error("expected finalizer after suspend")
		}
		if mustGetPod(t, fc, "db-1").Annotations[SafeToEvictSuspendedAnnotation] != "" || len(mustGetPod(t, fc, "db-2").Annotations) != 0 {
			t.Error("expected db-1 and db-2 untouched")
		}
		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessNotReady, true); err != nil {
			t.Fatalf("not ready: %v", err)
		}
		assertPodRestored(t, fc, "db-0")
		if controllerutil.ContainsFinalizer(wc, SafeToEvictFinalizer) {
			t.Error("expected finalizer removed after restore")
		}
	})

	t.Run("disabled_overdue_invalid", func(t *testing.T) {
		wc := makeSTEWC("wc", false, db)
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, wc,
			makeSTEPod("db-0", db, map[string]string{SafeToEvictSuspendedAnnotation: "wc"})).Build()
		r := &WorkloadClassReconciler{Client: fc, Scheme: scheme}
		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessReady, true); err != nil {
			t.Fatalf("disabled: %v", err)
		}
		assertPodRestored(t, fc, "db-0")
		wc.Spec.DisruptionPolicy.ManageSafeToEvictAnnotation = true
		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessOverdue, true); err != nil {
			t.Fatalf("overdue: %v", err)
		}
		if mustGetPod(t, fc, "db-0").Annotations[SafeToEvictSuspendedAnnotation] != "wc" {
			t.Error("expected overdue to suspend")
		}
		if err := r.reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessReady, false); err != nil {
			t.Fatalf("invalid: %v", err)
		}
		assertPodRestored(t, fc, "db-0")
	})

	t.Run("selector_narrowed_and_other_owner_untouched", func(t *testing.T) {
		wc := makeSTEWC("wc", true, map[string]string{"app": "cache"})
		wc.Finalizers = []string{SafeToEvictFinalizer}
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, wc,
			makeSTEPod("db-0", db, map[string]string{SafeToEvictSuspendedAnnotation: "wc"}),
			makeSTEPod("db-other", db, map[string]string{SafeToEvictSuspendedAnnotation: "other-wc"}),
		).Build()
		if err := (&WorkloadClassReconciler{Client: fc, Scheme: scheme}).reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessReady, true); err != nil {
			t.Fatalf("narrowed: %v", err)
		}
		assertPodRestored(t, fc, "db-0")
		if mustGetPod(t, fc, "db-other").Annotations[SafeToEvictSuspendedAnnotation] != "other-wc" {
			t.Error("must not touch pod suspended by other-wc")
		}
	})

	t.Run("best_match_takeover_and_enforced_class", func(t *testing.T) {
		wc := makeSTEWC("wc", true, db)
		more := map[string]string{"app": "db", "tier": "primary"}
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, wc, makeSTEWC("wc-spec", false, more),
			makeSTEPod("db-0", more, map[string]string{SafeToEvictSuspendedAnnotation: "wc"})).Build()
		if err := (&WorkloadClassReconciler{Client: fc, Scheme: scheme}).reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessReady, true); err != nil {
			t.Fatalf("takeover: %v", err)
		}
		assertPodRestored(t, fc, "db-0")

		enfNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default", Labels: map[string]string{workloadsv1.EnforcedClassLabel: "wc-enf"}}}
		fc2 := fake.NewClientBuilder().WithScheme(scheme).WithObjects(enfNS, wc, makeSTEWC("wc-enf", false, map[string]string{"x": "y"}),
			makeSTEPod("db-1", db, map[string]string{SafeToEvictSuspendedAnnotation: "wc"})).Build()
		if err := (&WorkloadClassReconciler{Client: fc2, Scheme: scheme}).reconcileSafeToEvictAnnotations(t.Context(), wc, workloadsv1.ReadinessReady, true); err != nil {
			t.Fatalf("enforced: %v", err)
		}
		assertPodRestored(t, fc2, "db-1")
	})

	t.Run("findWorkloadClassesForPod_enqueues_suspending_owner", func(t *testing.T) {
		pod := makeSTEPod("db-0", map[string]string{"app": "other"}, map[string]string{SafeToEvictSuspendedAnnotation: "old-wc"})
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, makeSTEWC("old-wc", true, db), pod).Build()
		reqs := (&WorkloadClassReconciler{Client: fc, Scheme: scheme}).findWorkloadClassesForPod(t.Context(), pod)
		want := types.NamespacedName{Namespace: "default", Name: "old-wc"}
		if !slices.ContainsFunc(reqs, func(rq ctrl.Request) bool { return rq.NamespacedName == want }) {
			t.Errorf("want %v in %v", want, reqs)
		}
	})
}

func TestReconcileSafeToEvictEndToEnd(t *testing.T) {
	scheme := safeToEvictTestScheme(t)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	db := map[string]string{"app": "db"}
	key := func(name string) ctrl.Request {
		return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: name}}
	}

	t.Run("delete_restores_via_finalizer", func(t *testing.T) {
		now := metav1.Now()
		wc := makeSTEWC("wc-del", true, db)
		wc.Finalizers, wc.DeletionTimestamp = []string{SafeToEvictFinalizer}, &now
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, wc,
			makeSTEPod("db-0", map[string]string{"app": "other"}, map[string]string{SafeToEvictSuspendedAnnotation: "wc-del"})).Build()
		if _, err := (&WorkloadClassReconciler{Client: fc, Scheme: scheme}).Reconcile(t.Context(), key("wc-del")); err != nil {
			t.Fatalf("delete: %v", err)
		}
		assertPodRestored(t, fc, "db-0")
		if err := fc.Get(t.Context(), key("wc-del").NamespacedName, wc); err == nil && controllerutil.ContainsFinalizer(wc, SafeToEvictFinalizer) {
			t.Error("expected finalizer removed on delete")
		}
	})

	t.Run("no_guardrails_never_suspends", func(t *testing.T) {
		wc := makeSTEWC("wc", true, db)
		fc := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(wc).WithObjects(ns, wc,
			makeSTEPod("db-0", db, map[string]string{SafeToEvictAnnotation: annotationValueFalse})).Build()
		if _, err := (&WorkloadClassReconciler{Client: fc, Scheme: scheme, Recorder: events.NewFakeRecorder(4)}).Reconcile(t.Context(), key("wc")); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		assertPodRestored(t, fc, "db-0")
	})

	t.Run("pdb_error_does_not_block_restore", func(t *testing.T) {
		failPDB := false
		wc := makeSTEWC("wc", true, db)
		wc.UID, wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = "wc-uid", 10
		pod := makeSTEPod("db-0", db, map[string]string{SafeToEvictAnnotation: annotationValueFalse})
		pod.UID = "pod-uid"
		gr := &workloadsv1.WorkloadClassGuardrail{
			ObjectMeta: metav1.ObjectMeta{Name: "g"},
			Spec:       workloadsv1.WorkloadClassGuardrailSpec{Constraints: workloadsv1.Constraints{Disruption: workloadsv1.Disruption{MaxNonDisruptionDurationDays: 30}}},
		}
		fc := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(wc).WithObjects(ns, gr, wc, pod).
			WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*policyv1.PodDisruptionBudgetList); ok && failPDB {
					return errors.New("injected pdb list error")
				}
				return c.List(ctx, list, opts...)
			}}).Build()
		r := &WorkloadClassReconciler{Client: fc, Scheme: scheme, Recorder: events.NewFakeRecorder(4)}
		if _, err := r.Reconcile(t.Context(), key("wc")); err != nil {
			t.Fatalf("open-window reconcile: %v", err)
		}
		if mustGetPod(t, fc, "db-0").Annotations[SafeToEvictSuspendedAnnotation] != "wc" {
			t.Fatal("expected db-0 suspended")
		}
		if err := fc.Get(t.Context(), key("wc").NamespacedName, wc); err != nil {
			t.Fatalf("get wc: %v", err)
		}
		wc.Spec.DisruptionPolicy.ManageSafeToEvictAnnotation = false
		if err := fc.Update(t.Context(), wc); err != nil {
			t.Fatalf("update wc: %v", err)
		}
		failPDB = true
		if _, err := r.Reconcile(t.Context(), key("wc")); err == nil {
			t.Fatal("expected PDB error")
		}
		assertPodRestored(t, fc, "db-0")
	})
}
