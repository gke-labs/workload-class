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

package v1

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/metrics"
)

const rmTestGeneration = 2

func rmCondition(status metav1.ConditionStatus, reason string, observedGeneration int64) metav1.Condition {
	return metav1.Condition{
		Type:               workloadsv1.ConditionTypeResourceManagementValidated,
		Status:             status,
		Reason:             reason,
		ObservedGeneration: observedGeneration,
	}
}

func rmValidated() metav1.Condition {
	return rmCondition(metav1.ConditionTrue, workloadsv1.ReasonValidationPassed, rmTestGeneration)
}

// rmTestClass returns a WorkloadClass that governs the Pods of newIncomingPod and sets initial resources.
func rmTestClass(conds ...metav1.Condition) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: testWLCName, Namespace: testNamespace, Generation: rmTestGeneration},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "batch"}},
			ResourceManagement: &workloadsv1.ResourceManagement{
				InitialResources: &workloadsv1.InitialResources{Requests: rmTestResources("250m", "256Mi")},
			},
		},
		Status: workloadsv1.WorkloadClassStatus{Conditions: conds},
	}
}

func rmTestDefaulter(t *testing.T, objs ...client.Object) *PodDefaulter {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(rmTestScheme(t)).WithObjects(objs...).Build()
	return &PodDefaulter{Client: c, EnableInitialResources: true}
}

func appliedCount(kind string) float64 {
	return testutil.ToFloat64(metrics.InitialResourcesApplied.WithLabelValues(kind))
}

func skippedCount(reason string) float64 {
	return testutil.ToFloat64(metrics.InitialResourcesSkipped.WithLabelValues(reason))
}

func expectInitialResources(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	got := pod.Spec.Containers[0].Resources
	want := corev1.ResourceRequirements{
		Requests: rmTestResources("250m", "256Mi"),
		Limits:   corev1.ResourceList{corev1.ResourceMemory: rmTestResources("250m", "256Mi")[corev1.ResourceMemory]},
	}
	if !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("container resources = %+v, want %+v", got, want)
	}
	if got, want := pod.Annotations[workloadsv1.InitialResourcesAnnotation], "cpu-request,memory-request,memory-limit"; got != want {
		t.Errorf("annotation %s = %q, want %q", workloadsv1.InitialResourcesAnnotation, got, want)
	}
}

func expectNoInitialResources(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if r := pod.Spec.Containers[0].Resources; len(r.Requests)+len(r.Limits) != 0 {
		t.Errorf("expected the container resources to be left alone, got %+v", r)
	}
	if v, ok := pod.Annotations[workloadsv1.InitialResourcesAnnotation]; ok {
		t.Errorf("expected no %s annotation, got %q", workloadsv1.InitialResourcesAnnotation, v)
	}
}

func TestPodDefaulterAppliesInitialResources(t *testing.T) {
	testCases := []struct {
		name string
		cond metav1.Condition
	}{
		{name: "validated_for_the_current_generation", cond: rmValidated()},
		{name: "validated_without_guardrails", cond: rmCondition(metav1.ConditionTrue, workloadsv1.ReasonNoGuardrails, rmTestGeneration)},
		{
			// An overlapping PodSelector is resolved per Pod, so the WorkloadClass still applies.
			name: "only_flagged_for_selector_overlap",
			cond: rmCondition(metav1.ConditionFalse, workloadsv1.ReasonSelectorOverlap, rmTestGeneration),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			before := appliedCount("memory-limit")
			pod := newIncomingPod()
			if err := rmTestDefaulter(t, rmTestClass(tc.cond)).Default(context.Background(), pod); err != nil {
				t.Fatalf("Default failed: %v", err)
			}
			expectInitialResources(t, pod)
			if got := appliedCount("memory-limit") - before; got != 1 {
				t.Errorf("workloadclass_initial_resources_applied_total{resource=memory-limit} increased by %v, want 1", got)
			}
			// A WorkloadClass without a capacityStrategy does not change the Pod's placement.
			if pod.Spec.Affinity != nil || len(pod.Spec.NodeSelector)+len(pod.Spec.Tolerations) != 0 {
				t.Errorf("expected the Pod placement to be left alone, got %+v", pod.Spec)
			}
		})
	}
}

func TestPodDefaulterSkipsInitialResourcesWhenNotValidated(t *testing.T) {
	testCases := []struct {
		name  string
		conds []metav1.Condition
	}{
		{name: "condition_not_set"},
		{name: "validation_failed", conds: []metav1.Condition{rmCondition(metav1.ConditionFalse, workloadsv1.ReasonValidationFailed, rmTestGeneration)}},
		{name: "condition_unknown", conds: []metav1.Condition{rmCondition(metav1.ConditionUnknown, "", rmTestGeneration)}},
		{name: "observed_for_an_older_generation", conds: []metav1.Condition{rmCondition(metav1.ConditionTrue, workloadsv1.ReasonValidationPassed, rmTestGeneration-1)}},
		{
			// Only the resource management pillar gates initial resources.
			name: "only_other_pillars_validated",
			conds: []metav1.Condition{
				{Type: workloadsv1.ConditionTypeValidated, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonValidationPassed, ObservedGeneration: rmTestGeneration},
				{Type: workloadsv1.ConditionTypeDisruptionValidated, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonValidationPassed, ObservedGeneration: rmTestGeneration},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			before := skippedCount(metrics.InitialResourcesSkipClassNotValidated)
			pod := newIncomingPod()
			if err := rmTestDefaulter(t, rmTestClass(tc.conds...)).Default(context.Background(), pod); err != nil {
				t.Fatalf("Default failed: %v", err)
			}
			expectNoInitialResources(t, pod)
			if got := skippedCount(metrics.InitialResourcesSkipClassNotValidated) - before; got != 1 {
				t.Errorf("workloadclass_initial_resources_skipped_total{reason=class-not-validated} increased by %v, want 1", got)
			}
		})
	}
}

func TestPodDefaulterInitialResourcesDisabled(t *testing.T) {
	skippedBefore := skippedCount(metrics.InitialResourcesSkipClassNotValidated)
	d := rmTestDefaulter(t, rmTestClass(rmValidated()))
	d.EnableInitialResources = false

	pod := newIncomingPod()
	if err := d.Default(context.Background(), pod); err != nil {
		t.Fatalf("Default failed: %v", err)
	}
	expectNoInitialResources(t, pod)
	if got := skippedCount(metrics.InitialResourcesSkipClassNotValidated) - skippedBefore; got != 0 {
		t.Errorf("expected no skip to be recorded when initial resources are disabled, got %v", got)
	}
}

func TestPodDefaulterInitialResourcesNeverOverrideOwnerValues(t *testing.T) {
	pod := newIncomingPod()
	owner := corev1.ResourceRequirements{Requests: rmTestResources("2", "4Gi"), Limits: rmTestResources("4", "8Gi")}
	pod.Spec.Containers[0].Resources = *owner.DeepCopy()

	if err := rmTestDefaulter(t, rmTestClass(rmValidated())).Default(context.Background(), pod); err != nil {
		t.Fatalf("Default failed: %v", err)
	}
	if got := pod.Spec.Containers[0].Resources; !equality.Semantic.DeepEqual(got, owner) {
		t.Errorf("container resources = %+v, want the owner's values %+v", got, owner)
	}
	if v, ok := pod.Annotations[workloadsv1.InitialResourcesAnnotation]; ok {
		t.Errorf("expected no annotation when nothing was filled in, got %q", v)
	}
}

func TestPodDefaulterRecordsSkippedPodLevelResources(t *testing.T) {
	before := skippedCount("pod-level-resources")
	pod := newIncomingPod()
	pod.Spec.Resources = &corev1.ResourceRequirements{Requests: rmTestResources("1", "1Gi")}

	if err := rmTestDefaulter(t, rmTestClass(rmValidated())).Default(context.Background(), pod); err != nil {
		t.Fatalf("Default failed: %v", err)
	}
	if r := pod.Spec.Containers[0].Resources; len(r.Requests)+len(r.Limits) != 0 {
		t.Errorf("expected the container resources to be left alone, got %+v", r)
	}
	if got, want := pod.Annotations[workloadsv1.InitialResourcesAnnotation], "skipped: pod-level-resources"; got != want {
		t.Errorf("annotation = %q, want %q", got, want)
	}
	if got := skippedCount("pod-level-resources") - before; got != 1 {
		t.Errorf("workloadclass_initial_resources_skipped_total{reason=pod-level-resources} increased by %v, want 1", got)
	}
}

// Initial resources and placement are independent pillars of the same WorkloadClass.
func TestPodDefaulterPillarsAreIndependent(t *testing.T) {
	onDemand := &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand}
	placementValidated := metav1.Condition{
		Type: workloadsv1.ConditionTypePlacementValidated, Status: metav1.ConditionTrue,
		Reason: workloadsv1.ReasonValidationPassed, ObservedGeneration: rmTestGeneration,
	}
	placementFailed := placementValidated
	placementFailed.Status = metav1.ConditionFalse
	placementFailed.Reason = workloadsv1.ReasonValidationFailed

	placed := func(pod *corev1.Pod) bool { return pod.Spec.Affinity != nil }

	t.Run("both_applied", func(t *testing.T) {
		wlc := rmTestClass(rmValidated(), placementValidated)
		wlc.Spec.CapacityStrategy = onDemand
		pod := newIncomingPod()
		if err := rmTestDefaulter(t, wlc).Default(context.Background(), pod); err != nil {
			t.Fatalf("Default failed: %v", err)
		}
		expectInitialResources(t, pod)
		if !placed(pod) {
			t.Errorf("expected On-Demand placement to be applied")
		}
	})

	t.Run("placement_applied_when_resource_management_is_invalid", func(t *testing.T) {
		wlc := rmTestClass(rmCondition(metav1.ConditionFalse, workloadsv1.ReasonValidationFailed, rmTestGeneration), placementValidated)
		wlc.Spec.CapacityStrategy = onDemand
		pod := newIncomingPod()
		if err := rmTestDefaulter(t, wlc).Default(context.Background(), pod); err != nil {
			t.Fatalf("Default failed: %v", err)
		}
		expectNoInitialResources(t, pod)
		if !placed(pod) {
			t.Errorf("expected On-Demand placement to be applied")
		}
	})

	t.Run("placement_applied_without_initial_resources", func(t *testing.T) {
		wlc := rmTestClass(placementValidated)
		wlc.Spec.ResourceManagement = nil
		wlc.Spec.CapacityStrategy = onDemand
		pod := newIncomingPod()
		if err := rmTestDefaulter(t, wlc).Default(context.Background(), pod); err != nil {
			t.Fatalf("Default failed: %v", err)
		}
		expectNoInitialResources(t, pod)
		if !placed(pod) {
			t.Errorf("expected On-Demand placement to be applied")
		}
	})

	t.Run("initial_resources_applied_when_placement_is_invalid", func(t *testing.T) {
		wlc := rmTestClass(rmValidated(), placementFailed)
		wlc.Spec.CapacityStrategy = onDemand
		pod := newIncomingPod()
		if err := rmTestDefaulter(t, wlc).Default(context.Background(), pod); err != nil {
			t.Fatalf("Default failed: %v", err)
		}
		expectInitialResources(t, pod)
		if placed(pod) {
			t.Errorf("expected the Pod placement to be left alone")
		}
	})
}

func TestPodDefaulterFailsOpenWhenTheWorkloadClassCannotBeResolved(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(rmTestScheme(t)).WithObjects(rmTestClass(rmValidated())).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("the API server is unavailable")
			},
		}).Build()
	d := &PodDefaulter{Client: c, EnableInitialResources: true}

	pod := newIncomingPod()
	if err := d.Default(context.Background(), pod); err != nil {
		t.Fatalf("expected the Pod to be admitted, got %v", err)
	}
	expectNoInitialResources(t, pod)
}

func TestPodDefaulterLeavesUngovernedPodsAlone(t *testing.T) {
	pod := newIncomingPod()
	pod.Labels = map[string]string{"app": "other"}
	if err := rmTestDefaulter(t, rmTestClass(rmValidated())).Default(context.Background(), pod); err != nil {
		t.Fatalf("Default failed: %v", err)
	}
	expectNoInitialResources(t, pod)
}
