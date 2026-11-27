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
	"strings"
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func rmResources(cpu, memory string) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	}
}

func rmGuardrails(constraints ...*workloadsv1.ResourceManagementConstraints) *workloadsv1.WorkloadClassGuardrailList {
	list := &workloadsv1.WorkloadClassGuardrailList{}
	for _, c := range constraints {
		g := testGuardrail("guardrail", workloadsv1.Disruption{})
		g.Spec.Constraints.ResourceManagement = c
		list.Items = append(list.Items, g)
	}
	return list
}

func rmWorkloadClass(rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
	wc := testWorkloadClass("web", "test-namespace", map[string]string{"tier": "web"})
	wc.Generation = 3
	wc.Spec.ResourceManagement = rm
	return wc
}

func rmInitialResources() *workloadsv1.ResourceManagement {
	return &workloadsv1.ResourceManagement{
		InitialResources: &workloadsv1.InitialResources{Requests: rmResources("250m", "256Mi")},
	}
}

func TestResourceManagementValidatedCondition(t *testing.T) {
	required := &workloadsv1.ResourceManagementConstraints{RequireInitialResources: true}
	maxReplicas := int32(10)

	invalidSelector := rmWorkloadClass(rmInitialResources())
	invalidSelector.Spec.PodSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
		{Key: "app", Operator: metav1.LabelSelectorOpIn},
	}

	testCases := []struct {
		name        string
		wc          *workloadsv1.WorkloadClass
		guardrails  *workloadsv1.WorkloadClassGuardrailList
		overlapping []workloadsv1.WorkloadClass
		wantNil     bool
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string
	}{
		{
			name:       "unset_without_guardrails",
			wc:         rmWorkloadClass(nil),
			guardrails: rmGuardrails(),
			wantNil:    true,
		},
		{
			name:       "unset_and_not_required",
			wc:         rmWorkloadClass(nil),
			guardrails: rmGuardrails(nil, &workloadsv1.ResourceManagementConstraints{MaxReplicas: &maxReplicas}),
			wantNil:    true,
		},
		{
			name:        "unset_but_initial_resources_required",
			wc:          rmWorkloadClass(nil),
			guardrails:  rmGuardrails(required),
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonValidationFailed,
			wantMessage: "`resourceManagement.initialResources` must be specified (guardrail limit)",
		},
		{
			name:        "set_without_guardrails",
			wc:          rmWorkloadClass(rmInitialResources()),
			guardrails:  rmGuardrails(),
			wantStatus:  metav1.ConditionTrue,
			wantReason:  workloadsv1.ReasonNoGuardrails,
			wantMessage: noGuardrailsMsg,
		},
		{
			name:        "compliant",
			wc:          rmWorkloadClass(rmInitialResources()),
			guardrails:  rmGuardrails(required),
			wantStatus:  metav1.ConditionTrue,
			wantReason:  workloadsv1.ReasonValidationPassed,
			wantMessage: validationPassedMsg,
		},
		{
			name: "guardrail_violation",
			wc: rmWorkloadClass(&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Max: 50},
				Metrics:  []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType}},
			}}),
			guardrails:  rmGuardrails(&workloadsv1.ResourceManagementConstraints{MaxReplicas: &maxReplicas}),
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonValidationFailed,
			wantMessage: "`resourceManagement.autoscaling.replicas.max` must be at most 10 (guardrail limit), got 50",
		},
		{
			name: "spec_violation_without_guardrails",
			wc: rmWorkloadClass(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{
				Bounds: &workloadsv1.ContainerBounds{Min: rmResources("2", "1Gi"), Max: rmResources("1", "2Gi")},
			}}),
			guardrails:  rmGuardrails(),
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonValidationFailed,
			wantMessage: "`resourceManagement.rightsizing.bounds.min` cpu must not exceed",
		},
		{
			name:        "selector_overlap_only",
			wc:          rmWorkloadClass(rmInitialResources()),
			guardrails:  rmGuardrails(required),
			overlapping: []workloadsv1.WorkloadClass{*testWorkloadClass("other", "test-namespace", map[string]string{"tier": "web"})},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonSelectorOverlap,
			wantMessage: "'other'",
		},
		{
			name:        "invalid_pod_selector",
			wc:          invalidSelector,
			guardrails:  rmGuardrails(required),
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonValidationFailed,
			wantMessage: "`podSelector` must be a valid label selector",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := resourceManagementValidatedCondition(tc.wc, tc.guardrails, tc.overlapping)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("expected no condition, got %+v", *got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected a condition, got nil")
			}
			if got.Type != workloadsv1.ConditionTypeResourceManagementValidated {
				t.Errorf("type = %q", got.Type)
			}
			if got.Status != tc.wantStatus || got.Reason != tc.wantReason {
				t.Errorf("status/reason = %s/%s, want %s/%s (message: %s)", got.Status, got.Reason, tc.wantStatus, tc.wantReason, got.Message)
			}
			if !strings.Contains(got.Message, tc.wantMessage) {
				t.Errorf("message = %q, want it to contain %q", got.Message, tc.wantMessage)
			}
			if got.ObservedGeneration != tc.wc.Generation {
				t.Errorf("observedGeneration = %d, want %d", got.ObservedGeneration, tc.wc.Generation)
			}
		})
	}
}

// A resource management violation fails the overall Validated rollup but never the disruption pillar, so
// Per-Pod PDB protection is not dropped.
func TestResourceManagementViolationDoesNotAffectDisruptionValidation(t *testing.T) {
	wc := rmWorkloadClass(nil)
	wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 7
	guardrails := rmGuardrails(&workloadsv1.ResourceManagementConstraints{RequireInitialResources: true})
	guardrails.Items[0].Spec.Constraints.Disruption.MaxNonDisruptionDurationDays = 30

	disruption := disruptionValidatedCondition(wc, guardrails, nil)
	if disruption.Status != metav1.ConditionTrue || disruption.Reason != workloadsv1.ReasonValidationPassed {
		t.Errorf("DisruptionValidated = %s/%s (%s), want True/ValidationPassed", disruption.Status, disruption.Reason, disruption.Message)
	}

	overall := validatedCondition(context.Background(), wc, guardrails, nil)
	if overall.Status != metav1.ConditionFalse || overall.Reason != workloadsv1.ReasonValidationFailed {
		t.Errorf("Validated = %s/%s, want False/ValidationFailed", overall.Status, overall.Reason)
	}
	if !strings.Contains(overall.Message, "`resourceManagement.initialResources` must be specified") {
		t.Errorf("Validated message = %q, want it to report the resource management violation", overall.Message)
	}
}

func TestReconcileSetsAndRemovesResourceManagementCondition(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	wc := rmWorkloadClass(rmInitialResources())
	wc.Generation = 1
	guardrail := rmGuardrails(&workloadsv1.ResourceManagementConstraints{
		ContainerBounds: &workloadsv1.ContainerBounds{Max: rmResources("100m", "1Gi")},
	}).Items[0]

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, &guardrail).WithStatusSubresource(wc).Build()
	r := &WorkloadClassReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(wc)}

	reconcile := func() *workloadsv1.WorkloadClass {
		t.Helper()
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		got := &workloadsv1.WorkloadClass{}
		if err := c.Get(ctx, req.NamespacedName, got); err != nil {
			t.Fatalf("failed to get WorkloadClass: %v", err)
		}
		return got
	}

	// The initial cpu request (250m) is above the guardrail maximum (100m).
	got := reconcile()
	cond := meta.FindStatusCondition(got.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != workloadsv1.ReasonValidationFailed {
		t.Fatalf("ResourceManagementValidated = %+v, want False/ValidationFailed", cond)
	}
	if !strings.Contains(cond.Message, "cpu must be at most 100m (guardrail limit), got 250m") {
		t.Errorf("unexpected message: %q", cond.Message)
	}
	if overall := meta.FindStatusCondition(got.Status.Conditions, workloadsv1.ConditionTypeValidated); overall == nil || overall.Status != metav1.ConditionFalse {
		t.Errorf("Validated = %+v, want False", overall)
	}

	// Fixing the request makes the pillar pass.
	got.Spec.ResourceManagement.InitialResources.Requests = rmResources("100m", "256Mi")
	if err := c.Update(ctx, got); err != nil {
		t.Fatalf("failed to update WorkloadClass: %v", err)
	}
	got = reconcile()
	if !meta.IsStatusConditionTrue(got.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated) {
		t.Errorf("expected ResourceManagementValidated to be True, got %+v", got.Status.Conditions)
	}

	// Removing resourceManagement removes the condition.
	got.Spec.ResourceManagement = nil
	if err := c.Update(ctx, got); err != nil {
		t.Fatalf("failed to update WorkloadClass: %v", err)
	}
	got = reconcile()
	if cond := meta.FindStatusCondition(got.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated); cond != nil {
		t.Errorf("expected the condition to be removed, got %+v", *cond)
	}
}

func TestWarnUtilizationWithoutRequests(t *testing.T) {
	utilization := func(name corev1.ResourceName) autoscalingv2.MetricSpec {
		return autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name:   name,
				Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType},
			},
		}
	}
	autoscaling := func(metrics ...autoscalingv2.MetricSpec) *workloadsv1.ResourceManagement {
		return &workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
			Replicas: workloadsv1.ReplicaBounds{Max: 5},
			Metrics:  metrics,
		}}
	}
	recorder := events.NewFakeRecorder(10)
	r := &WorkloadClassReconciler{Recorder: recorder}
	recorded := func() []string {
		var got []string
		for {
			select {
			case ev := <-recorder.Events:
				got = append(got, ev)
			default:
				return got
			}
		}
	}

	wc := rmWorkloadClass(autoscaling(utilization(corev1.ResourceCPU), utilization(corev1.ResourceMemory)))
	r.warnUtilizationWithoutRequests(wc)
	got := recorded()
	if len(got) != 1 || !strings.Contains(got[0], reasonUtilizationWithoutRequests) || !strings.Contains(got[0], "utilization of cpu and memory") {
		t.Fatalf("expected one UtilizationWithoutRequests warning for cpu and memory, got %q", got)
	}
	if !strings.HasPrefix(got[0], corev1.EventTypeWarning) {
		t.Errorf("expected a Warning event, got %q", got[0])
	}

	// The warning is recorded once per generation.
	r.warnUtilizationWithoutRequests(wc)
	if got := recorded(); len(got) != 0 {
		t.Errorf("expected no second warning for the same generation, got %q", got)
	}
	wc.Generation++
	r.warnUtilizationWithoutRequests(wc)
	if got := recorded(); len(got) != 1 {
		t.Errorf("expected a warning for the new generation, got %q", got)
	}

	// No warning when requests are filled in, when nothing scales on utilization, or without autoscaling.
	withInitial := rmWorkloadClass(autoscaling(utilization(corev1.ResourceCPU)))
	withInitial.Name = "with-initial-resources"
	withInitial.Spec.ResourceManagement.InitialResources = rmInitialResources().InitialResources
	podsMetric := rmWorkloadClass(autoscaling(autoscalingv2.MetricSpec{Type: autoscalingv2.PodsMetricSourceType}))
	podsMetric.Name = "pods-metric"
	noAutoscaling := rmWorkloadClass(rmInitialResources())
	noAutoscaling.Name = "no-autoscaling"
	for _, quiet := range []*workloadsv1.WorkloadClass{withInitial, podsMetric, noAutoscaling, rmWorkloadClass(nil)} {
		r.warnUtilizationWithoutRequests(quiet)
		if got := recorded(); len(got) != 0 {
			t.Errorf("WorkloadClass %q: expected no warning, got %q", quiet.Name, got)
		}
	}

	// A reconciler without a recorder never panics.
	(&WorkloadClassReconciler{}).warnUtilizationWithoutRequests(rmWorkloadClass(autoscaling(utilization(corev1.ResourceCPU))))
}
