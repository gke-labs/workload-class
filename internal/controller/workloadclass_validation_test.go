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
	"fmt"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func testWorkloadClass(name, namespace string, matchLabels map[string]string) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
		},
	}
}

func TestOverlappingWorkloadClasses(t *testing.T) {
	const ns = "test-namespace"
	nginx := map[string]string{"app": "nginx"}
	wcCurrent := testWorkloadClass("wc-current", ns, nginx)

	invalid := testWorkloadClass("wc-invalid", ns, nil)
	invalid.Spec.PodSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
		{Key: "app", Operator: metav1.LabelSelectorOpIn},
	}

	now := metav1.Now()
	deleting := testWorkloadClass("wc-deleting", ns, nginx)
	deleting.Finalizers = []string{"workloads.gke.io/test"}
	deleting.DeletionTimestamp = &now

	testCases := []struct {
		name      string
		existing  []client.Object
		wantNames []string
	}{
		{
			name:     "no_other_workload_classes",
			existing: []client.Object{wcCurrent},
		},
		{
			name:     "other_disjoint_selector",
			existing: []client.Object{wcCurrent, testWorkloadClass("wc-different", ns, map[string]string{"app": "redis"})},
		},
		{
			name:      "other_same_selector",
			existing:  []client.Object{wcCurrent, testWorkloadClass("wc-same", ns, nginx)},
			wantNames: []string{"wc-same"},
		},
		{
			name:      "other_superset_selector",
			existing:  []client.Object{wcCurrent, testWorkloadClass("wc-superset", ns, map[string]string{"app": "nginx", "tier": "web"})},
			wantNames: []string{"wc-superset"},
		},
		{
			name:     "same_selector_different_namespace",
			existing: []client.Object{wcCurrent, testWorkloadClass("wc-same-other-ns", "another-namespace", nginx)},
		},
		{
			name:     "other_invalid_selector_is_skipped",
			existing: []client.Object{wcCurrent, invalid},
		},
		{
			name:     "other_deleting_is_skipped",
			existing: []client.Object{wcCurrent, deleting},
		},
		{
			name: "namespace_enforces_a_workload_class",
			existing: []client.Object{
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
					Name:   ns,
					Labels: map[string]string{workloadsv1.EnforcedClassLabel: "wc-current"},
				}},
				wcCurrent,
				testWorkloadClass("wc-same", ns, nginx),
			},
		},
		{
			name: "multiple_overlapping_sorted_by_name",
			existing: []client.Object{
				wcCurrent,
				testWorkloadClass("wc-same-3", ns, nginx),
				testWorkloadClass("wc-same-1", ns, nginx),
				testWorkloadClass("wc-same-2", ns, nginx),
			},
			wantNames: []string{"wc-same-1", "wc-same-2", "wc-same-3"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			r := &WorkloadClassReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.existing...).Build(),
				Scheme: scheme,
			}

			got, err := r.overlappingWorkloadClasses(context.Background(), wcCurrent)
			if err != nil {
				t.Fatalf("overlappingWorkloadClasses() returned unexpected error: %v", err)
			}

			gotNames := make([]string, 0, len(got))
			for _, wc := range got {
				gotNames = append(gotNames, wc.Name)
			}
			if !slices.Equal(gotNames, tc.wantNames) {
				t.Errorf("overlappingWorkloadClasses() = %v, want %v", gotNames, tc.wantNames)
			}
		})
	}
}

func testGuardrail(name string, disruption workloadsv1.Disruption) workloadsv1.WorkloadClassGuardrail {
	return workloadsv1.WorkloadClassGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: workloadsv1.WorkloadClassGuardrailSpec{
			Constraints: workloadsv1.Constraints{Disruption: disruption},
		},
	}
}

func TestValidateAgainstGuardrails(t *testing.T) {
	wc := &workloadsv1.WorkloadClass{
		Spec: workloadsv1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 40},
		},
	}

	testCases := []struct {
		name       string
		guardrails []workloadsv1.WorkloadClassGuardrail
		want       []string
	}{
		{
			name: "no_guardrails",
		},
		{
			name: "compliant_with_all_guardrails",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				testGuardrail("a", workloadsv1.Disruption{MaxNonDisruptionDurationDays: 40}),
				testGuardrail("b", workloadsv1.Disruption{MaxNonDisruptionDurationDays: 60}),
			},
		},
		{
			name: "effective_strictest_limits_across_guardrails_without_guardrail_names",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				testGuardrail("lax", workloadsv1.Disruption{
					DaysOfWeek:                   []string{"Friday", "Saturday", "Sunday"},
					MaxNonDisruptionDurationDays: 30,
				}),
				testGuardrail("strict", workloadsv1.Disruption{
					DaysOfWeek:                   []string{"Saturday", "Sunday", "Monday"},
					MaxNonDisruptionDurationDays: 5,
				}),
			},
			want: []string{
				"`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only 'Sunday', 'Saturday'",
				"`maxNonDisruptionDurationDays` must be between 1 and 5, inclusive",
			},
		},
		{
			name: "effective_days_order_is_deterministic_regardless_of_guardrail_order",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				testGuardrail("strict", workloadsv1.Disruption{
					DaysOfWeek:                   []string{"Saturday", "Sunday", "Monday"},
					MaxNonDisruptionDurationDays: 5,
				}),
				testGuardrail("lax", workloadsv1.Disruption{
					DaysOfWeek:                   []string{"Friday", "Saturday", "Sunday"},
					MaxNonDisruptionDurationDays: 30,
				}),
			},
			want: []string{
				"`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only 'Sunday', 'Saturday'",
				"`maxNonDisruptionDurationDays` must be between 1 and 5, inclusive",
			},
		},
		{
			name: "identical_violations_from_several_guardrails_are_reported_once",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				testGuardrail("strict-1", workloadsv1.Disruption{MaxNonDisruptionDurationDays: 5}),
				testGuardrail("strict-2", workloadsv1.Disruption{MaxNonDisruptionDurationDays: 5}),
			},
			want: []string{
				"`maxNonDisruptionDurationDays` must be between 1 and 5, inclusive",
			},
		},
		{
			name: "disjoint_days_of_week_constraints_report_actionable_conflict_message",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				testGuardrail("saturday", workloadsv1.Disruption{
					DaysOfWeek:                   []string{"Saturday"},
					MaxNonDisruptionDurationDays: 60,
				}),
				testGuardrail("sunday", workloadsv1.Disruption{
					DaysOfWeek:                   []string{"Sunday"},
					MaxNonDisruptionDurationDays: 60,
				}),
			},
			want: []string{
				disjointDaysOfWeekMsg,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateAgainstGuardrails(wc, tc.guardrails)
			if len(got) != len(tc.want) {
				t.Fatalf("validateAgainstGuardrails() = %q, want %d violation(s) starting with %q", got, len(tc.want), tc.want)
			}
			for i, want := range tc.want {
				if !strings.HasPrefix(got[i], want) {
					t.Errorf("violation[%d] = %q, want it to start with %q", i, got[i], want)
				}
				for _, g := range tc.guardrails {
					if strings.Contains(got[i], "'"+g.Name+"'") {
						t.Errorf("violation[%d] = %q must not contain guardrail name %q", i, got[i], g.Name)
					}
				}
			}
		})
	}
}

func TestValidatedCondition(t *testing.T) {
	wc := testWorkloadClass("wc", "ns", map[string]string{"app": "nginx"})
	wc.Generation = 3
	wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 10
	invalidSelectorWC := wc.DeepCopy()
	invalidSelectorWC.Spec.PodSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
		{Key: "tier", Operator: metav1.LabelSelectorOpIn},
	}
	overlapping := []workloadsv1.WorkloadClass{*testWorkloadClass("wc-same", "ns", map[string]string{"app": "nginx"})}
	permissive := testGuardrail("permissive", workloadsv1.Disruption{MaxNonDisruptionDurationDays: 30})
	restrictive := testGuardrail("restrictive", workloadsv1.Disruption{MaxNonDisruptionDurationDays: 5})

	testCases := []struct {
		name        string
		wc          *workloadsv1.WorkloadClass
		guardrails  []workloadsv1.WorkloadClassGuardrail
		overlapping []workloadsv1.WorkloadClass
		wantStatus  metav1.ConditionStatus
		wantReason  string
	}{
		{
			name:       "no_guardrails",
			wc:         wc,
			wantStatus: metav1.ConditionTrue,
			wantReason: workloadsv1.ReasonNoGuardrails,
		},
		{
			name:       "guardrails_pass",
			wc:         wc,
			guardrails: []workloadsv1.WorkloadClassGuardrail{permissive},
			wantStatus: metav1.ConditionTrue,
			wantReason: workloadsv1.ReasonValidationPassed,
		},
		{
			name:       "guardrails_fail",
			wc:         wc,
			guardrails: []workloadsv1.WorkloadClassGuardrail{restrictive},
			wantStatus: metav1.ConditionFalse,
			wantReason: workloadsv1.ReasonValidationFailed,
		},
		{
			name:       "invalid_pod_selector",
			wc:         invalidSelectorWC,
			wantStatus: metav1.ConditionFalse,
			wantReason: workloadsv1.ReasonValidationFailed,
		},
		{
			name:        "selector_overlap_only",
			wc:          wc,
			guardrails:  []workloadsv1.WorkloadClassGuardrail{permissive},
			overlapping: overlapping,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonSelectorOverlap,
		},
		{
			name:        "selector_overlap_without_guardrails",
			wc:          wc,
			overlapping: overlapping,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonSelectorOverlap,
		},
		{
			name:        "guardrail_violation_takes_precedence_over_overlap",
			wc:          wc,
			guardrails:  []workloadsv1.WorkloadClassGuardrail{restrictive},
			overlapping: overlapping,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonValidationFailed,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := validatedCondition(context.Background(), tc.wc, &workloadsv1.WorkloadClassGuardrailList{Items: tc.guardrails}, tc.overlapping)
			if got.Type != workloadsv1.ConditionTypeValidated {
				t.Errorf("Type = %q, want %q", got.Type, workloadsv1.ConditionTypeValidated)
			}
			if got.Status != tc.wantStatus || got.Reason != tc.wantReason {
				t.Errorf("got (%s, %s), want (%s, %s); message: %q", got.Status, got.Reason, tc.wantStatus, tc.wantReason, got.Message)
			}
			if got.ObservedGeneration != tc.wc.Generation {
				t.Errorf("ObservedGeneration = %d, want %d", got.ObservedGeneration, tc.wc.Generation)
			}
		})
	}
}

func TestSelectorOverlapMessageCapsItems(t *testing.T) {
	order := []int{7, 2, 5, 1, 6, 3, 4}
	overlapping := make([]workloadsv1.WorkloadClass, 0, len(order))
	for _, i := range order {
		overlapping = append(overlapping, *testWorkloadClass(fmt.Sprintf("wc-%d", i), "ns", nil))
	}

	got := selectorOverlapMessage(overlapping)
	want := "`podSelector` must not select the same Pods as another WorkloadClass in the namespace, " +
		"but it overlaps with the `podSelector` of 'wc-1', 'wc-2', 'wc-3', 'wc-4', 'wc-5', and 2 more"
	if got != want {
		t.Errorf("selectorOverlapMessage() = %q, want %q", got, want)
	}
}

func TestTruncateMessage(t *testing.T) {
	if got := truncateMessage("short", 10); got != "short" {
		t.Errorf("truncateMessage(\"short\", 10) = %q, want \"short\"", got)
	}
	if got := truncateMessage("0123456789abcdef", 10); got != "0123456..." {
		t.Errorf("truncateMessage(\"0123456789abcdef\", 10) = %q, want \"0123456...\"", got)
	}
	// Multi-byte UTF-8 rune ("é" is 2 bytes at indices 6..7) must not be split mid-rune.
	if got := truncateMessage("123456é890", 10); got != "123456..." {
		t.Errorf("truncateMessage(\"123456é890\", 10) = %q, want \"123456...\"", got)
	}
}

func TestProtectsPods(t *testing.T) {
	testCases := []struct {
		reason        string
		hasGuardrails bool
		want          bool
	}{
		{reason: workloadsv1.ReasonValidationPassed, hasGuardrails: true, want: true},
		{reason: workloadsv1.ReasonSelectorOverlap, hasGuardrails: true, want: true},
		{reason: workloadsv1.ReasonSelectorOverlap, hasGuardrails: false, want: false},
		{reason: workloadsv1.ReasonNoGuardrails, hasGuardrails: false, want: false},
		{reason: workloadsv1.ReasonValidationFailed, hasGuardrails: true, want: false},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_hasGuardrails_%t", tc.reason, tc.hasGuardrails), func(t *testing.T) {
			if got := protectsPods(tc.reason, tc.hasGuardrails); got != tc.want {
				t.Errorf("protectsPods(%q, %t) = %t, want %t", tc.reason, tc.hasGuardrails, got, tc.want)
			}
		})
	}
}
