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
	"slices"
	"strings"
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func testResources(cpu, memory string) corev1.ResourceList {
	rl := corev1.ResourceList{}
	if cpu != "" {
		rl[corev1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		rl[corev1.ResourceMemory] = resource.MustParse(memory)
	}
	return rl
}

func testMetric(t autoscalingv2.MetricSourceType) autoscalingv2.MetricSpec {
	return autoscalingv2.MetricSpec{Type: t}
}

func testRMGuardrail(c *workloadsv1.ResourceManagementConstraints) workloadsv1.WorkloadClassGuardrail {
	return workloadsv1.WorkloadClassGuardrail{
		Spec: workloadsv1.WorkloadClassGuardrailSpec{Constraints: workloadsv1.Constraints{ResourceManagement: c}},
	}
}

func testRMClass(rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "ns"},
		Spec:       workloadsv1.WorkloadClassSpec{ResourceManagement: rm},
	}
}

// fullResourceManagement returns a resourceManagement that complies with strictConstraints.
func fullResourceManagement() *workloadsv1.ResourceManagement {
	return &workloadsv1.ResourceManagement{
		InitialResources: &workloadsv1.InitialResources{Requests: testResources("250m", "256Mi")},
		Autoscaling: &workloadsv1.Autoscaling{
			Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](2), Max: 20},
			Metrics:  []autoscalingv2.MetricSpec{testMetric(autoscalingv2.ResourceMetricSourceType)},
		},
		Rightsizing: &workloadsv1.Rightsizing{
			Mode:   workloadsv1.RightsizingModeAuto,
			Bounds: &workloadsv1.ContainerBounds{Min: testResources("100m", "128Mi"), Max: testResources("4", "8Gi")},
		},
	}
}

func strictConstraints() *workloadsv1.ResourceManagementConstraints {
	return &workloadsv1.ResourceManagementConstraints{
		RequireInitialResources:    true,
		AllowedLimitPolicies:       []workloadsv1.LimitPolicy{workloadsv1.LimitPolicyMemoryEqualsRequest, workloadsv1.LimitPolicyEqualsRequest},
		AllowedRightsizingModes:    []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeRecommend, workloadsv1.RightsizingModeAuto},
		AllowedUpdateStrategies:    []workloadsv1.RightsizingUpdateStrategy{workloadsv1.RightsizingUpdateStrategyInPlacePreferred},
		MaxReplicas:                ptr.To[int32](100),
		AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileBalanced, workloadsv1.AutoscalingProfileConservative},
		AllowedMetricTypes:         []autoscalingv2.MetricSourceType{autoscalingv2.ResourceMetricSourceType, autoscalingv2.PodsMetricSourceType},
		ContainerBounds:            &workloadsv1.ContainerBounds{Min: testResources("10m", "32Mi"), Max: testResources("16", "64Gi")},
	}
}

func TestEffectiveResourceManagementConstraints(t *testing.T) {
	t.Run("no_guardrails_allow_everything", func(t *testing.T) {
		e := EffectiveResourceManagementConstraints(nil)
		if e.RequireInitialResources || e.MaxReplicas != nil || len(e.ContainerBounds.Min)+len(e.ContainerBounds.Max) != 0 {
			t.Errorf("expected no constraints, got %+v", e)
		}
		if !e.RightsizingModes.Allows(workloadsv1.RightsizingModeAuto) || e.RightsizingModes.Values() != nil {
			t.Errorf("expected every rightsizing mode to be allowed")
		}
		if got := e.Conflicts(); len(got) != 0 {
			t.Errorf("expected no conflicts, got %v", got)
		}
	})

	t.Run("guardrails_without_resource_management_are_ignored", func(t *testing.T) {
		e := EffectiveResourceManagementConstraints([]workloadsv1.WorkloadClassGuardrail{testRMGuardrail(nil), testRMGuardrail(nil)})
		if e.RequireInitialResources || !e.LimitPolicies.Allows(workloadsv1.LimitPolicyNone) {
			t.Errorf("expected no constraints, got %+v", e)
		}
	})

	t.Run("strictest_value_wins", func(t *testing.T) {
		e := EffectiveResourceManagementConstraints([]workloadsv1.WorkloadClassGuardrail{
			testRMGuardrail(&workloadsv1.ResourceManagementConstraints{
				AllowedRightsizingModes: []workloadsv1.RightsizingMode{
					workloadsv1.RightsizingModeOff, workloadsv1.RightsizingModeRecommend, workloadsv1.RightsizingModeAuto,
				},
				MaxReplicas:     ptr.To[int32](100),
				ContainerBounds: &workloadsv1.ContainerBounds{Min: testResources("10m", "64Mi"), Max: testResources("16", "64Gi")},
			}),
			testRMGuardrail(&workloadsv1.ResourceManagementConstraints{
				RequireInitialResources: true,
				AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeRecommend, workloadsv1.RightsizingModeOff},
				AllowedLimitPolicies:    []workloadsv1.LimitPolicy{workloadsv1.LimitPolicyEqualsRequest},
				MaxReplicas:             ptr.To[int32](50),
				ContainerBounds:         &workloadsv1.ContainerBounds{Min: testResources("50m", "32Mi"), Max: testResources("8", "")},
			}),
			testRMGuardrail(&workloadsv1.ResourceManagementConstraints{MaxReplicas: ptr.To[int32](75)}),
		})

		if !e.RequireInitialResources {
			t.Errorf("expected requireInitialResources to be true if any guardrail sets it")
		}
		if got, want := e.RightsizingModes.Values(), []string{"Off", "Recommend"}; !slices.Equal(got, want) {
			t.Errorf("rightsizing modes = %v, want %v", got, want)
		}
		if got, want := e.LimitPolicies.Values(), []string{"EqualsRequest"}; !slices.Equal(got, want) {
			t.Errorf("limit policies = %v, want %v", got, want)
		}
		if e.UpdateStrategies.Values() != nil {
			t.Errorf("expected update strategies to be unrestricted, got %v", e.UpdateStrategies.Values())
		}
		if e.MaxReplicas == nil || *e.MaxReplicas != 50 {
			t.Errorf("maxReplicas = %v, want 50", e.MaxReplicas)
		}
		for _, tc := range []struct {
			name string
			got  resource.Quantity
			want string
		}{
			{"min cpu (highest minimum)", e.ContainerBounds.Min[corev1.ResourceCPU], "50m"},
			{"min memory (highest minimum)", e.ContainerBounds.Min[corev1.ResourceMemory], "64Mi"},
			{"max cpu (lowest maximum)", e.ContainerBounds.Max[corev1.ResourceCPU], "8"},
			{"max memory (only one guardrail sets it)", e.ContainerBounds.Max[corev1.ResourceMemory], "64Gi"},
		} {
			if tc.got.Cmp(resource.MustParse(tc.want)) != 0 {
				t.Errorf("%s = %s, want %s", tc.name, tc.got.String(), tc.want)
			}
		}
		if got := e.Conflicts(); len(got) != 0 {
			t.Errorf("expected no conflicts, got %v", got)
		}
	})

	t.Run("unsatisfiable_guardrails_are_reported_as_conflicts", func(t *testing.T) {
		e := EffectiveResourceManagementConstraints([]workloadsv1.WorkloadClassGuardrail{
			testRMGuardrail(&workloadsv1.ResourceManagementConstraints{
				AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeAuto},
				AllowedMetricTypes:      []autoscalingv2.MetricSourceType{autoscalingv2.ResourceMetricSourceType},
				ContainerBounds:         &workloadsv1.ContainerBounds{Min: testResources("2", "")},
			}),
			testRMGuardrail(&workloadsv1.ResourceManagementConstraints{
				AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeOff},
				AllowedMetricTypes:      []autoscalingv2.MetricSourceType{autoscalingv2.PodsMetricSourceType},
				ContainerBounds:         &workloadsv1.ContainerBounds{Max: testResources("1", "")},
			}),
		})
		got := e.Conflicts()
		for _, want := range []string{"`allowedRightsizingModes`", "`allowedMetricTypes`", "minimum (2) above the maximum (1) for cpu"} {
			if !slices.ContainsFunc(got, func(s string) bool { return strings.Contains(s, want) }) {
				t.Errorf("expected a conflict mentioning %q, got %v", want, got)
			}
		}
		if len(got) != 3 {
			t.Errorf("expected 3 conflicts, got %d: %v", len(got), got)
		}
	})
}

func TestResourceManagementViolations(t *testing.T) {
	strict := EffectiveResourceManagementConstraints([]workloadsv1.WorkloadClassGuardrail{testRMGuardrail(strictConstraints())})

	testCases := []struct {
		name string
		// mutate changes a compliant resourceManagement; returning nil removes it from the WorkloadClass.
		mutate    func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement
		effective *EffectiveResourceManagement
		want      []string
	}{
		{
			name:   "compliant",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement { return rm },
		},
		{
			name:      "no_constraints",
			mutate:    func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement { return rm },
			effective: &EffectiveResourceManagement{},
		},
		{
			name:   "resource_management_unset_but_initial_resources_required",
			mutate: func(*workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement { return nil },
			want:   []string{"`resourceManagement.initialResources` must be specified (guardrail limit)"},
		},
		{
			name:      "resource_management_unset_and_not_required",
			mutate:    func(*workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement { return nil },
			effective: &EffectiveResourceManagement{},
		},
		{
			name: "initial_resources_unset_but_required",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.InitialResources = nil
				return rm
			},
			want: []string{"`resourceManagement.initialResources` must be specified (guardrail limit)"},
		},
		{
			name: "limit_policy_not_allowed",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.InitialResources.LimitPolicy = workloadsv1.LimitPolicyNone
				return rm
			},
			want: []string{"`resourceManagement.initialResources.limitPolicy` must be one of 'EqualsRequest', 'MemoryEqualsRequest' (guardrail limit), got 'None'"},
		},
		{
			name: "initial_requests_outside_container_bounds",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.InitialResources.Requests = testResources("5m", "128Gi")
				return rm
			},
			want: []string{
				"`resourceManagement.initialResources.requests` cpu must be at least 10m (guardrail limit), got 5m",
				"`resourceManagement.initialResources.requests` memory must be at most 64Gi (guardrail limit), got 128Gi",
			},
		},
		{
			name: "replicas_max_above_guardrail",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.Autoscaling.Replicas.Max = 200
				return rm
			},
			want: []string{"`resourceManagement.autoscaling.replicas.max` must be at most 100 (guardrail limit), got 200"},
		},
		{
			name: "profile_not_allowed",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.Autoscaling.Profile = workloadsv1.AutoscalingProfileResponsive
				return rm
			},
			want: []string{"`resourceManagement.autoscaling.profile` must be one of 'Balanced', 'Conservative' (guardrail limit), got 'Responsive'"},
		},
		{
			name: "metric_types_not_allowed_are_reported_once_each",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.Autoscaling.Metrics = []autoscalingv2.MetricSpec{
					testMetric(autoscalingv2.ExternalMetricSourceType),
					testMetric(autoscalingv2.ResourceMetricSourceType),
					testMetric(autoscalingv2.ExternalMetricSourceType),
					testMetric(autoscalingv2.ContainerResourceMetricSourceType),
				}
				return rm
			},
			want: []string{"`resourceManagement.autoscaling.metrics` must only use the metric types 'Pods', 'Resource' (guardrail limit), got 'ContainerResource', 'External'"},
		},
		{
			name: "rightsizing_mode_not_allowed",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.Rightsizing.Mode = workloadsv1.RightsizingModeOnCreate
				return rm
			},
			want: []string{"`resourceManagement.rightsizing.mode` must be one of 'Auto', 'Recommend' (guardrail limit), got 'OnCreate'"},
		},
		{
			name: "update_strategy_not_allowed_with_auto",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.Rightsizing.UpdateStrategy = workloadsv1.RightsizingUpdateStrategyRecreate
				return rm
			},
			want: []string{"`resourceManagement.rightsizing.updateStrategy` must be one of 'InPlacePreferred' (guardrail limit), got 'Recreate'"},
		},
		{
			name: "update_strategy_is_not_checked_outside_of_auto",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.Rightsizing.Mode = workloadsv1.RightsizingModeRecommend
				rm.Rightsizing.UpdateStrategy = workloadsv1.RightsizingUpdateStrategyRecreate
				return rm
			},
		},
		{
			name: "rightsizing_bounds_outside_container_bounds",
			mutate: func(rm *workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				rm.Rightsizing.Bounds = &workloadsv1.ContainerBounds{Min: testResources("5m", "128Mi"), Max: testResources("32", "8Gi")}
				return rm
			},
			want: []string{
				"`resourceManagement.rightsizing.bounds.min` cpu must be at least 10m (guardrail limit), got 5m",
				"`resourceManagement.rightsizing.bounds.max` cpu must be at most 16 (guardrail limit), got 32",
			},
		},
		{
			name: "defaults_are_applied_before_checking",
			mutate: func(*workloadsv1.ResourceManagement) *workloadsv1.ResourceManagement {
				// limitPolicy, profile, mode and updateStrategy are unset, as when a WorkloadClass is read
				// before the API server applied defaults.
				return &workloadsv1.ResourceManagement{
					InitialResources: &workloadsv1.InitialResources{Requests: testResources("250m", "256Mi")},
					Autoscaling: &workloadsv1.Autoscaling{
						Replicas: workloadsv1.ReplicaBounds{Max: 3},
						Metrics:  []autoscalingv2.MetricSpec{testMetric(autoscalingv2.PodsMetricSourceType)},
					},
					Rightsizing: &workloadsv1.Rightsizing{},
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e := strict
			if tc.effective != nil {
				e = *tc.effective
			}
			got := ResourceManagementViolations(testRMClass(tc.mutate(fullResourceManagement())), e)
			if !slices.Equal(got, tc.want) && (len(got) != 0 || len(tc.want) != 0) {
				t.Errorf("violations mismatch\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}

	t.Run("conflicting_guardrails_are_reported_instead_of_per_value_violations", func(t *testing.T) {
		e := EffectiveResourceManagementConstraints([]workloadsv1.WorkloadClassGuardrail{
			testRMGuardrail(&workloadsv1.ResourceManagementConstraints{AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeAuto}}),
			testRMGuardrail(&workloadsv1.ResourceManagementConstraints{AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeOff}}),
		})
		got := ResourceManagementViolations(testRMClass(fullResourceManagement()), e)
		if len(got) != 1 || !strings.Contains(got[0], "disjoint sets") {
			t.Errorf("expected a single conflict violation, got %q", got)
		}
		if got := ResourceManagementViolations(testRMClass(nil), e); len(got) != 0 {
			t.Errorf("expected no violations for a WorkloadClass without resourceManagement, got %q", got)
		}
	})
}

func TestResourceManagementSpecViolations(t *testing.T) {
	testCases := []struct {
		name   string
		mutate func(rm *workloadsv1.ResourceManagement)
		want   []string
	}{
		{name: "valid", mutate: func(*workloadsv1.ResourceManagement) {}},
		{
			name: "initial_requests_missing_memory",
			mutate: func(rm *workloadsv1.ResourceManagement) {
				rm.InitialResources.Requests = testResources("250m", "")
			},
			want: []string{"`resourceManagement.initialResources.requests` must set memory"},
		},
		{
			name: "initial_requests_for_another_resource",
			mutate: func(rm *workloadsv1.ResourceManagement) {
				rm.InitialResources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse("1Gi")
				rm.InitialResources.Requests["nvidia.com/gpu"] = resource.MustParse("1")
			},
			want: []string{"`resourceManagement.initialResources.requests` may only set cpu and memory, got 'ephemeral-storage', 'nvidia.com/gpu'"},
		},
		{
			name: "initial_requests_not_positive",
			mutate: func(rm *workloadsv1.ResourceManagement) {
				rm.InitialResources.Requests = testResources("0", "-1Mi")
				rm.Rightsizing = nil
			},
			want: []string{
				"`resourceManagement.initialResources.requests` cpu must be greater than 0, got 0",
				"`resourceManagement.initialResources.requests` memory must be greater than 0, got -1Mi",
			},
		},
		{
			name: "initial_requests_outside_rightsizing_bounds",
			mutate: func(rm *workloadsv1.ResourceManagement) {
				rm.InitialResources.Requests = testResources("50m", "16Gi")
			},
			want: []string{
				"`resourceManagement.initialResources.requests` cpu must be at least 100m (`resourceManagement.rightsizing.bounds`), got 50m",
				"`resourceManagement.initialResources.requests` memory must be at most 8Gi (`resourceManagement.rightsizing.bounds`), got 16Gi",
			},
		},
		{
			name: "replicas_min_above_max",
			mutate: func(rm *workloadsv1.ResourceManagement) {
				rm.Autoscaling.Replicas = workloadsv1.ReplicaBounds{Min: ptr.To[int32](5), Max: 2}
			},
			want: []string{"`resourceManagement.autoscaling.replicas.min` must not exceed `replicas.max` (2), got 5"},
		},
		{
			name: "no_metrics",
			mutate: func(rm *workloadsv1.ResourceManagement) {
				rm.Autoscaling.Metrics = nil
			},
			want: []string{"`resourceManagement.autoscaling.metrics` must have at least 1 item"},
		},
		{
			name: "object_metric",
			mutate: func(rm *workloadsv1.ResourceManagement) {
				rm.Autoscaling.Metrics = append(rm.Autoscaling.Metrics,
					testMetric(autoscalingv2.ObjectMetricSourceType), testMetric(autoscalingv2.ObjectMetricSourceType))
			},
			want: []string{"`resourceManagement.autoscaling.metrics` must not use 'Object' metrics, because they name one specific object"},
		},
		{
			name: "bounds_min_above_max",
			mutate: func(rm *workloadsv1.ResourceManagement) {
				rm.InitialResources = nil
				rm.Rightsizing.Bounds = &workloadsv1.ContainerBounds{Min: testResources("2", "128Mi"), Max: testResources("1", "8Gi")}
			},
			want: []string{"`resourceManagement.rightsizing.bounds.min` cpu must not exceed `resourceManagement.rightsizing.bounds.max` (1), got 2"},
		},
		{
			name: "bounds_for_another_resource_or_negative",
			mutate: func(rm *workloadsv1.ResourceManagement) {
				rm.InitialResources = nil
				rm.Rightsizing.Bounds = &workloadsv1.ContainerBounds{
					Min: testResources("-1", ""),
					Max: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"), corev1.ResourceMemory: resource.MustParse("0")},
				}
			},
			want: []string{
				"`resourceManagement.rightsizing.bounds.min` cpu must not be negative, got -1",
				"`resourceManagement.rightsizing.bounds.max` may only set cpu and memory, got 'ephemeral-storage'",
				"`resourceManagement.rightsizing.bounds.max` memory must be greater than 0, got 0",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rm := fullResourceManagement()
			tc.mutate(rm)
			got := ResourceManagementSpecViolations(rm)
			if !slices.Equal(got, tc.want) && (len(got) != 0 || len(tc.want) != 0) {
				t.Errorf("violations mismatch\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}

	if got := ResourceManagementSpecViolations(nil); got != nil {
		t.Errorf("expected no violations for an unset resourceManagement, got %q", got)
	}
}

func TestResourceManagementConstraintsViolations(t *testing.T) {
	if got := ResourceManagementConstraintsViolations(nil); got != nil {
		t.Errorf("expected no violations for unset constraints, got %q", got)
	}
	if got := ResourceManagementConstraintsViolations(strictConstraints()); len(got) != 0 {
		t.Errorf("expected no violations, got %q", got)
	}

	got := ResourceManagementConstraintsViolations(&workloadsv1.ResourceManagementConstraints{
		ContainerBounds: &workloadsv1.ContainerBounds{Min: testResources("", "2Gi"), Max: testResources("", "1Gi")},
	})
	want := []string{"`constraints.resourceManagement.containerBounds.min` memory must not exceed `constraints.resourceManagement.containerBounds.max` (1Gi), got 2Gi"}
	if !slices.Equal(got, want) {
		t.Errorf("violations mismatch\n got: %q\nwant: %q", got, want)
	}
}

func TestEffectiveDefaults(t *testing.T) {
	if got := EffectiveLimitPolicy(&workloadsv1.InitialResources{}); got != workloadsv1.LimitPolicyMemoryEqualsRequest {
		t.Errorf("limit policy default = %q", got)
	}
	if got := EffectiveLimitPolicy(&workloadsv1.InitialResources{LimitPolicy: workloadsv1.LimitPolicyNone}); got != workloadsv1.LimitPolicyNone {
		t.Errorf("limit policy = %q", got)
	}
	if got := EffectiveAutoscalingProfile(&workloadsv1.Autoscaling{}); got != workloadsv1.AutoscalingProfileBalanced {
		t.Errorf("profile default = %q", got)
	}
	if got := EffectiveMinReplicas(&workloadsv1.Autoscaling{}); got != 1 {
		t.Errorf("min replicas default = %d", got)
	}
	if got := EffectiveMinReplicas(&workloadsv1.Autoscaling{Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](3)}}); got != 3 {
		t.Errorf("min replicas = %d", got)
	}
	if got := EffectiveRightsizingMode(nil); got != workloadsv1.RightsizingModeOff {
		t.Errorf("rightsizing mode without rightsizing = %q", got)
	}
	if got := EffectiveRightsizingMode(&workloadsv1.Rightsizing{}); got != workloadsv1.RightsizingModeRecommend {
		t.Errorf("rightsizing mode default = %q", got)
	}
	if got := EffectiveUpdateStrategy(&workloadsv1.Rightsizing{}); got != workloadsv1.RightsizingUpdateStrategyInPlacePreferred {
		t.Errorf("update strategy default = %q", got)
	}
}
