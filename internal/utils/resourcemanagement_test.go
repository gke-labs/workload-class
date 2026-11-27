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
	"slices"
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func rmGuardrail(name string, c *workloadsv1.ResourceManagementConstraints) workloadsv1.WorkloadClassGuardrail {
	return workloadsv1.WorkloadClassGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: workloadsv1.WorkloadClassGuardrailSpec{
			Constraints: workloadsv1.Constraints{ResourceManagement: c},
		},
	}
}

func resources(cpu, memory string) corev1.ResourceList {
	list := corev1.ResourceList{}
	if cpu != "" {
		list[corev1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		list[corev1.ResourceMemory] = resource.MustParse(memory)
	}
	return list
}

func cpuUtilizationMetric(utilization int32) autoscalingv2.MetricSpec {
	return autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name:   corev1.ResourceCPU,
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To(utilization)},
		},
	}
}

func externalMetric() autoscalingv2.MetricSpec {
	return autoscalingv2.MetricSpec{
		Type: autoscalingv2.ExternalMetricSourceType,
		External: &autoscalingv2.ExternalMetricSource{
			Metric: autoscalingv2.MetricIdentifier{Name: "queue_depth"},
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.ValueMetricType, Value: ptr.To(resource.MustParse("30"))},
		},
	}
}

// equalQuantities reports whether two resource lists hold the same quantities, ignoring their formatting.
func equalQuantities(a, b corev1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}
	for name, q := range a {
		other, ok := b[name]
		if !ok || q.Cmp(other) != 0 {
			return false
		}
	}
	return true
}

func TestEffectiveResourceManagementConstraints(t *testing.T) {
	testCases := []struct {
		name       string
		guardrails []workloadsv1.WorkloadClassGuardrail
		want       EffectiveResourceManagement
	}{
		{
			name: "no_guardrails",
		},
		{
			name: "guardrails_without_resource_management",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				rmGuardrail("a", nil),
				rmGuardrail("b", &workloadsv1.ResourceManagementConstraints{}),
			},
		},
		{
			name: "single_guardrail_is_copied",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				rmGuardrail("a", &workloadsv1.ResourceManagementConstraints{
					AllowedRightsizingModes:    []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeRecommend},
					AllowedUpdateStrategies:    []workloadsv1.RightsizingUpdateStrategy{workloadsv1.RightsizingUpdateStrategyRecreate},
					MaxReplicas:                ptr.To[int32](50),
					AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileBalanced},
					AllowedMetricTypes:         []autoscalingv2.MetricSourceType{autoscalingv2.ResourceMetricSourceType},
					ContainerBounds:            &workloadsv1.ContainerBounds{Min: resources("10m", ""), Max: resources("", "1Gi")},
				}),
			},
			want: EffectiveResourceManagement{
				AllowedRightsizingModes:    []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeRecommend},
				AllowedUpdateStrategies:    []workloadsv1.RightsizingUpdateStrategy{workloadsv1.RightsizingUpdateStrategyRecreate},
				MaxReplicas:                ptr.To[int32](50),
				AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileBalanced},
				AllowedMetricTypes:         []autoscalingv2.MetricSourceType{autoscalingv2.ResourceMetricSourceType},
				ContainerBounds:            workloadsv1.ContainerBounds{Min: resources("10m", ""), Max: resources("", "1Gi")},
			},
		},
		{
			name: "strictest_value_wins",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				rmGuardrail("lax", &workloadsv1.ResourceManagementConstraints{
					AllowedRightsizingModes: []workloadsv1.RightsizingMode{
						workloadsv1.RightsizingModeOff, workloadsv1.RightsizingModeRecommend, workloadsv1.RightsizingModeAuto,
					},
					MaxReplicas:     ptr.To[int32](100),
					ContainerBounds: &workloadsv1.ContainerBounds{Min: resources("10m", "32Mi"), Max: resources("16", "64Gi")},
				}),
				rmGuardrail("unset", nil),
				rmGuardrail("strict", &workloadsv1.ResourceManagementConstraints{
					AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeAuto, workloadsv1.RightsizingModeRecommend},
					AllowedMetricTypes:      []autoscalingv2.MetricSourceType{autoscalingv2.PodsMetricSourceType},
					MaxReplicas:             ptr.To[int32](20),
					ContainerBounds:         &workloadsv1.ContainerBounds{Min: resources("100m", "16Mi"), Max: resources("32", "8Gi")},
				}),
			},
			want: EffectiveResourceManagement{
				// The intersection keeps the order of the first guardrail that sets the list.
				AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeRecommend, workloadsv1.RightsizingModeAuto},
				AllowedMetricTypes:      []autoscalingv2.MetricSourceType{autoscalingv2.PodsMetricSourceType},
				MaxReplicas:             ptr.To[int32](20),
				ContainerBounds:         workloadsv1.ContainerBounds{Min: resources("100m", "32Mi"), Max: resources("16", "8Gi")},
			},
		},
		{
			name: "disjoint_lists_leave_an_empty_list",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				rmGuardrail("a", &workloadsv1.ResourceManagementConstraints{
					AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileResponsive},
				}),
				rmGuardrail("b", &workloadsv1.ResourceManagementConstraints{
					AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileConservative},
				}),
			},
			want: EffectiveResourceManagement{AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := EffectiveResourceManagementConstraints(tc.guardrails)
			if !equalQuantities(got.ContainerBounds.Min, tc.want.ContainerBounds.Min) || !equalQuantities(got.ContainerBounds.Max, tc.want.ContainerBounds.Max) {
				t.Errorf("ContainerBounds = %v, want %v", got.ContainerBounds, tc.want.ContainerBounds)
			}
			got.ContainerBounds, tc.want.ContainerBounds = workloadsv1.ContainerBounds{}, workloadsv1.ContainerBounds{}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("EffectiveResourceManagementConstraints() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestEffectiveResourceManagementConstraintsDoesNotModifyGuardrails(t *testing.T) {
	modes := []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeRecommend, workloadsv1.RightsizingModeAuto}
	bounds := &workloadsv1.ContainerBounds{Min: resources("10m", "")}
	guardrails := []workloadsv1.WorkloadClassGuardrail{
		rmGuardrail("a", &workloadsv1.ResourceManagementConstraints{AllowedRightsizingModes: modes, ContainerBounds: bounds}),
		rmGuardrail("b", &workloadsv1.ResourceManagementConstraints{
			AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeAuto},
			ContainerBounds:         &workloadsv1.ContainerBounds{Min: resources("1", "")},
		}),
	}
	_ = EffectiveResourceManagementConstraints(guardrails)
	if !slices.Equal(modes, []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeRecommend, workloadsv1.RightsizingModeAuto}) {
		t.Errorf("guardrail allowedRightsizingModes were modified: %v", modes)
	}
	if !equalQuantities(bounds.Min, resources("10m", "")) {
		t.Errorf("guardrail containerBounds.min was modified: %v", bounds.Min)
	}
}

func TestResourceManagementViolations(t *testing.T) {
	newWC := func(rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
		return &workloadsv1.WorkloadClass{Spec: workloadsv1.WorkloadClassSpec{ResourceManagement: rm}}
	}
	autoscaling := func(maxReplicas int32, profile workloadsv1.AutoscalingProfile, metrics ...autoscalingv2.MetricSpec) *workloadsv1.ResourceManagement {
		return &workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
			Replicas: workloadsv1.ReplicaBounds{Max: maxReplicas},
			Metrics:  metrics,
			Profile:  profile,
		}}
	}
	rightsizing := func(r workloadsv1.Rightsizing) *workloadsv1.ResourceManagement {
		return &workloadsv1.ResourceManagement{Rightsizing: &r}
	}
	cpu := cpuUtilizationMetric(60)

	testCases := []struct {
		name      string
		rm        *workloadsv1.ResourceManagement
		effective EffectiveResourceManagement
		want      []string
	}{
		{
			name:      "no_resource_management",
			effective: EffectiveResourceManagement{MaxReplicas: ptr.To[int32](1)},
		},
		{
			name: "no_constraints",
			rm:   autoscaling(1000, workloadsv1.AutoscalingProfileResponsive, cpu),
		},
		{
			name:      "replicas_within_limit",
			rm:        autoscaling(100, "", cpu),
			effective: EffectiveResourceManagement{MaxReplicas: ptr.To[int32](100)},
		},
		{
			name:      "replicas_above_limit",
			rm:        autoscaling(101, "", cpu),
			effective: EffectiveResourceManagement{MaxReplicas: ptr.To[int32](100)},
			want:      []string{"`resourceManagement.autoscaling.replicas.max` must be at most 100 (guardrail limit), got 101"},
		},
		{
			name: "default_profile_is_checked_as_balanced",
			rm:   autoscaling(10, "", cpu),
			effective: EffectiveResourceManagement{
				AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileConservative},
			},
			want: []string{"`resourceManagement.autoscaling.profile` must be one of 'Conservative' (guardrail limit), got 'Balanced'"},
		},
		{
			name: "allowed_profile",
			rm:   autoscaling(10, workloadsv1.AutoscalingProfileConservative, cpu),
			effective: EffectiveResourceManagement{
				AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileBalanced, workloadsv1.AutoscalingProfileConservative},
			},
		},
		{
			name:      "disjoint_profiles_are_a_guardrail_conflict",
			rm:        autoscaling(10, workloadsv1.AutoscalingProfileBalanced, cpu),
			effective: EffectiveResourceManagement{AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{}},
			want: []string{"cluster guardrails restrict `allowedAutoscalingProfiles` to disjoint sets, so no autoscaling profile " +
				"can comply; contact your cluster administrator"},
		},
		{
			name: "disallowed_metric_types_are_listed_once",
			rm:   autoscaling(10, "", cpu, externalMetric(), externalMetric()),
			effective: EffectiveResourceManagement{
				AllowedMetricTypes: []autoscalingv2.MetricSourceType{autoscalingv2.ResourceMetricSourceType, autoscalingv2.PodsMetricSourceType},
			},
			want: []string{"`resourceManagement.autoscaling.metrics` must only use metric types 'Resource', 'Pods' (guardrail limit), got 'External'"},
		},
		{
			name:      "disjoint_metric_types_are_a_guardrail_conflict",
			rm:        autoscaling(10, "", cpu),
			effective: EffectiveResourceManagement{AllowedMetricTypes: []autoscalingv2.MetricSourceType{}},
			want: []string{"cluster guardrails restrict `allowedMetricTypes` to disjoint sets, so no metric type " +
				"can comply; contact your cluster administrator"},
		},
		{
			name: "autoscaling_constraints_do_not_apply_to_rightsizing_only_classes",
			rm:   rightsizing(workloadsv1.Rightsizing{}),
			effective: EffectiveResourceManagement{
				MaxReplicas:                ptr.To[int32](1),
				AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{},
				AllowedMetricTypes:         []autoscalingv2.MetricSourceType{},
			},
		},
		{
			name:      "default_mode_is_checked_as_recommend",
			rm:        rightsizing(workloadsv1.Rightsizing{}),
			effective: EffectiveResourceManagement{AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeOff}},
			want:      []string{"`resourceManagement.rightsizing.mode` must be one of 'Off' (guardrail limit), got 'Recommend'"},
		},
		{
			name: "disallowed_auto_mode",
			rm:   rightsizing(workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto}),
			effective: EffectiveResourceManagement{
				AllowedRightsizingModes: []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeOff, workloadsv1.RightsizingModeRecommend},
			},
			want: []string{"`resourceManagement.rightsizing.mode` must be one of 'Off', 'Recommend' (guardrail limit), got 'Auto'"},
		},
		{
			name: "disjoint_modes_are_a_guardrail_conflict",
			rm:   rightsizing(workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOff}),
			effective: EffectiveResourceManagement{
				AllowedRightsizingModes: []workloadsv1.RightsizingMode{},
			},
			want: []string{"cluster guardrails restrict `allowedRightsizingModes` to disjoint sets, so no rightsizing mode " +
				"can comply; contact your cluster administrator"},
		},
		{
			name: "default_update_strategy_is_checked_as_in_place_preferred_in_auto_mode",
			rm:   rightsizing(workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto}),
			effective: EffectiveResourceManagement{
				AllowedUpdateStrategies: []workloadsv1.RightsizingUpdateStrategy{workloadsv1.RightsizingUpdateStrategyRecreate},
			},
			want: []string{"`resourceManagement.rightsizing.updateStrategy` must be one of 'Recreate' (guardrail limit), got 'InPlacePreferred'"},
		},
		{
			name: "update_strategy_is_not_checked_outside_auto_mode",
			rm: rightsizing(workloadsv1.Rightsizing{
				Mode:           workloadsv1.RightsizingModeOnCreate,
				UpdateStrategy: workloadsv1.RightsizingUpdateStrategyInPlacePreferred,
			}),
			effective: EffectiveResourceManagement{
				AllowedUpdateStrategies: []workloadsv1.RightsizingUpdateStrategy{workloadsv1.RightsizingUpdateStrategyRecreate},
			},
		},
		{
			name: "bounds_inside_guardrail_bounds",
			rm: rightsizing(workloadsv1.Rightsizing{
				Bounds: &workloadsv1.ContainerBounds{Min: resources("100m", "128Mi"), Max: resources("4", "8Gi")},
			}),
			effective: EffectiveResourceManagement{
				ContainerBounds: workloadsv1.ContainerBounds{Min: resources("10m", "32Mi"), Max: resources("16", "64Gi")},
			},
		},
		{
			name: "bounds_outside_guardrail_bounds",
			rm: rightsizing(workloadsv1.Rightsizing{
				Bounds: &workloadsv1.ContainerBounds{Min: resources("5m", "128Gi"), Max: resources("32", "16Mi")},
			}),
			effective: EffectiveResourceManagement{
				ContainerBounds: workloadsv1.ContainerBounds{Min: resources("10m", "32Mi"), Max: resources("16", "64Gi")},
			},
			want: []string{
				"`resourceManagement.rightsizing.bounds.min.cpu` must be at least '10m' (guardrail limit), got '5m'",
				"`resourceManagement.rightsizing.bounds.max.cpu` must be at most '16' (guardrail limit), got '32'",
				"`resourceManagement.rightsizing.bounds.min.memory` must be at most '64Gi' (guardrail limit), got '128Gi'",
				"`resourceManagement.rightsizing.bounds.max.memory` must be at least '32Mi' (guardrail limit), got '16Mi'",
			},
		},
		{
			name: "inverted_guardrail_bounds_are_a_guardrail_conflict",
			rm:   rightsizing(workloadsv1.Rightsizing{}),
			effective: EffectiveResourceManagement{
				ContainerBounds: workloadsv1.ContainerBounds{Min: resources("2", ""), Max: resources("1", "")},
			},
			want: []string{"cluster guardrails set `containerBounds.max` below `containerBounds.min` for 'cpu', so no rightsizing " +
				"bounds can comply; contact your cluster administrator"},
		},
		{
			name: "bounds_are_not_checked_when_rightsizing_is_off",
			rm: rightsizing(workloadsv1.Rightsizing{
				Mode:   workloadsv1.RightsizingModeOff,
				Bounds: &workloadsv1.ContainerBounds{Max: resources("32", "")},
			}),
			effective: EffectiveResourceManagement{
				ContainerBounds: workloadsv1.ContainerBounds{Min: resources("2", ""), Max: resources("1", "")},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResourceManagementViolations(newWC(tc.rm), tc.effective)
			if !slices.Equal(got, tc.want) {
				t.Errorf("ResourceManagementViolations() =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

func TestResourceManagementSpecViolations(t *testing.T) {
	autoscaling := func(metrics ...autoscalingv2.MetricSpec) *workloadsv1.ResourceManagement {
		return &workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
			Replicas: workloadsv1.ReplicaBounds{Max: 5},
			Metrics:  metrics,
		}}
	}
	pods := func(target autoscalingv2.MetricTarget) autoscalingv2.MetricSpec {
		return autoscalingv2.MetricSpec{
			Type:   autoscalingv2.PodsMetricSourceType,
			Pods:   &autoscalingv2.PodsMetricSource{Metric: autoscalingv2.MetricIdentifier{Name: "rps"}, Target: target},
			Object: nil,
		}
	}
	containerResource := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ContainerResourceMetricSourceType,
		ContainerResource: &autoscalingv2.ContainerResourceMetricSource{
			Name:      corev1.ResourceMemory,
			Container: "app",
			Target:    autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType, AverageValue: ptr.To(resource.MustParse("1Gi"))},
		},
	}

	testCases := []struct {
		name string
		rm   *workloadsv1.ResourceManagement
		want []string
	}{
		{
			name: "nil",
		},
		{
			name: "valid_metrics",
			rm: autoscaling(
				cpuUtilizationMetric(60),
				externalMetric(),
				containerResource,
				pods(autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType, AverageValue: ptr.To(resource.MustParse("100"))}),
			),
		},
		{
			name: "empty",
			rm:   &workloadsv1.ResourceManagement{},
			want: []string{"`resourceManagement` must set `autoscaling`, `rightsizing`, or both"},
		},
		{
			name: "min_replicas_above_max",
			rm: &workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](6), Max: 5},
				Metrics:  []autoscalingv2.MetricSpec{cpuUtilizationMetric(60)},
			}},
			want: []string{"`resourceManagement.autoscaling.replicas.min` must not exceed `replicas.max` (5), got 6"},
		},
		{
			name: "no_metrics",
			rm:   autoscaling(),
			want: []string{"`resourceManagement.autoscaling.metrics` must have at least one metric"},
		},
		{
			name: "object_metric",
			rm:   autoscaling(autoscalingv2.MetricSpec{Type: autoscalingv2.ObjectMetricSourceType, Object: &autoscalingv2.ObjectMetricSource{}}),
			want: []string{"`resourceManagement.autoscaling.metrics[0].type` must be one of 'Resource', 'Pods', 'ContainerResource', 'External', got 'Object'"},
		},
		{
			name: "missing_source",
			rm:   autoscaling(autoscalingv2.MetricSpec{Type: autoscalingv2.ResourceMetricSourceType}),
			want: []string{"`resourceManagement.autoscaling.metrics[0].resource` must be set for metric type 'Resource'"},
		},
		{
			name: "extra_source",
			rm: autoscaling(func() autoscalingv2.MetricSpec {
				m := externalMetric()
				m.Pods = &autoscalingv2.PodsMetricSource{}
				return m
			}()),
			want: []string{"`resourceManagement.autoscaling.metrics[0]` must only set `external` for metric type 'External'"},
		},
		{
			name: "target_type_not_allowed_for_metric_type",
			rm:   autoscaling(pods(autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](50)})),
			want: []string{"`resourceManagement.autoscaling.metrics[0].pods.target.type` must be one of 'AverageValue' for metric type 'Pods', got 'Utilization'"},
		},
		{
			name: "missing_target_value",
			rm: autoscaling(
				cpuUtilizationMetric(0),
				pods(autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType}),
			),
			want: []string{
				"`resourceManagement.autoscaling.metrics[0].resource.target.averageUtilization` must be greater than 0 when `type` is 'Utilization'",
				"`resourceManagement.autoscaling.metrics[1].pods.target.averageValue` must be greater than 0 when `type` is 'AverageValue'",
			},
		},
		{
			name: "inverted_bounds",
			rm: &workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{
				Bounds: &workloadsv1.ContainerBounds{Min: resources("", "2Gi"), Max: resources("", "1Gi")},
			}},
			want: []string{"`resourceManagement.rightsizing.bounds.min.memory` must not exceed `bounds.max.memory` ('1Gi'), got '2Gi'"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResourceManagementSpecViolations(tc.rm)
			if !slices.Equal(got, tc.want) {
				t.Errorf("ResourceManagementSpecViolations() =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

func TestResourceManagementConstraintViolations(t *testing.T) {
	testCases := []struct {
		name string
		c    *workloadsv1.ResourceManagementConstraints
		want []string
	}{
		{
			name: "nil",
		},
		{
			name: "valid",
			c: &workloadsv1.ResourceManagementConstraints{
				AllowedRightsizingModes:    []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeOff, workloadsv1.RightsizingModeAuto},
				AllowedUpdateStrategies:    []workloadsv1.RightsizingUpdateStrategy{workloadsv1.RightsizingUpdateStrategyInPlacePreferred},
				MaxReplicas:                ptr.To[int32](10),
				AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileResponsive},
				AllowedMetricTypes:         []autoscalingv2.MetricSourceType{autoscalingv2.ObjectMetricSourceType, autoscalingv2.ExternalMetricSourceType},
				ContainerBounds:            &workloadsv1.ContainerBounds{Min: resources("10m", "32Mi"), Max: resources("10m", "64Gi")},
			},
		},
		{
			name: "invalid_enum_values",
			c: &workloadsv1.ResourceManagementConstraints{
				AllowedRightsizingModes:    []workloadsv1.RightsizingMode{"Always"},
				AllowedUpdateStrategies:    []workloadsv1.RightsizingUpdateStrategy{"Rolling"},
				AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{"Eager"},
				AllowedMetricTypes:         []autoscalingv2.MetricSourceType{"Custom"},
				MaxReplicas:                ptr.To[int32](0),
			},
			want: []string{
				"`resourceManagement.allowedRightsizingModes` must only contain 'Off', 'Recommend', 'OnCreate', 'Auto', got 'Always'",
				"`resourceManagement.allowedUpdateStrategies` must only contain 'InPlacePreferred', 'Recreate', got 'Rolling'",
				"`resourceManagement.allowedAutoscalingProfiles` must only contain 'Responsive', 'Balanced', 'Conservative', got 'Eager'",
				"`resourceManagement.allowedMetricTypes` must only contain 'Object', 'Resource', 'Pods', 'ContainerResource', 'External', got 'Custom'",
				"`resourceManagement.maxReplicas` must be at least 1, got 0",
			},
		},
		{
			name: "invalid_container_bounds",
			c: &workloadsv1.ResourceManagementConstraints{
				ContainerBounds: &workloadsv1.ContainerBounds{
					Min: resources("2", ""),
					Max: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")},
				},
			},
			want: []string{
				"`resourceManagement.containerBounds.max` must only set 'cpu' and 'memory', got 'ephemeral-storage'",
				"`resourceManagement.containerBounds.min.cpu` must not exceed `containerBounds.max.cpu` ('1'), got '2'",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResourceManagementConstraintViolations(tc.c)
			if !slices.Equal(got, tc.want) {
				t.Errorf("ResourceManagementConstraintViolations() =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

func TestResourceManagementDefaults(t *testing.T) {
	if got := AutoscalingProfileOrDefault(""); got != workloadsv1.AutoscalingProfileBalanced {
		t.Errorf("AutoscalingProfileOrDefault(\"\") = %q, want Balanced", got)
	}
	if got := AutoscalingProfileOrDefault(workloadsv1.AutoscalingProfileResponsive); got != workloadsv1.AutoscalingProfileResponsive {
		t.Errorf("AutoscalingProfileOrDefault(Responsive) = %q, want Responsive", got)
	}
	if got := RightsizingModeOrDefault(""); got != workloadsv1.RightsizingModeRecommend {
		t.Errorf("RightsizingModeOrDefault(\"\") = %q, want Recommend", got)
	}
	if got := RightsizingModeOrDefault(workloadsv1.RightsizingModeOff); got != workloadsv1.RightsizingModeOff {
		t.Errorf("RightsizingModeOrDefault(Off) = %q, want Off", got)
	}
	if got := RightsizingUpdateStrategyOrDefault(""); got != workloadsv1.RightsizingUpdateStrategyInPlacePreferred {
		t.Errorf("RightsizingUpdateStrategyOrDefault(\"\") = %q, want InPlacePreferred", got)
	}
	if got := RightsizingUpdateStrategyOrDefault(workloadsv1.RightsizingUpdateStrategyRecreate); got != workloadsv1.RightsizingUpdateStrategyRecreate {
		t.Errorf("RightsizingUpdateStrategyOrDefault(Recreate) = %q, want Recreate", got)
	}
}
