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

func TestBuildHPA(t *testing.T) {
	workload := WorkloadRef{Kind: WorkloadKindDeployment, Name: "web", Namespace: "shop", UID: "uid-web"}
	newWC := func(rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
		return &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "web-services", Namespace: "shop"},
			Spec:       workloadsv1.WorkloadClassSpec{ResourceManagement: rm},
		}
	}

	t.Run("no_autoscaling", func(t *testing.T) {
		for name, wc := range map[string]*workloadsv1.WorkloadClass{
			"nil_workload_class":      nil,
			"no_resource_management":  newWC(nil),
			"rightsizing_only":        newWC(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{}}),
			"empty_resourcemanagment": newWC(&workloadsv1.ResourceManagement{}),
		} {
			if got := BuildHPA(wc, workload); got != nil {
				t.Errorf("%s: BuildHPA() = %+v, want nil", name, got)
			}
		}
	})

	t.Run("all_fields", func(t *testing.T) {
		metrics := []autoscalingv2.MetricSpec{cpuUtilizationMetric(60), externalMetric()}
		wc := newWC(&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
			Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](2), Max: 20},
			Metrics:  metrics,
			Profile:  workloadsv1.AutoscalingProfileConservative,
		}})

		got := BuildHPA(wc, workload)
		want := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "wc-deployment-web",
				Namespace: "shop",
				Labels: map[string]string{
					workloadsv1.ManagedByLabel:     workloadsv1.ManagedByLabelValue,
					workloadsv1.WorkloadClassLabel: "web-services",
				},
			},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"},
				MinReplicas:    ptr.To[int32](2),
				MaxReplicas:    20,
				Metrics:        []autoscalingv2.MetricSpec{cpuUtilizationMetric(60), externalMetric()},
				Behavior:       HPABehaviorForProfile(workloadsv1.AutoscalingProfileConservative),
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("BuildHPA() =\n%+v\nwant\n%+v", got, want)
		}

		// The metrics are copied, so changing the HorizontalPodAutoscaler does not change the WorkloadClass.
		got.Spec.Metrics[0].Resource.Target.AverageUtilization = ptr.To[int32](99)
		if *metrics[0].Resource.Target.AverageUtilization != 60 {
			t.Errorf("BuildHPA() shares metrics with the WorkloadClass")
		}
	})

	t.Run("defaults", func(t *testing.T) {
		wc := newWC(&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
			Replicas: workloadsv1.ReplicaBounds{Max: 5},
			Metrics:  []autoscalingv2.MetricSpec{cpuUtilizationMetric(70)},
		}})
		got := BuildHPA(wc, WorkloadRef{Kind: WorkloadKindStatefulSet, Name: "db", Namespace: "shop"})
		if got.Name != "wc-statefulset-db" {
			t.Errorf("Name = %q, want wc-statefulset-db", got.Name)
		}
		if got.Spec.ScaleTargetRef.Kind != "StatefulSet" {
			t.Errorf("ScaleTargetRef.Kind = %q, want StatefulSet", got.Spec.ScaleTargetRef.Kind)
		}
		if got.Spec.MinReplicas == nil || *got.Spec.MinReplicas != 1 {
			t.Errorf("MinReplicas = %v, want 1", got.Spec.MinReplicas)
		}
		if !reflect.DeepEqual(got.Spec.Behavior, HPABehaviorForProfile(workloadsv1.AutoscalingProfileBalanced)) {
			t.Errorf("Behavior = %+v, want the Balanced profile", got.Spec.Behavior)
		}
	})
}

func TestHPABehaviorForProfile(t *testing.T) {
	type rules struct {
		window   int32
		policies []autoscalingv2.HPAScalingPolicy
	}
	pods := func(value, period int32) autoscalingv2.HPAScalingPolicy {
		return autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PodsScalingPolicy, Value: value, PeriodSeconds: period}
	}
	percent := func(value, period int32) autoscalingv2.HPAScalingPolicy {
		return autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PercentScalingPolicy, Value: value, PeriodSeconds: period}
	}
	// Balanced matches the upstream HorizontalPodAutoscaler defaults for autoscaling/v2.
	balancedUp := rules{window: 0, policies: []autoscalingv2.HPAScalingPolicy{pods(4, 15), percent(100, 15)}}

	testCases := []struct {
		profile       workloadsv1.AutoscalingProfile
		up, down      rules
		wantDifferent bool
	}{
		{profile: workloadsv1.AutoscalingProfileResponsive, up: balancedUp, down: rules{60, []autoscalingv2.HPAScalingPolicy{percent(100, 15)}}},
		{profile: workloadsv1.AutoscalingProfileBalanced, up: balancedUp, down: rules{300, []autoscalingv2.HPAScalingPolicy{percent(100, 15)}}},
		{profile: "", up: balancedUp, down: rules{300, []autoscalingv2.HPAScalingPolicy{percent(100, 15)}}},
		{
			profile: workloadsv1.AutoscalingProfileConservative,
			up:      rules{60, []autoscalingv2.HPAScalingPolicy{percent(50, 60)}},
			down:    rules{600, []autoscalingv2.HPAScalingPolicy{percent(10, 60)}},
		},
	}
	for _, tc := range testCases {
		t.Run(string(tc.profile), func(t *testing.T) {
			got := HPABehaviorForProfile(tc.profile)
			for direction, check := range map[string]struct {
				got  *autoscalingv2.HPAScalingRules
				want rules
			}{"scaleUp": {got.ScaleUp, tc.up}, "scaleDown": {got.ScaleDown, tc.down}} {
				if check.got == nil {
					t.Fatalf("%s = nil", direction)
				}
				if check.got.StabilizationWindowSeconds == nil || *check.got.StabilizationWindowSeconds != check.want.window {
					t.Errorf("%s.stabilizationWindowSeconds = %v, want %d", direction, check.got.StabilizationWindowSeconds, check.want.window)
				}
				if check.got.SelectPolicy == nil || *check.got.SelectPolicy != autoscalingv2.MaxChangePolicySelect {
					t.Errorf("%s.selectPolicy = %v, want Max", direction, check.got.SelectPolicy)
				}
				if !slices.Equal(check.got.Policies, check.want.policies) {
					t.Errorf("%s.policies = %+v, want %+v", direction, check.got.Policies, check.want.policies)
				}
			}
		})
	}

	// Each call returns a new object, so a caller can't change the behavior of another HorizontalPodAutoscaler.
	a, b := HPABehaviorForProfile(workloadsv1.AutoscalingProfileBalanced), HPABehaviorForProfile(workloadsv1.AutoscalingProfileBalanced)
	*a.ScaleUp.StabilizationWindowSeconds = 42
	if *b.ScaleUp.StabilizationWindowSeconds != 0 {
		t.Errorf("HPABehaviorForProfile() returned shared state")
	}
}

func TestUtilizationTargetedResources(t *testing.T) {
	memoryUtilization := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ContainerResourceMetricSourceType,
		ContainerResource: &autoscalingv2.ContainerResourceMetricSource{
			Name:      corev1.ResourceMemory,
			Container: "app",
			Target:    autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](80)},
		},
	}
	cpuAverageValue := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name:   corev1.ResourceCPU,
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType, AverageValue: ptr.To(resource.MustParse("500m"))},
		},
	}

	testCases := []struct {
		name    string
		metrics []autoscalingv2.MetricSpec
		want    []corev1.ResourceName
	}{
		{name: "no_metrics"},
		{name: "external_metric_only", metrics: []autoscalingv2.MetricSpec{externalMetric()}},
		{name: "average_value_target_is_not_utilization", metrics: []autoscalingv2.MetricSpec{cpuAverageValue}},
		{name: "resource_utilization", metrics: []autoscalingv2.MetricSpec{cpuUtilizationMetric(60)}, want: []corev1.ResourceName{corev1.ResourceCPU}},
		{
			name:    "container_resource_utilization_sorted_and_deduplicated",
			metrics: []autoscalingv2.MetricSpec{memoryUtilization, cpuUtilizationMetric(60), externalMetric(), cpuUtilizationMetric(80), memoryUtilization},
			want:    []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
		},
		{
			name:    "missing_source_is_ignored",
			metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := UtilizationTargetedResources(tc.metrics); !slices.Equal(got, tc.want) {
				t.Errorf("UtilizationTargetedResources() = %v, want %v", got, tc.want)
			}
		})
	}
}
