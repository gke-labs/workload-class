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

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// BuildHPA returns the HorizontalPodAutoscaler that the WorkloadClass autoscaling settings describe for a workload,
// or nil if the WorkloadClass has no autoscaling. The caller sets the workload as the controller owner.
//
//   - spec.scaleTargetRef references the workload (apps/v1).
//   - spec.minReplicas and spec.maxReplicas come from replicas.min (default 1) and replicas.max.
//   - spec.metrics is a copy of metrics.
//   - spec.behavior is the fixed behavior of the profile (see HPABehaviorForProfile).
func BuildHPA(wc *workloadsv1.WorkloadClass, workload WorkloadRef) *autoscalingv2.HorizontalPodAutoscaler {
	if wc == nil || wc.Spec.ResourceManagement == nil || wc.Spec.ResourceManagement.Autoscaling == nil {
		return nil
	}
	a := wc.Spec.ResourceManagement.Autoscaling

	minReplicas := int32(1)
	if a.Replicas.Min != nil {
		minReplicas = *a.Replicas.Min
	}
	metrics := make([]autoscalingv2.MetricSpec, len(a.Metrics))
	for i := range a.Metrics {
		a.Metrics[i].DeepCopyInto(&metrics[i])
	}

	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ManagedAutoscalerName(workload.Kind, workload.Name),
			Namespace: workload.Namespace,
			Labels:    ManagedAutoscalerLabels(wc.Name),
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: WorkloadAPIVersion,
				Kind:       workload.Kind,
				Name:       workload.Name,
			},
			MinReplicas: &minReplicas,
			MaxReplicas: a.Replicas.Max,
			Metrics:     metrics,
			Behavior:    HPABehaviorForProfile(AutoscalingProfileOrDefault(a.Profile)),
		},
	}
}

// HPABehaviorForProfile returns the HorizontalPodAutoscaler scaling behavior of an autoscaling profile:
//
//	Profile             Scale up                                         Scale down
//	Responsive          No stabilization; up to 4 Pods or 100% per 15s   60s stabilization; up to 100% per 15s
//	Balanced (default)  No stabilization; up to 4 Pods or 100% per 15s   300s stabilization; up to 100% per 15s
//	Conservative        60s stabilization; up to 50% per 60s             600s stabilization; up to 10% per 60s
//
// When several policies apply, the one that allows the larger change wins. Balanced is the upstream
// HorizontalPodAutoscaler default, so it changes nothing for workloads that don't pick a profile. Every field is set
// explicitly, so the API server's defaulting doesn't change the stored object and the controller never sees a
// difference to correct.
func HPABehaviorForProfile(profile workloadsv1.AutoscalingProfile) *autoscalingv2.HorizontalPodAutoscalerBehavior {
	switch profile {
	case workloadsv1.AutoscalingProfileResponsive:
		return &autoscalingv2.HorizontalPodAutoscalerBehavior{
			ScaleUp:   scalingRules(0, podsPolicy(4, 15), percentPolicy(100, 15)),
			ScaleDown: scalingRules(60, percentPolicy(100, 15)),
		}
	case workloadsv1.AutoscalingProfileConservative:
		return &autoscalingv2.HorizontalPodAutoscalerBehavior{
			ScaleUp:   scalingRules(60, percentPolicy(50, 60)),
			ScaleDown: scalingRules(600, percentPolicy(10, 60)),
		}
	default:
		return &autoscalingv2.HorizontalPodAutoscalerBehavior{
			ScaleUp:   scalingRules(0, podsPolicy(4, 15), percentPolicy(100, 15)),
			ScaleDown: scalingRules(300, percentPolicy(100, 15)),
		}
	}
}

func scalingRules(stabilizationWindowSeconds int32, policies ...autoscalingv2.HPAScalingPolicy) *autoscalingv2.HPAScalingRules {
	selectPolicy := autoscalingv2.MaxChangePolicySelect
	return &autoscalingv2.HPAScalingRules{
		StabilizationWindowSeconds: &stabilizationWindowSeconds,
		SelectPolicy:               &selectPolicy,
		Policies:                   policies,
	}
}

func podsPolicy(pods, periodSeconds int32) autoscalingv2.HPAScalingPolicy {
	return autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PodsScalingPolicy, Value: pods, PeriodSeconds: periodSeconds}
}

func percentPolicy(percent, periodSeconds int32) autoscalingv2.HPAScalingPolicy {
	return autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PercentScalingPolicy, Value: percent, PeriodSeconds: periodSeconds}
}

// UtilizationTargetedResources returns the container resources whose utilization the metrics target, i.e. the
// resources of Resource and ContainerResource metrics with target.type Utilization, in a stable order. Autoscaling
// on the utilization of a resource measures usage against its request, so rightsizing that resource would fight
// the autoscaler.
func UtilizationTargetedResources(metrics []autoscalingv2.MetricSpec) []corev1.ResourceName {
	var names []corev1.ResourceName
	for _, m := range metrics {
		var name corev1.ResourceName
		var target autoscalingv2.MetricTarget
		switch {
		case m.Type == autoscalingv2.ResourceMetricSourceType && m.Resource != nil:
			name, target = m.Resource.Name, m.Resource.Target
		case m.Type == autoscalingv2.ContainerResourceMetricSourceType && m.ContainerResource != nil:
			name, target = m.ContainerResource.Name, m.ContainerResource.Target
		default:
			continue
		}
		if target.Type == autoscalingv2.UtilizationMetricType && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}
