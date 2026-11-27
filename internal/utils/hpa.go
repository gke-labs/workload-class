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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// BuildHPA returns the HorizontalPodAutoscaler that the autoscaling of the WorkloadClass asks for on the
// workload, or nil if the WorkloadClass does not set autoscaling.
//
// The result holds the name, namespace, labels and spec of the managed HPA. Ownership is set by the caller.
//
//   - scaleTargetRef is the workload.
//   - minReplicas and maxReplicas are the replica bounds (replicas.min defaults to 1).
//   - metrics are copied as written.
//   - behavior is derived from the profile (see AutoscalingBehavior).
func BuildHPA(wc *workloadsv1.WorkloadClass, w WorkloadRef) *autoscalingv2.HorizontalPodAutoscaler {
	if wc == nil || wc.Spec.ResourceManagement == nil || wc.Spec.ResourceManagement.Autoscaling == nil {
		return nil
	}
	a := wc.Spec.ResourceManagement.Autoscaling

	metrics := make([]autoscalingv2.MetricSpec, len(a.Metrics))
	for i := range a.Metrics {
		a.Metrics[i].DeepCopyInto(&metrics[i])
	}

	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ManagedAutoscalerName(w.Kind, w.Name),
			Namespace: w.Namespace,
			Labels:    ManagedAutoscalerLabels(wc),
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: WorkloadAPIVersion,
				Kind:       w.Kind,
				Name:       w.Name,
			},
			MinReplicas: ptr.To(EffectiveMinReplicas(a)),
			MaxReplicas: a.Replicas.Max,
			Metrics:     metrics,
			Behavior:    AutoscalingBehavior(EffectiveAutoscalingProfile(a)),
		},
	}
}

// AutoscalingBehavior returns the HPA scaling behavior of an autoscaling profile.
//
//	Profile       Scale up                                      Scale down
//	Responsive    no window, 100% or 4 Pods (max) per 15s       60s window, 100% per 15s
//	Balanced      no window, 100% or 4 Pods (max) per 15s       300s window, 100% per 15s
//	Conservative  60s window, 50% per 60s                       600s window, 10% per 60s
//
// Balanced spells out the upstream HorizontalPodAutoscaler defaults, so it scales exactly like an HPA
// without a behavior. Every field that the API server defaults is set explicitly, so that the stored HPA
// compares equal to the desired one. Unknown profiles fall back to Balanced.
func AutoscalingBehavior(profile workloadsv1.AutoscalingProfile) *autoscalingv2.HorizontalPodAutoscalerBehavior {
	fastUp := scalingRules(0,
		autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15},
		autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PodsScalingPolicy, Value: 4, PeriodSeconds: 15},
	)
	switch profile {
	case workloadsv1.AutoscalingProfileResponsive:
		return &autoscalingv2.HorizontalPodAutoscalerBehavior{
			ScaleUp: fastUp,
			ScaleDown: scalingRules(60,
				autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}),
		}
	case workloadsv1.AutoscalingProfileConservative:
		return &autoscalingv2.HorizontalPodAutoscalerBehavior{
			ScaleUp: scalingRules(60,
				autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PercentScalingPolicy, Value: 50, PeriodSeconds: 60}),
			ScaleDown: scalingRules(600,
				autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PercentScalingPolicy, Value: 10, PeriodSeconds: 60}),
		}
	default:
		return &autoscalingv2.HorizontalPodAutoscalerBehavior{
			ScaleUp: fastUp,
			ScaleDown: scalingRules(300,
				autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}),
		}
	}
}

func scalingRules(stabilizationWindowSeconds int32, policies ...autoscalingv2.HPAScalingPolicy) *autoscalingv2.HPAScalingRules {
	return &autoscalingv2.HPAScalingRules{
		StabilizationWindowSeconds: ptr.To(stabilizationWindowSeconds),
		SelectPolicy:               ptr.To(autoscalingv2.MaxChangePolicySelect),
		Policies:                   policies,
	}
}

// UtilizationTargetedResources returns the resources whose utilization the metrics scale on: the resources
// of the Resource and ContainerResource metrics with a Utilization target, sorted and without duplicates.
//
// Utilization is measured against the request, so these are the resources that horizontal scaling and
// rightsizing would fight over, and the resources that need a request for the HPA to work.
func UtilizationTargetedResources(metrics []autoscalingv2.MetricSpec) []corev1.ResourceName {
	var resources []corev1.ResourceName
	add := func(name corev1.ResourceName, target autoscalingv2.MetricTarget) {
		if target.Type == autoscalingv2.UtilizationMetricType && !slices.Contains(resources, name) {
			resources = append(resources, name)
		}
	}
	for _, m := range metrics {
		switch {
		case m.Type == autoscalingv2.ResourceMetricSourceType && m.Resource != nil:
			add(m.Resource.Name, m.Resource.Target)
		case m.Type == autoscalingv2.ContainerResourceMetricSourceType && m.ContainerResource != nil:
			add(m.ContainerResource.Name, m.ContainerResource.Target)
		}
	}
	slices.Sort(resources)
	return resources
}

// ScaleTargetsWorkload reports whether an autoscaler target reference with the given API version, kind and
// name points at the workload. Only the API group is compared, so that references through any version of
// the apps group match.
func ScaleTargetsWorkload(apiVersion, kind, name string, w WorkloadRef) bool {
	if kind != w.Kind || name != w.Name {
		return false
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	return err == nil && gv.Group == "apps"
}
