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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// cpuUtilizationMetric returns a Resource metric that targets an average CPU utilization of 60%.
func cpuUtilizationMetric() autoscalingv2.MetricSpec {
	return autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name:   corev1.ResourceCPU,
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](60)},
		},
	}
}

// These specs check the resourceManagement schema of the WorkloadClass and WorkloadClassGuardrail CRDs: the
// defaults the API server applies, and the OpenAPI and CEL rules that reject malformed specs at admission.
var _ = Describe("ResourceManagement API", func() {
	const namespace = "default"

	newWC := func(name string, rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
		return &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector:        &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
				ResourceManagement: rm,
			},
		}
	}

	create := func(obj client.Object) error {
		err := k8sClient.Create(ctx, obj)
		if err == nil {
			DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, obj))).To(Succeed()) })
		}
		return err
	}

	autoscaling := func() *workloadsv1.Autoscaling {
		return &workloadsv1.Autoscaling{
			Replicas: workloadsv1.ReplicaBounds{Max: 10},
			Metrics:  []autoscalingv2.MetricSpec{cpuUtilizationMetric()},
		}
	}

	Context("WorkloadClass", func() {
		It("applies the documented defaults", func() {
			wc := newWC("rm-api-defaults", &workloadsv1.ResourceManagement{
				Autoscaling: autoscaling(),
				Rightsizing: &workloadsv1.Rightsizing{},
			})
			Expect(create(wc)).To(Succeed())

			got := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wc), got)).To(Succeed())
			rm := got.Spec.ResourceManagement
			Expect(rm.Autoscaling.Replicas.Min).To(HaveValue(Equal(int32(1))))
			Expect(rm.Autoscaling.Profile).To(Equal(workloadsv1.AutoscalingProfileBalanced))
			Expect(rm.Rightsizing.Mode).To(Equal(workloadsv1.RightsizingModeRecommend))
			Expect(rm.Rightsizing.Resources).To(ConsistOf(workloadsv1.RightsizedResourceCPU, workloadsv1.RightsizedResourceMemory))
			Expect(rm.Rightsizing.UpdateStrategy).To(Equal(workloadsv1.RightsizingUpdateStrategyInPlacePreferred))
		})

		It("accepts a fully specified resourceManagement", func() {
			wc := newWC("rm-api-full", &workloadsv1.ResourceManagement{
				Autoscaling: &workloadsv1.Autoscaling{
					Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](2), Max: 20},
					Metrics: []autoscalingv2.MetricSpec{
						cpuUtilizationMetric(),
						{
							Type: autoscalingv2.PodsMetricSourceType,
							Pods: &autoscalingv2.PodsMetricSource{
								Metric: autoscalingv2.MetricIdentifier{Name: "requests_per_second"},
								Target: autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType, AverageValue: ptr.To(resource.MustParse("100"))},
							},
						},
					},
					Profile: workloadsv1.AutoscalingProfileConservative,
				},
				Rightsizing: &workloadsv1.Rightsizing{
					Mode:              workloadsv1.RightsizingModeAuto,
					Resources:         []workloadsv1.RightsizedResource{workloadsv1.RightsizedResourceMemory},
					UpdateStrategy:    workloadsv1.RightsizingUpdateStrategyRecreate,
					ExcludeContainers: []string{"istio-proxy"},
					Bounds: &workloadsv1.ContainerBounds{
						Min: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
						Max: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi")},
					},
				},
			})
			Expect(create(wc)).To(Succeed())
		})

		It("accepts rightsizing mode Off on its own", func() {
			wc := newWC("rm-api-off", &workloadsv1.ResourceManagement{
				Rightsizing: &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOff},
			})
			Expect(create(wc)).To(Succeed())
		})

		DescribeTable("rejects malformed specs",
			func(name string, rm *workloadsv1.ResourceManagement, wantMessage string) {
				Expect(create(newWC(name, rm))).To(MatchError(ContainSubstring(wantMessage)))
			},
			Entry("neither autoscaling nor rightsizing", "rm-api-empty",
				&workloadsv1.ResourceManagement{},
				"set autoscaling, rightsizing, or both"),
			Entry("replicas.min above replicas.max", "rm-api-min-above-max",
				&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
					Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](5), Max: 2},
					Metrics:  []autoscalingv2.MetricSpec{cpuUtilizationMetric()},
				}},
				"replicas.min must not exceed replicas.max"),
			Entry("replicas.max of zero", "rm-api-max-zero",
				&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
					Replicas: workloadsv1.ReplicaBounds{Max: 0},
					Metrics:  []autoscalingv2.MetricSpec{cpuUtilizationMetric()},
				}},
				"should be greater than or equal to 1"),
			Entry("no metrics", "rm-api-no-metrics",
				&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
					Replicas: workloadsv1.ReplicaBounds{Max: 3},
					Metrics:  []autoscalingv2.MetricSpec{},
				}},
				"should have at least 1 items"),
			Entry("an Object metric", "rm-api-object-metric",
				&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
					Replicas: workloadsv1.ReplicaBounds{Max: 3},
					Metrics: []autoscalingv2.MetricSpec{{
						Type: autoscalingv2.ObjectMetricSourceType,
						Object: &autoscalingv2.ObjectMetricSource{
							DescribedObject: autoscalingv2.CrossVersionObjectReference{APIVersion: "v1", Kind: "Service", Name: "web"},
							Metric:          autoscalingv2.MetricIdentifier{Name: "requests_per_second"},
							Target:          autoscalingv2.MetricTarget{Type: autoscalingv2.ValueMetricType, Value: ptr.To(resource.MustParse("10"))},
						},
					}},
				}},
				"Object metrics are not allowed"),
			Entry("an unknown profile", "rm-api-bad-profile",
				&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
					Replicas: workloadsv1.ReplicaBounds{Max: 3},
					Metrics:  []autoscalingv2.MetricSpec{cpuUtilizationMetric()},
					Profile:  "Eager",
				}},
				"Unsupported value: \"Eager\""),
			Entry("an unknown rightsizing mode", "rm-api-bad-mode",
				&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{Mode: "Always"}},
				"Unsupported value: \"Always\""),
			Entry("an unknown update strategy", "rm-api-bad-strategy",
				&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{UpdateStrategy: "Rolling"}},
				"Unsupported value: \"Rolling\""),
			Entry("an unknown rightsized resource", "rm-api-bad-resource",
				&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{
					Resources: []workloadsv1.RightsizedResource{"ephemeral-storage"},
				}},
				"Unsupported value: \"ephemeral-storage\""),
			Entry("bounds.min above bounds.max", "rm-api-bounds-inverted",
				&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{
					Bounds: &workloadsv1.ContainerBounds{
						Min: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
						Max: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
					},
				}},
				"min must not exceed max for any resource"),
			Entry("bounds on a resource other than cpu and memory", "rm-api-bounds-resource",
				&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{
					Bounds: &workloadsv1.ContainerBounds{
						Max: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")},
					},
				}},
				"only cpu and memory are supported"),
		)

		// Typed clients omit empty lists and can't send integer quantities, so these specs use unstructured objects
		// to send exactly what a user could write in YAML.
		newUnstructuredWC := func(name string, rightsizing map[string]any) *unstructured.Unstructured {
			return &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": workloadsv1.GroupVersion.String(),
				"kind":       "WorkloadClass",
				"metadata":   map[string]any{"name": name, "namespace": namespace},
				"spec":       map[string]any{"resourceManagement": map[string]any{"rightsizing": rightsizing}},
			}}
		}

		It("rejects an explicitly empty resources list", func() {
			Expect(create(newUnstructuredWC("rm-api-no-resources", map[string]any{"resources": []any{}}))).
				To(MatchError(ContainSubstring("should have at least 1 items")))
		})

		It("compares bounds written as integers", func() {
			bounds := func(minCPU, maxCPU any) map[string]any {
				return map[string]any{"bounds": map[string]any{
					"min": map[string]any{"cpu": minCPU},
					"max": map[string]any{"cpu": maxCPU},
				}}
			}
			Expect(create(newUnstructuredWC("rm-api-int-bounds-inverted", bounds(int64(4), int64(2))))).
				To(MatchError(ContainSubstring("min must not exceed max for any resource")))
			Expect(create(newUnstructuredWC("rm-api-int-bounds", bounds(int64(1), "1500m")))).To(Succeed())
		})
	})

	Context("WorkloadClassGuardrail", func() {
		newGuardrail := func(name string, rm *workloadsv1.ResourceManagementConstraints) *workloadsv1.WorkloadClassGuardrail {
			return &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption:         workloadsv1.Disruption{MaxNonDisruptionDurationDays: 30},
						ResourceManagement: rm,
					},
				},
			}
		}

		It("accepts fully specified resourceManagement constraints", func() {
			g := newGuardrail("rm-api-guardrail-full", &workloadsv1.ResourceManagementConstraints{
				AllowedRightsizingModes: []workloadsv1.RightsizingMode{
					workloadsv1.RightsizingModeOff, workloadsv1.RightsizingModeRecommend,
					workloadsv1.RightsizingModeOnCreate, workloadsv1.RightsizingModeAuto,
				},
				AllowedUpdateStrategies:    []workloadsv1.RightsizingUpdateStrategy{workloadsv1.RightsizingUpdateStrategyInPlacePreferred},
				MaxReplicas:                ptr.To[int32](100),
				AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileBalanced, workloadsv1.AutoscalingProfileConservative},
				AllowedMetricTypes: []autoscalingv2.MetricSourceType{
					autoscalingv2.ResourceMetricSourceType, autoscalingv2.PodsMetricSourceType, autoscalingv2.ExternalMetricSourceType,
				},
				ContainerBounds: &workloadsv1.ContainerBounds{
					Min: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
					Max: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("64Gi")},
				},
			})
			Expect(create(g)).To(Succeed())
		})

		DescribeTable("rejects malformed constraints",
			func(name string, rm *workloadsv1.ResourceManagementConstraints, wantMessage string) {
				Expect(create(newGuardrail(name, rm))).To(MatchError(ContainSubstring(wantMessage)))
			},
			Entry("maxReplicas of zero", "rm-api-guardrail-max-zero",
				&workloadsv1.ResourceManagementConstraints{MaxReplicas: ptr.To[int32](0)},
				"should be greater than or equal to 1"),
			Entry("an unknown metric type", "rm-api-guardrail-bad-metric",
				&workloadsv1.ResourceManagementConstraints{AllowedMetricTypes: []autoscalingv2.MetricSourceType{"Custom"}},
				"Unsupported value: \"Custom\""),
			Entry("an unknown rightsizing mode", "rm-api-guardrail-bad-mode",
				&workloadsv1.ResourceManagementConstraints{AllowedRightsizingModes: []workloadsv1.RightsizingMode{"Always"}},
				"Unsupported value: \"Always\""),
			Entry("containerBounds.min above containerBounds.max", "rm-api-guardrail-bounds-inverted",
				&workloadsv1.ResourceManagementConstraints{ContainerBounds: &workloadsv1.ContainerBounds{
					Min: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
					Max: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
				}},
				"min must not exceed max for any resource"),
		)
	})
})
