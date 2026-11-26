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
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// These specs exercise the CRD OpenAPI and CEL validation of the resourceManagement API against a real API server.
var _ = Describe("ResourceManagement API validation", func() {
	cpuUtilization := func() autoscalingv2.MetricSpec {
		return autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: corev1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: ptr.To[int32](60),
				},
			},
		}
	}
	resources := func(cpu, memory string) corev1.ResourceList {
		rl := corev1.ResourceList{}
		if cpu != "" {
			rl[corev1.ResourceCPU] = resource.MustParse(cpu)
		}
		if memory != "" {
			rl[corev1.ResourceMemory] = resource.MustParse(memory)
		}
		return rl
	}
	newClass := func(rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
		return &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "rm-", Namespace: "default"},
			Spec:       workloadsv1.WorkloadClassSpec{ResourceManagement: rm},
		}
	}
	newGuardrail := func(c *workloadsv1.ResourceManagementConstraints) *workloadsv1.WorkloadClassGuardrail {
		return &workloadsv1.WorkloadClassGuardrail{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "rm-"},
			Spec: workloadsv1.WorkloadClassGuardrailSpec{Constraints: workloadsv1.Constraints{
				Disruption:         workloadsv1.Disruption{MaxNonDisruptionDurationDays: 30},
				ResourceManagement: c,
			}},
		}
	}
	expectInvalid := func(obj client.Object, wantMessage string) {
		err := k8sClient.Create(ctx, obj)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
		Expect(err.Error()).To(ContainSubstring(wantMessage))
	}

	It("accepts a WorkloadClass with all three parts and applies defaults", func() {
		wc := newClass(&workloadsv1.ResourceManagement{
			InitialResources: &workloadsv1.InitialResources{Requests: resources("250m", "256Mi")},
			Autoscaling: &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Max: 20},
				Metrics:  []autoscalingv2.MetricSpec{cpuUtilization()},
			},
			Rightsizing: &workloadsv1.Rightsizing{
				Bounds: &workloadsv1.ContainerBounds{Min: resources("100m", "128Mi"), Max: resources("4", "8Gi")},
			},
		})
		Expect(k8sClient.Create(ctx, wc)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, wc)).To(Succeed()) })

		rm := wc.Spec.ResourceManagement
		Expect(rm.InitialResources.LimitPolicy).To(Equal(workloadsv1.LimitPolicyMemoryEqualsRequest))
		Expect(rm.Autoscaling.Replicas.Min).To(HaveValue(Equal(int32(1))))
		Expect(rm.Autoscaling.Profile).To(Equal(workloadsv1.AutoscalingProfileBalanced))
		Expect(rm.Rightsizing.Mode).To(Equal(workloadsv1.RightsizingModeRecommend))
		Expect(rm.Rightsizing.UpdateStrategy).To(Equal(workloadsv1.RightsizingUpdateStrategyInPlacePreferred))
	})

	It("accepts each part on its own", func() {
		for _, rm := range []*workloadsv1.ResourceManagement{
			{InitialResources: &workloadsv1.InitialResources{Requests: resources("250m", "256Mi")}},
			{Autoscaling: &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](2), Max: 2},
				Metrics:  []autoscalingv2.MetricSpec{cpuUtilization()},
			}},
			{Rightsizing: &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto}},
		} {
			wc := newClass(rm)
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			Expect(k8sClient.Delete(ctx, wc)).To(Succeed())
		}
	})

	DescribeTable("rejects an invalid WorkloadClass",
		func(rm *workloadsv1.ResourceManagement, wantMessage string) {
			expectInvalid(newClass(rm), wantMessage)
		},
		Entry("no part set", &workloadsv1.ResourceManagement{},
			"set at least one of initialResources, autoscaling or rightsizing"),
		Entry("initial requests without memory",
			&workloadsv1.ResourceManagement{InitialResources: &workloadsv1.InitialResources{Requests: resources("250m", "")}},
			"requests must set both cpu and memory"),
		Entry("initial requests without cpu",
			&workloadsv1.ResourceManagement{InitialResources: &workloadsv1.InitialResources{Requests: resources("", "256Mi")}},
			"requests must set both cpu and memory"),
		Entry("unknown limit policy",
			&workloadsv1.ResourceManagement{InitialResources: &workloadsv1.InitialResources{
				Requests: resources("250m", "256Mi"), LimitPolicy: "Double",
			}},
			"Unsupported value"),
		Entry("replicas.min above replicas.max",
			&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](5), Max: 2},
				Metrics:  []autoscalingv2.MetricSpec{cpuUtilization()},
			}},
			"replicas.min must not exceed replicas.max"),
		Entry("no metrics",
			&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Max: 2},
				Metrics:  []autoscalingv2.MetricSpec{},
			}},
			"metrics"),
		Entry("Object metric",
			&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Max: 2},
				Metrics: []autoscalingv2.MetricSpec{{
					Type: autoscalingv2.ObjectMetricSourceType,
					Object: &autoscalingv2.ObjectMetricSource{
						DescribedObject: autoscalingv2.CrossVersionObjectReference{Kind: "Ingress", Name: "main", APIVersion: "networking.k8s.io/v1"},
						Metric:          autoscalingv2.MetricIdentifier{Name: "requests-per-second"},
						Target:          autoscalingv2.MetricTarget{Type: autoscalingv2.ValueMetricType, Value: ptr.To(resource.MustParse("10k"))},
					},
				}},
			}},
			"Object metrics are not allowed"),
		Entry("unknown rightsizing mode",
			&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{Mode: "Always"}},
			"Unsupported value"),
		Entry("unknown rightsized resource",
			&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{Resources: []workloadsv1.RightsizedResource{"gpu"}}},
			"Unsupported value"),
	)

	It("accepts a WorkloadClassGuardrail with resource management constraints", func() {
		g := newGuardrail(&workloadsv1.ResourceManagementConstraints{
			RequireInitialResources:    true,
			AllowedLimitPolicies:       []workloadsv1.LimitPolicy{workloadsv1.LimitPolicyMemoryEqualsRequest, workloadsv1.LimitPolicyEqualsRequest},
			AllowedRightsizingModes:    []workloadsv1.RightsizingMode{workloadsv1.RightsizingModeOff, workloadsv1.RightsizingModeRecommend},
			AllowedUpdateStrategies:    []workloadsv1.RightsizingUpdateStrategy{workloadsv1.RightsizingUpdateStrategyInPlacePreferred},
			MaxReplicas:                ptr.To[int32](100),
			AllowedAutoscalingProfiles: []workloadsv1.AutoscalingProfile{workloadsv1.AutoscalingProfileBalanced},
			AllowedMetricTypes:         []autoscalingv2.MetricSourceType{autoscalingv2.ResourceMetricSourceType, autoscalingv2.PodsMetricSourceType},
			ContainerBounds:            &workloadsv1.ContainerBounds{Min: resources("10m", "32Mi"), Max: resources("16", "64Gi")},
		})
		Expect(k8sClient.Create(ctx, g)).To(Succeed())
		Expect(k8sClient.Delete(ctx, g)).To(Succeed())
	})

	DescribeTable("accepts the sample manifests",
		func(file string, obj client.Object) {
			data, err := os.ReadFile(filepath.Join("..", "..", "config", "samples", file))
			Expect(err).NotTo(HaveOccurred())
			Expect(yaml.UnmarshalStrict(data, obj)).To(Succeed())
			if obj.GetNamespace() != "" {
				obj.SetNamespace("default")
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			Expect(k8sClient.Delete(ctx, obj)).To(Succeed())
		},
		Entry("initial resources", "workloads_v1_workloadclass_initial_resources.yaml", &workloadsv1.WorkloadClass{}),
		Entry("autoscaling", "workloads_v1_workloadclass_autoscaling.yaml", &workloadsv1.WorkloadClass{}),
		Entry("all three parts", "workloads_v1_workloadclass_resource_management.yaml", &workloadsv1.WorkloadClass{}),
		Entry("guardrail", "workloads_v1_workloadclassguardrail_resource_management.yaml", &workloadsv1.WorkloadClassGuardrail{}),
	)

	DescribeTable("rejects an invalid WorkloadClassGuardrail",
		func(c *workloadsv1.ResourceManagementConstraints, wantMessage string) {
			expectInvalid(newGuardrail(c), wantMessage)
		},
		Entry("Object metric type",
			&workloadsv1.ResourceManagementConstraints{
				AllowedMetricTypes: []autoscalingv2.MetricSourceType{autoscalingv2.ObjectMetricSourceType},
			},
			"Unsupported value"),
		Entry("maxReplicas of zero",
			&workloadsv1.ResourceManagementConstraints{MaxReplicas: ptr.To[int32](0)},
			"maxReplicas"),
		Entry("unknown limit policy",
			&workloadsv1.ResourceManagementConstraints{AllowedLimitPolicies: []workloadsv1.LimitPolicy{"Double"}},
			"Unsupported value"),
	)
})
