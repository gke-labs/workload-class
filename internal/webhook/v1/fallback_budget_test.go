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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// newFallbackGuardrail returns a WorkloadClassGuardrail whose gke-spot-placement constraint sets maxFallbackRatio.
func newFallbackGuardrail(name, maxFallbackRatioJSON string) *workloadsv1.WorkloadClassGuardrail {
	raw := fmt.Sprintf(`{"fallback":{"allowFallbackToOnDemand":true,"maxFallbackRatio":%s}}`, maxFallbackRatioJSON)
	return &workloadsv1.WorkloadClassGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: workloadsv1.WorkloadClassGuardrailSpec{
			PluginConstraints: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{{
					Name:       workloadsv1.PluginNameGKESpotPlacement,
					Parameters: &apiextensionsv1.JSON{Raw: []byte(raw)},
				}},
			},
		},
	}
}

func expectRequiredSpot(pod *corev1.Pod) {
	ExpectWithOffset(1, pod.Spec.NodeSelector).To(HaveKeyWithValue(SpotLabelKey, SpotLabelValue))
	ExpectWithOffset(1, pod.Spec.Tolerations).To(ContainElement(HaveField("Key", SpotLabelKey)))
}

func expectPreferredSpot(pod *corev1.Pod) {
	ExpectWithOffset(1, pod.Spec.NodeSelector).NotTo(HaveKey(SpotLabelKey))
	ExpectWithOffset(1, pod.Spec.Tolerations).To(ContainElement(HaveField("Key", SpotLabelKey)))
	ExpectWithOffset(1, pod.Spec.Affinity).NotTo(BeNil())
	ExpectWithOffset(1, pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
}

var _ = Describe("Guardrail maxFallbackRatio", func() {
	DescribeTable("fallbackWithinBudget",
		func(fallbackPods, totalPods int, ratio intstr.IntOrString, want bool, wantErr bool) {
			got, err := fallbackWithinBudget(fallbackPods, totalPods, ratio)
			if wantErr {
				Expect(err).To(HaveOccurred())
				return
			}
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want))
		},
		Entry("1 of 4 within 25%", 1, 4, intstr.FromString("25%"), true, false),
		Entry("2 of 4 exceeds 25%", 2, 4, intstr.FromString("25%"), false, false),
		Entry("1 of 1 exceeds 25%", 1, 1, intstr.FromString("25%"), false, false),
		Entry("0 of 1 within 0%", 0, 1, intstr.FromString("0%"), true, false),
		Entry("1 of 1 exceeds 0%", 1, 1, intstr.FromString("0%"), false, false),
		Entry("anything within 100%", 7, 7, intstr.FromString("100%"), true, false),
		Entry("2 within absolute 2", 2, 10, intstr.FromInt32(2), true, false),
		Entry("3 exceeds absolute 2", 3, 10, intstr.FromInt32(2), false, false),
		Entry("1 exceeds absolute 0", 1, 10, intstr.FromInt32(0), false, false),
		Entry("invalid percentage", 1, 1, intstr.FromString("abc"), false, true),
		Entry("negative count", 1, 1, intstr.FromInt32(-1), false, true),
	)

	Context("Default with a fallback budget", func() {
		var testCtx context.Context
		fallbackCS := func() *workloadsv1.CapacityStrategy {
			return &workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}
		}
		// Lazy reversion: new Pods target Spot again after a fallback, so the budget check is reached. (Under
		// None, in-fallback Pods are pinned to On-Demand before the budget is consulted; see the last spec.)
		lazyPolicy := func() *workloadsv1.GKESpotPlacementPolicy {
			return newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.LazyReversionAction}})
		}
		// Two Spot-targeted Pods: one on a Spot node, one fallen back to an On-Demand node.
		inFallbackObjects := func() []client.Object {
			return []client.Object{
				newExistingPod("batch-0", "spot-node", true), newNode("spot-node", true),
				newExistingPod("batch-1", "od-node", true), newNode("od-node", false),
			}
		}

		BeforeEach(func() {
			testCtx = context.Background()
		})

		It("downgrades to strict Spot when the percentage budget would be exceeded", func() {
			// 1 of 2 existing Pods is on fallback; admitting this one with fallback would make 2 of 3 (67%) > 25%.
			objs := append(inFallbackObjects(), newWorkloadClass(fallbackCS(), readyConditions()),
				lazyPolicy(), newFallbackGuardrail("default", `"25%"`))
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectRequiredSpot(pod)
			Expect(pod.Spec.Affinity).To(BeNil())
		})

		It("allows fallback while within the percentage budget", func() {
			// 1 of 2 on fallback; 2 of 3 is within 67%.
			objs := append(inFallbackObjects(), newWorkloadClass(fallbackCS(), readyConditions()),
				lazyPolicy(), newFallbackGuardrail("default", `"67%"`))
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectPreferredSpot(pod)
		})

		It("downgrades to strict Spot when the absolute budget would be exceeded", func() {
			// 1 Pod already on fallback; the budget is 1.
			objs := append(inFallbackObjects(), newWorkloadClass(fallbackCS(), readyConditions()),
				lazyPolicy(), newFallbackGuardrail("default", `1`))
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectRequiredSpot(pod)
		})

		It("applies the strictest of several guardrails", func() {
			objs := append(inFallbackObjects(), newWorkloadClass(fallbackCS(), readyConditions()),
				lazyPolicy(),
				newFallbackGuardrail("lenient", `"100%"`), newFallbackGuardrail("strict", `1`))
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectRequiredSpot(pod)
		})

		It("does not enforce the budget when the workload is not in fallback", func() {
			// Both existing Pods are on Spot nodes; a 1-Pod workload would otherwise never fit a 25% budget.
			objs := []client.Object{
				newExistingPod("batch-0", "spot-node", true), newNode("spot-node", true),
				newWorkloadClass(fallbackCS(), readyConditions()),
				lazyPolicy(), newFallbackGuardrail("default", `"25%"`),
			}
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectPreferredSpot(pod)
		})

		It("enforces the budget when InFallback is set even if no fallback Pod is visible", func() {
			conds := append(readyConditions(), metav1.Condition{
				Type: workloadsv1.ConditionTypeInFallback, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonFallbackActive,
			})
			objs := []client.Object{
				newWorkloadClass(fallbackCS(), conds),
				lazyPolicy(), newFallbackGuardrail("default", `"25%"`),
			}
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			// 0 existing Pods: this Pod falling back would be 1 of 1 (100%) > 25%.
			expectRequiredSpot(pod)
		})

		It("ignores guardrails without a maxFallbackRatio", func() {
			g := newFallbackGuardrail("default", `"25%"`)
			g.Spec.PluginConstraints.Placement[0].Parameters.Raw = []byte(`{"enforcementMode":"Allowed"}`)
			objs := append(inFallbackObjects(), newWorkloadClass(fallbackCS(), readyConditions()),
				lazyPolicy(), g)
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectPreferredSpot(pod)
		})

		It("does not affect Fail workloads, which never fall back", func() {
			objs := append(inFallbackObjects(), newWorkloadClass(&workloadsv1.CapacityStrategy{}, readyConditions()),
				lazyPolicy(), newFallbackGuardrail("default", `1`))
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectRequiredSpot(pod)
		})

		It("still pins to On-Demand under None reversion once in fallback (budget does not apply)", func() {
			// None + in fallback keeps new Pods on On-Demand; that branch runs before the budget check.
			objs := append(inFallbackObjects(), newWorkloadClass(fallbackCS(), readyConditions()),
				newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.NoneReversionAction}}),
				newFallbackGuardrail("default", `1`))
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectOnDemand(pod)
		})

		It("does not count fallback Pods that best-match a more specific WorkloadClass", func() {
			// general matches app=batch (specificity 1), while specific matches app=batch,tier=gpu (specificity 2).
			general := newWorkloadClass(fallbackCS(), readyConditions())

			specific := newWorkloadClass(fallbackCS(), readyConditions())
			specific.Name = "specific"
			specific.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "batch", "tier": "gpu"}}

			specificFallbackPod := newExistingPod("gpu-0", "od-node", true)
			specificFallbackPod.Labels = map[string]string{"app": "batch", "tier": "gpu"}

			objs := []client.Object{
				general, specific,
				specificFallbackPod, newNode("od-node", false),
				lazyPolicy(), newFallbackGuardrail("default", `1`),
			}
			// Incoming Pod has app=batch only, so it matches general (which has 0 fallback Pods of its own).
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectPreferredSpot(pod)
		})
	})
})
