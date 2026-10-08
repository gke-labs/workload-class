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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func readyConditions() []metav1.Condition {
	return []metav1.Condition{
		{Type: workloadsv1.ConditionTypeValidated, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonValidationPassed},
		{Type: workloadsv1.ConditionTypePlacementPluginAttached, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonProfileResolved},
	}
}

func expectUnmutated(pod *corev1.Pod) {
	ExpectWithOffset(1, pod.Spec.NodeSelector).To(BeEmpty())
	ExpectWithOffset(1, pod.Spec.Tolerations).To(BeEmpty())
	ExpectWithOffset(1, pod.Spec.Affinity).To(BeNil())
}

var _ = Describe("Pod Webhook", func() {
	var testCtx context.Context

	BeforeEach(func() {
		testCtx = context.Background()
	})

	Context("When the Pod should not be mutated", func() {
		It("does nothing when no WorkloadClass matches", func() {
			pod := newIncomingPod()
			Expect(newDefaulter().Default(testCtx, pod)).To(Succeed())
			expectUnmutated(pod)
		})

		It("does nothing when the WorkloadClass references a different infrastructure profile kind", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, readyConditions())
			wlc.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{
				Group: "karpenter.example.io", Kind: "KarpenterSpotPolicy", Name: "other",
			}
			pod := newIncomingPod()
			Expect(newDefaulter(wlc).Default(testCtx, pod)).To(Succeed())
			expectUnmutated(pod)
		})

		It("does nothing when the WorkloadClass has no CapacityStrategy", func() {
			wlc := newWorkloadClass(nil, readyConditions())
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{})).Default(testCtx, pod)).To(Succeed())
			expectUnmutated(pod)
		})

		It("does nothing when Validated is False", func() {
			conds := readyConditions()
			conds[0].Status = metav1.ConditionFalse
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, conds)
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{})).Default(testCtx, pod)).To(Succeed())
			expectUnmutated(pod)
		})

		It("does nothing when PlacementPluginAttached is not set", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, readyConditions()[:1])
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{})).Default(testCtx, pod)).To(Succeed())
			expectUnmutated(pod)
		})

		It("fails open when the GKESpotPlacementPolicy does not exist", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, readyConditions())
			pod := newIncomingPod()
			Expect(newDefaulter(wlc).Default(testCtx, pod)).To(Succeed())
			expectUnmutated(pod)
		})
	})

	Context("When the WorkloadClass targets Spot", func() {
		It("requires Spot when fallback action is Fail (defaults)", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, readyConditions())
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{})).Default(testCtx, pod)).To(Succeed())

			Expect(pod.Spec.NodeSelector).To(HaveKeyWithValue(SpotLabelKey, SpotLabelValue))
			Expect(pod.Spec.Tolerations).To(ContainElement(HaveField("Key", SpotLabelKey)))
		})

		It("prefers Spot and restricts fallback machine families when fallback is FallbackToOnDemand", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}, readyConditions())
			policy := newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{FallbackAllowedMachineFamilies: []string{"e2", "n2d"}})
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, policy).Default(testCtx, pod)).To(Succeed())

			Expect(pod.Spec.NodeSelector).NotTo(HaveKey(SpotLabelKey))
			Expect(isPodTargetedForSpot(pod)).To(BeTrue())
			Expect(pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
			Expect(requiredTerms(pod)).To(Equal([]corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{spotIn()}},
				{MatchExpressions: []corev1.NodeSelectorRequirement{
					spotDoesNotExist(),
					{Key: MachineFamilyLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{"e2", "n2d"}},
				}},
			}))
		})

		It("targets the CustomComputeClass when set", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, readyConditions())
			policy := newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{CustomComputeClass: "spot-first"})
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, policy).Default(testCtx, pod)).To(Succeed())

			Expect(pod.Spec.NodeSelector).To(HaveKeyWithValue(ComputeClassLabelKey, "spot-first"))
		})

		It("keeps Pods on On-Demand when reversion is None and the workload is in fallback", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}, readyConditions())
			policy := newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.NoneReversionAction}})
			// A Spot-targeted Pod running on an On-Demand node indicates fallback.
			existing := newExistingPod("batch-0", "od-node", true)
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, policy, existing, newNode("od-node", false)).Default(testCtx, pod)).To(Succeed())

			expectOnDemand(pod)
		})

		It("targets Spot when reversion is Lazy even if the workload is in fallback", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}, readyConditions())
			policy := newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.LazyReversionAction}})
			existing := newExistingPod("batch-0", "od-node", true)
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, policy, existing, newNode("od-node", false)).Default(testCtx, pod)).To(Succeed())

			Expect(isPodTargetedForSpot(pod)).To(BeTrue())
		})
	})

	Context("When the WorkloadClass has a SpotRatio below 100%", func() {
		It("places the Pod on Spot while below the target ratio", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{SpotRatio: "50%"}, readyConditions())
			// 1 Spot of 2 existing -> 1/3 < 50%, so the next Pod goes to Spot.
			objs := []client.Object{
				wlc, newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{}),
				newExistingPod("batch-0", "spot-node", true), newExistingPod("batch-1", "od-node", false),
				newNode("spot-node", true), newNode("od-node", false),
			}
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			Expect(isPodTargetedForSpot(pod)).To(BeTrue())
		})

		It("places the Pod on On-Demand once the target ratio is met", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{SpotRatio: "50%"}, readyConditions())
			// 2 Spot of 3 existing -> 2/4 is not < 50%, so the next Pod goes to On-Demand.
			objs := []client.Object{
				wlc, newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{}),
				newExistingPod("batch-0", "spot-node", true), newExistingPod("batch-1", "spot-node", true),
				newExistingPod("batch-2", "od-node", false),
				newNode("spot-node", true), newNode("od-node", false),
			}
			pod := newIncomingPod()
			Expect(newDefaulter(objs...).Default(testCtx, pod)).To(Succeed())

			expectOnDemand(pod)
		})

		It("places all Pods on On-Demand when SpotRatio is 0%", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{SpotRatio: "0%"}, readyConditions())
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{})).Default(testCtx, pod)).To(Succeed())

			expectOnDemand(pod)
		})
	})

	Context("When the WorkloadClass targets OnDemand", func() {
		It("pins the Pod to On-Demand and strips Spot targeting", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand}, readyConditions())
			pod := newIncomingPod()
			setNodeSelector(pod, SpotLabelKey, SpotLabelValue)
			ensureSpotToleration(pod)
			Expect(newDefaulter(wlc, newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{})).Default(testCtx, pod)).To(Succeed())

			expectOnDemand(pod)
		})
	})

	Context("When the WorkloadClass has no InfrastructureProfileRef", func() {
		newNoRefWorkloadClass := func(cs *workloadsv1.CapacityStrategy, conds []metav1.Condition) *workloadsv1.WorkloadClass {
			wlc := newWorkloadClass(cs, conds)
			wlc.Spec.InfrastructureProfileRef = nil
			return wlc
		}
		// The WorkloadClass controller removes PlacementPluginAttached when there is no ref.
		validatedOnly := func() []metav1.Condition { return readyConditions()[:1] }

		It("pins the Pod to On-Demand when Type is OnDemand", func() {
			wlc := newNoRefWorkloadClass(&workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand}, validatedOnly())
			pod := newIncomingPod()
			Expect(newDefaulter(wlc).Default(testCtx, pod)).To(Succeed())

			expectOnDemand(pod)
		})

		It("requires Spot with default settings", func() {
			wlc := newNoRefWorkloadClass(&workloadsv1.CapacityStrategy{}, validatedOnly())
			pod := newIncomingPod()
			Expect(newDefaulter(wlc).Default(testCtx, pod)).To(Succeed())

			Expect(pod.Spec.NodeSelector).To(Equal(map[string]string{SpotLabelKey: SpotLabelValue}))
			Expect(pod.Spec.Tolerations).To(ContainElement(HaveField("Key", SpotLabelKey)))
		})

		It("prefers Spot without machine family restrictions when fallback is FallbackToOnDemand", func() {
			wlc := newNoRefWorkloadClass(&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}, validatedOnly())
			pod := newIncomingPod()
			Expect(newDefaulter(wlc).Default(testCtx, pod)).To(Succeed())

			Expect(isPodTargetedForSpot(pod)).To(BeTrue())
			Expect(pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
			Expect(requiredTerms(pod)).To(BeEmpty())
		})

		It("treats reversion as Lazy and targets Spot even if the workload is in fallback", func() {
			wlc := newNoRefWorkloadClass(&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}, validatedOnly())
			existing := newExistingPod("batch-0", "od-node", true)
			pod := newIncomingPod()
			Expect(newDefaulter(wlc, existing, newNode("od-node", false)).Default(testCtx, pod)).To(Succeed())

			Expect(isPodTargetedForSpot(pod)).To(BeTrue())
		})

		It("does nothing when Validated is not True", func() {
			conds := validatedOnly()
			conds[0].Status = metav1.ConditionFalse
			wlc := newNoRefWorkloadClass(&workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand}, conds)
			pod := newIncomingPod()
			Expect(newDefaulter(wlc).Default(testCtx, pod)).To(Succeed())

			expectUnmutated(pod)
		})
	})
})
