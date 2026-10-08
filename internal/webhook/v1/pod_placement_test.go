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
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	testNamespace  = "team-a"
	testWLCName    = "batch"
	testPolicyName = "spot-policy"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(s)).To(Succeed())
	Expect(workloadsv1.AddToScheme(s)).To(Succeed())
	return s
}

func newWorkloadClass(cs *workloadsv1.CapacityStrategy, conds []metav1.Condition) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: testWLCName, Namespace: testNamespace},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector:      &metav1.LabelSelector{MatchLabels: map[string]string{"app": "batch"}},
			CapacityStrategy: cs,
			InfrastructureProfileRef: &workloadsv1.InfrastructureProfileReference{
				Group: "workloads.gke.io",
				Kind:  workloadsv1.GKESpotPlacementPolicyKind,
				Name:  testPolicyName,
			},
		},
		Status: workloadsv1.WorkloadClassStatus{Conditions: conds},
	}
}

func newSpotPolicy(spec workloadsv1.GKESpotPlacementPolicySpec) *workloadsv1.GKESpotPlacementPolicy {
	return &workloadsv1.GKESpotPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: testPolicyName},
		Spec:       spec,
	}
}

func newIncomingPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "batch-", Namespace: testNamespace, Labels: map[string]string{"app": "batch"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
	}
}

func newExistingPod(name, nodeName string, targetSpot bool) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, Labels: map[string]string{"app": "batch"}},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if targetSpot {
		ensureSpotToleration(p)
	}
	return p
}

func newNode(name string, spot bool) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}}}
	if spot {
		n.Labels[SpotLabelKey] = SpotLabelValue
	}
	return n
}

func newDefaulter(objs ...client.Object) *PodPlacementDefaulter {
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(objs...).Build()
	return &PodPlacementDefaulter{Client: c}
}

func requiredTerms(pod *corev1.Pod) []corev1.NodeSelectorTerm {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nil
	}
	return pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
}

func expectOnDemand(pod *corev1.Pod) {
	ExpectWithOffset(1, isPodTargetedForSpot(pod)).To(BeFalse())
	terms := requiredTerms(pod)
	ExpectWithOffset(1, terms).NotTo(BeEmpty())
	for _, t := range terms {
		ExpectWithOffset(1, t.MatchExpressions).To(ContainElement(spotDoesNotExist()))
	}
}

var _ = Describe("Pod placement helpers", func() {
	Context("mutateForOnDemand", func() {
		It("strips Spot targeting, keeps other tolerations, and requires non-Spot nodes", func() {
			other := corev1.Toleration{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "batch", Effect: corev1.TaintEffectNoSchedule}
			pod := newIncomingPod()
			setNodeSelector(pod, SpotLabelKey, SpotLabelValue)
			pod.Spec.Tolerations = []corev1.Toleration{other}
			ensureSpotToleration(pod)

			mutateForOnDemand(pod)

			expectOnDemand(pod)
			Expect(pod.Spec.Tolerations).To(Equal([]corev1.Toleration{other}))
		})

		It("ANDs new terms with existing required terms instead of ORing them", func() {
			zoneA := corev1.NodeSelectorRequirement{Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
			zoneB := corev1.NodeSelectorRequirement{Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}}
			pod := newIncomingPod()
			pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{zoneA}},
					{MatchExpressions: []corev1.NodeSelectorRequirement{zoneB}},
				}},
			}}

			mutateForOnDemand(pod)

			Expect(requiredTerms(pod)).To(Equal([]corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{zoneA, spotDoesNotExist()}},
				{MatchExpressions: []corev1.NodeSelectorRequirement{zoneB, spotDoesNotExist()}},
			}))
		})
	})

	Context("mutateForSpot", func() {
		It("requires Spot when fallback action is Fail and does not duplicate the toleration", func() {
			pod := newIncomingPod()
			ensureSpotToleration(pod)

			mutateForSpot(pod, workloadsv1.FallbackActionFail, nil)

			Expect(pod.Spec.NodeSelector).To(Equal(map[string]string{SpotLabelKey: SpotLabelValue}))
			Expect(pod.Spec.Tolerations).To(HaveLen(1))
			Expect(pod.Spec.Affinity).To(BeNil())
		})

		It("prefers Spot without required affinity when fallback is FallbackToOnDemand and there is no policy", func() {
			pod := newIncomingPod()

			mutateForSpot(pod, workloadsv1.FallbackActionFallbackToOnDemand, nil)

			Expect(isPodTargetedForSpot(pod)).To(BeTrue())
			Expect(pod.Spec.NodeSelector).NotTo(HaveKey(SpotLabelKey))
			Expect(pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
			Expect(requiredTerms(pod)).To(BeEmpty())
		})

		It("restricts On-Demand fallback to the policy's machine families", func() {
			pod := newIncomingPod()
			policy := newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{FallbackAllowedMachineFamilies: []string{"e2", "n2d"}})

			mutateForSpot(pod, workloadsv1.FallbackActionFallbackToOnDemand, policy)

			Expect(requiredTerms(pod)).To(Equal([]corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{spotIn()}},
				{MatchExpressions: []corev1.NodeSelectorRequirement{
					spotDoesNotExist(),
					{Key: MachineFamilyLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{"e2", "n2d"}},
				}},
			}))
		})

		It("targets the policy's CustomComputeClass unless the Pod already selects one", func() {
			policy := newSpotPolicy(workloadsv1.GKESpotPlacementPolicySpec{CustomComputeClass: "spot-first"})

			pod := newIncomingPod()
			mutateForSpot(pod, workloadsv1.FallbackActionFail, policy)
			Expect(pod.Spec.NodeSelector).To(HaveKeyWithValue(ComputeClassLabelKey, "spot-first"))

			preset := newIncomingPod()
			setNodeSelector(preset, ComputeClassLabelKey, "user-choice")
			mutateForSpot(preset, workloadsv1.FallbackActionFail, policy)
			Expect(preset.Spec.NodeSelector).To(HaveKeyWithValue(ComputeClassLabelKey, "user-choice"))
		})
	})

	DescribeTable("shouldUseSpot",
		func(spotExisting, totalExisting, spotRatio int, want bool) {
			Expect(shouldUseSpot(spotExisting, totalExisting, spotRatio)).To(Equal(want))
		},
		Entry("first Pod at 80% goes to Spot", 0, 0, 80, true),
		Entry("0% never uses Spot", 0, 0, 0, false),
		Entry("1 of 2 Spot at 50% -> next Pod to Spot", 1, 2, 50, true),
		Entry("2 of 3 Spot at 50% -> next Pod to On-Demand", 2, 3, 50, false),
	)

	DescribeTable("parsePercentage",
		func(in string, want int, wantErr bool) {
			got, err := parsePercentage(in)
			if wantErr {
				Expect(err).To(HaveOccurred())
				return
			}
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want))
		},
		Entry("0%", "0%", 0, false),
		Entry("80%", "80%", 80, false),
		Entry("100%", "100%", 100, false),
		Entry("missing suffix", "80", 0, true),
		Entry("above 100", "101%", 0, true),
		Entry("negative", "-1%", 0, true),
		Entry("not a number", "abc%", 0, true),
	)

	Context("Pod state inspection", func() {
		var testCtx context.Context

		BeforeEach(func() {
			testCtx = context.Background()
		})

		It("counts only active Pods selected by the WorkloadClass", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, nil)
			succeeded := newExistingPod("batch-done", "spot-node", true)
			succeeded.Status.Phase = corev1.PodSucceeded
			unselected := newExistingPod("other", "spot-node", true)
			unselected.Labels = map[string]string{"app": "other"}

			d := newDefaulter(
				newExistingPod("batch-0", "spot-node", true),
				newExistingPod("batch-1", "od-node", false),
				newExistingPod("batch-pending", "", true), // unscheduled but targeted for Spot
				succeeded, unselected,
				newNode("spot-node", true), newNode("od-node", false),
			)

			spot, total := d.countExistingWorkloadClassPods(testCtx, newIncomingPod(), wlc)
			Expect(spot).To(Equal(2))
			Expect(total).To(Equal(3))
		})

		It("detects fallback from the InFallback condition", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, []metav1.Condition{
				{Type: workloadsv1.ConditionTypeInFallback, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonFallbackActive},
			})
			Expect(newDefaulter().isWorkloadInFallback(testCtx, newIncomingPod(), wlc)).To(BeTrue())
		})

		It("detects fallback when a Spot-targeted Pod runs on an On-Demand node", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, nil)
			d := newDefaulter(newExistingPod("batch-0", "od-node", true), newNode("od-node", false))
			Expect(d.isWorkloadInFallback(testCtx, newIncomingPod(), wlc)).To(BeTrue())
		})

		It("does not report fallback when Spot-targeted Pods run on Spot nodes", func() {
			wlc := newWorkloadClass(&workloadsv1.CapacityStrategy{}, nil)
			d := newDefaulter(newExistingPod("batch-0", "spot-node", true), newNode("spot-node", true))
			Expect(d.isWorkloadInFallback(testCtx, newIncomingPod(), wlc)).To(BeFalse())
		})
	})
})
