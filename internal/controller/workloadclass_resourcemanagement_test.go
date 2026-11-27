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
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

var _ = Describe("WorkloadClass Controller resourceManagement validation", func() {
	const (
		namespace = "default"
		name      = "rm-validation"
	)
	key := client.ObjectKey{Name: name, Namespace: namespace}

	var reconciler *WorkloadClassReconciler

	reconcileAndGet := func() *workloadsv1.WorkloadClass {
		GinkgoHelper()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		wc := &workloadsv1.WorkloadClass{}
		Expect(k8sClient.Get(ctx, key, wc)).To(Succeed())
		return wc
	}

	BeforeEach(func() {
		reconciler = &WorkloadClassReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(100),
		}

		guardrail := &workloadsv1.WorkloadClassGuardrail{
			ObjectMeta: metav1.ObjectMeta{Name: "rm-validation-guardrail"},
			Spec: workloadsv1.WorkloadClassGuardrailSpec{
				Constraints: workloadsv1.Constraints{
					Disruption:         workloadsv1.Disruption{MaxNonDisruptionDurationDays: 30},
					ResourceManagement: &workloadsv1.ResourceManagementConstraints{MaxReplicas: ptr.To[int32](10)},
				},
			},
		}
		Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, guardrail))).To(Succeed()) })

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"app": name}},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pod))).To(Succeed()) })

		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector:      &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
				DisruptionPolicy: workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 10},
				ResourceManagement: &workloadsv1.ResourceManagement{
					Autoscaling: &workloadsv1.Autoscaling{
						Replicas: workloadsv1.ReplicaBounds{Max: 20},
						Metrics:  []autoscalingv2.MetricSpec{cpuUtilizationMetric()},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, wc)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, wc))).To(Succeed())
			pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(name), Namespace: namespace}}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pdb))).To(Succeed())
		})
	})

	It("reports guardrail violations on ResourceManagementValidated without affecting disruption protection", func() {
		By("reconciling a WorkloadClass whose replicas.max exceeds the guardrail maxReplicas")
		wc := reconcileAndGet()
		rmCond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated)
		Expect(rmCond).NotTo(BeNil())
		Expect(rmCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(rmCond.Reason).To(Equal(workloadsv1.ReasonValidationFailed))
		Expect(rmCond.Message).To(ContainSubstring("`resourceManagement.autoscaling.replicas.max` must be at most 10 (guardrail limit), got 20"))
		Expect(rmCond.ObservedGeneration).To(Equal(wc.Generation))
		Expect(meta.IsStatusConditionFalse(wc.Status.Conditions, workloadsv1.ConditionTypeValidated)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeDisruptionValidated)).To(BeTrue())

		By("checking that the governed Pod still gets its Per-Pod PDB")
		pdb := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: utils.PDBName(name), Namespace: namespace}, pdb)).To(Succeed())

		By("lowering replicas.max to the guardrail limit")
		wc.Spec.ResourceManagement.Autoscaling.Replicas.Max = 10
		Expect(k8sClient.Update(ctx, wc)).To(Succeed())
		wc = reconcileAndGet()
		rmCond = meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated)
		Expect(rmCond).NotTo(BeNil())
		Expect(rmCond.Status).To(Equal(metav1.ConditionTrue))
		Expect(rmCond.Reason).To(Equal(workloadsv1.ReasonValidationPassed))
		Expect(rmCond.ObservedGeneration).To(Equal(wc.Generation))
		Expect(meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeValidated)).To(BeTrue())

		By("removing resourceManagement")
		wc.Spec.ResourceManagement = nil
		Expect(k8sClient.Update(ctx, wc)).To(Succeed())
		wc = reconcileAndGet()
		Expect(meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated)).To(BeNil())
		Expect(meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeValidated)).To(BeTrue())
	})
})
