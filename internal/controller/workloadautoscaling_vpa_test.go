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

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

var _ = Describe("WorkloadAutoscaling Controller rightsizing", func() {
	var (
		namespace string
		recorder  *events.FakeRecorder
		webLabels = map[string]string{"app": "web"}
	)

	BeforeEach(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "workload-rightsizing-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		namespace = ns.Name
		recorder = events.NewFakeRecorder(100)
	})

	newReconciler := func(caps utils.AutoscalerCapabilities) *WorkloadAutoscalingReconciler {
		return &WorkloadAutoscalingReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder, Workload: DeploymentWorkload, Capabilities: caps}
	}
	vpaCaps := utils.AutoscalerCapabilities{VPAInstalled: true, InPlaceOrRecreate: true}

	// createValidWorkloadClass creates a WorkloadClass that selects app=web Pods and marks its resourceManagement as
	// validated, standing in for the WorkloadClass controller.
	createValidWorkloadClass := func(rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
		GinkgoHelper()
		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: namespace},
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector:        &metav1.LabelSelector{MatchLabels: webLabels},
				ResourceManagement: rm,
			},
		}
		Expect(k8sClient.Create(ctx, wc)).To(Succeed())
		meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type: workloadsv1.ConditionTypeResourceManagementValidated, Status: metav1.ConditionTrue,
			Reason: workloadsv1.ReasonValidationPassed, Message: "set by the test", ObservedGeneration: wc.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
		return wc
	}

	createDeployment := func() *corev1.ObjectReference {
		GinkgoHelper()
		deployment := newTestDeployment(namespace, "web", webLabels)
		Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
		return &corev1.ObjectReference{Name: deployment.Name, Namespace: deployment.Namespace, UID: deployment.UID}
	}

	reconcileWeb := func(r *WorkloadAutoscalingReconciler) {
		GinkgoHelper()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Name: "web", Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
	}

	vpaKey := func() client.ObjectKey {
		return client.ObjectKey{Name: utils.ManagedAutoscalerName("Deployment", "web"), Namespace: namespace}
	}
	getVPA := func() *vpav1.VerticalPodAutoscaler {
		GinkgoHelper()
		vpa := &vpav1.VerticalPodAutoscaler{}
		Expect(k8sClient.Get(ctx, vpaKey(), vpa)).To(Succeed())
		return vpa
	}
	expectNoVPA := func() {
		GinkgoHelper()
		err := k8sClient.Get(ctx, vpaKey(), &vpav1.VerticalPodAutoscaler{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected no VerticalPodAutoscaler, got %v", err)
	}

	DescribeTable("creates the VerticalPodAutoscaler for each rightsizing mode",
		func(rightsizing workloadsv1.Rightsizing, caps utils.AutoscalerCapabilities, wantMode vpav1.UpdateMode, wantEvent string) {
			createValidWorkloadClass(&workloadsv1.ResourceManagement{Rightsizing: &rightsizing})
			deployment := createDeployment()
			r := newReconciler(caps)
			reconcileWeb(r)

			vpa := getVPA()
			Expect(vpa.Spec.UpdatePolicy).NotTo(BeNil())
			Expect(vpa.Spec.UpdatePolicy.UpdateMode).To(HaveValue(Equal(wantMode)))
			Expect(vpa.Spec.TargetRef).To(Equal(&autoscalingv1.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"}))
			Expect(vpa.Labels).To(HaveKeyWithValue(workloadsv1.ManagedByLabel, workloadsv1.ManagedByLabelValue))
			Expect(vpa.Labels).To(HaveKeyWithValue(workloadsv1.WorkloadClassLabel, "web"))
			controllerRef := metav1.GetControllerOf(vpa)
			Expect(controllerRef).NotTo(BeNil())
			Expect(controllerRef.UID).To(Equal(deployment.UID))
			if wantEvent != "" {
				Expect(recorder.Events).To(Receive(ContainSubstring(wantEvent)))
			}

			By("reconciling again without changes")
			resourceVersion := vpa.ResourceVersion
			reconcileWeb(r)
			Expect(getVPA().ResourceVersion).To(Equal(resourceVersion))
		},
		Entry("Recommend (default)", workloadsv1.Rightsizing{}, vpaCaps, vpav1.UpdateModeOff, ""),
		Entry("OnCreate", workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOnCreate}, vpaCaps, vpav1.UpdateModeInitial, ""),
		Entry("Auto with InPlacePreferred", workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto}, vpaCaps,
			vpav1.UpdateModeInPlaceOrRecreate, ""),
		Entry("Auto with InPlacePreferred on an older VPA", workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto},
			utils.AutoscalerCapabilities{VPAInstalled: true}, vpav1.UpdateModeRecreate, EventReasonInPlaceUnsupported),
		Entry("Auto with Recreate", workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto, UpdateStrategy: workloadsv1.RightsizingUpdateStrategyRecreate},
			vpaCaps, vpav1.UpdateModeRecreate, ""),
	)

	It("creates no VerticalPodAutoscaler in Off mode, and deletes the managed one when the mode changes to Off", func() {
		wc := createValidWorkloadClass(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{}})
		createDeployment()
		r := newReconciler(vpaCaps)
		reconcileWeb(r)
		getVPA()

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wc), wc)).To(Succeed())
		wc.Spec.ResourceManagement.Rightsizing.Mode = workloadsv1.RightsizingModeOff
		Expect(k8sClient.Update(ctx, wc)).To(Succeed())
		meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type: workloadsv1.ConditionTypeResourceManagementValidated, Status: metav1.ConditionTrue,
			Reason: workloadsv1.ReasonValidationPassed, ObservedGeneration: wc.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
		reconcileWeb(r)
		expectNoVPA()
	})

	It("leaves out of rightsizing the resources whose utilization autoscaling targets, unless the platform coordinates them", func() {
		createValidWorkloadClass(&workloadsv1.ResourceManagement{
			Autoscaling: testAutoscaling(10),
			Rightsizing: &workloadsv1.Rightsizing{},
		})
		createDeployment()

		reconcileWeb(newReconciler(vpaCaps))
		policy := getVPA().Spec.ResourcePolicy.ContainerPolicies[0]
		Expect(*policy.ControlledResources).To(Equal([]corev1.ResourceName{corev1.ResourceMemory}))
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		Expect(k8sClient.Get(ctx, vpaKey(), hpa)).To(Succeed(), "the HorizontalPodAutoscaler is created alongside")

		coordinated := vpaCaps
		coordinated.CoordinatedResources = []corev1.ResourceName{corev1.ResourceCPU}
		reconcileWeb(newReconciler(coordinated))
		policy = getVPA().Spec.ResourcePolicy.ContainerPolicies[0]
		Expect(*policy.ControlledResources).To(Equal([]corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory}))
	})

	It("creates no VerticalPodAutoscaler when no resource is left to rightsize", func() {
		createValidWorkloadClass(&workloadsv1.ResourceManagement{
			Autoscaling: testAutoscaling(10),
			Rightsizing: &workloadsv1.Rightsizing{Resources: []workloadsv1.RightsizedResource{workloadsv1.RightsizedResourceCPU}},
		})
		createDeployment()
		reconcileWeb(newReconciler(vpaCaps))
		expectNoVPA()
		Expect(recorder.Events).To(Receive(ContainSubstring(EventReasonRightsizingSkipped)))
	})

	It("applies the guardrail containerBounds when the WorkloadClass sets no bounds", func() {
		guardrail := &workloadsv1.WorkloadClassGuardrail{
			ObjectMeta: metav1.ObjectMeta{Name: "rightsizing-bounds"},
			Spec: workloadsv1.WorkloadClassGuardrailSpec{
				Constraints: workloadsv1.Constraints{
					Disruption: workloadsv1.Disruption{MaxNonDisruptionDurationDays: 30},
					ResourceManagement: &workloadsv1.ResourceManagementConstraints{
						ContainerBounds: &workloadsv1.ContainerBounds{
							Min: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m")},
							Max: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, guardrail))).To(Succeed()) })
		createValidWorkloadClass(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{}})
		createDeployment()

		reconcileWeb(newReconciler(vpaCaps))
		policy := getVPA().Spec.ResourcePolicy.ContainerPolicies[0]
		Expect(policy.MinAllowed.Cpu().String()).To(Equal("50m"))
		Expect(policy.MaxAllowed.Memory().String()).To(Equal("2Gi"))
	})

	It("steps back for a user-created VerticalPodAutoscaler", func() {
		createValidWorkloadClass(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{}})
		createDeployment()
		r := newReconciler(vpaCaps)
		reconcileWeb(r)
		getVPA()

		off := vpav1.UpdateModeOff
		userVPA := &vpav1.VerticalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "my-vpa", Namespace: namespace},
			Spec: vpav1.VerticalPodAutoscalerSpec{
				TargetRef:    &autoscalingv1.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"},
				UpdatePolicy: &vpav1.PodUpdatePolicy{UpdateMode: &off},
			},
		}
		Expect(k8sClient.Create(ctx, userVPA)).To(Succeed())
		reconcileWeb(r)
		expectNoVPA()
		Expect(recorder.Events).To(Receive(ContainSubstring(EventReasonUserAutoscalerPresent)))

		Expect(k8sClient.Delete(ctx, userVPA)).To(Succeed())
		reconcileWeb(r)
		getVPA()
	})

	It("deletes the managed VerticalPodAutoscaler when the WorkloadClass is invalid", func() {
		wc := createValidWorkloadClass(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{}})
		createDeployment()
		r := newReconciler(vpaCaps)
		reconcileWeb(r)
		getVPA()

		meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type: workloadsv1.ConditionTypeResourceManagementValidated, Status: metav1.ConditionFalse,
			Reason: workloadsv1.ReasonValidationFailed, ObservedGeneration: wc.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
		reconcileWeb(r)
		expectNoVPA()
	})

	It("leaves VerticalPodAutoscalers alone when the VPA is not installed", func() {
		createValidWorkloadClass(&workloadsv1.ResourceManagement{
			Autoscaling: testAutoscaling(10),
			Rightsizing: &workloadsv1.Rightsizing{},
		})
		createDeployment()
		reconcileWeb(newReconciler(utils.AutoscalerCapabilities{}))
		expectNoVPA()
		Expect(k8sClient.Get(ctx, vpaKey(), &autoscalingv2.HorizontalPodAutoscaler{})).To(Succeed(), "autoscaling still works")
	})
})
