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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// These specs cover how managed autoscalers follow their workload: when its best-match WorkloadClass changes, when
// the namespace enforces a WorkloadClass, for long workload names, for workloads scaled to zero, and when a
// workload is recreated before the garbage collector removed the autoscalers of its previous incarnation.
var _ = Describe("WorkloadAutoscaling Controller ownership", func() {
	var (
		namespace  string
		reconciler *WorkloadAutoscalingReconciler
	)

	BeforeEach(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "workload-ownership-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		namespace = ns.Name
		reconciler = &WorkloadAutoscalingReconciler{
			Client:       k8sClient,
			Scheme:       k8sClient.Scheme(),
			Recorder:     events.NewFakeRecorder(100),
			Workload:     DeploymentWorkload,
			Capabilities: utils.AutoscalerCapabilities{VPAInstalled: true, InPlaceOrRecreate: true},
		}
	})

	// createValidWorkloadClass creates a WorkloadClass with autoscaling between 2 and maxReplicas replicas and
	// rightsizing, and marks it as validated, standing in for the WorkloadClass controller. A nil selector leaves
	// PodSelector unset.
	createValidWorkloadClass := func(name string, selector map[string]string, maxReplicas int32) *workloadsv1.WorkloadClass {
		GinkgoHelper()
		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: workloadsv1.WorkloadClassSpec{
				ResourceManagement: &workloadsv1.ResourceManagement{
					Autoscaling: testAutoscaling(maxReplicas),
					Rightsizing: &workloadsv1.Rightsizing{},
				},
			},
		}
		if selector != nil {
			wc.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: selector}
		}
		Expect(k8sClient.Create(ctx, wc)).To(Succeed())
		meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type: workloadsv1.ConditionTypeResourceManagementValidated, Status: metav1.ConditionTrue,
			Reason: workloadsv1.ReasonValidationPassed, Message: "set by the test", ObservedGeneration: wc.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
		return wc
	}

	reconcileWorkload := func(name string) error {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Name: name, Namespace: namespace}})
		return err
	}

	managedKey := func(name string) client.ObjectKey {
		return client.ObjectKey{Name: utils.ManagedAutoscalerName("Deployment", name), Namespace: namespace}
	}
	getHPA := func(name string) *autoscalingv2.HorizontalPodAutoscaler {
		GinkgoHelper()
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		Expect(k8sClient.Get(ctx, managedKey(name), hpa)).To(Succeed())
		return hpa
	}
	getVPA := func(name string) *vpav1.VerticalPodAutoscaler {
		GinkgoHelper()
		vpa := &vpav1.VerticalPodAutoscaler{}
		Expect(k8sClient.Get(ctx, managedKey(name), vpa)).To(Succeed())
		return vpa
	}

	It("moves the managed autoscalers to the workload's new best-match WorkloadClass in place", func() {
		labels := map[string]string{"app": "web", "tier": "frontend"}
		createValidWorkloadClass("general", map[string]string{"app": "web"}, 10)
		deployment := newTestDeployment(namespace, "web", labels)
		Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
		Expect(reconcileWorkload("web")).To(Succeed())
		hpa := getHPA("web")
		Expect(hpa.Labels).To(HaveKeyWithValue(workloadsv1.WorkloadClassLabel, "general"))
		Expect(hpa.Spec.MaxReplicas).To(Equal(int32(10)))
		vpa := getVPA("web")

		By("creating a more specific WorkloadClass")
		specific := createValidWorkloadClass("specific", labels, 20)
		Expect(reconcileWorkload("web")).To(Succeed())
		moved := getHPA("web")
		Expect(moved.UID).To(Equal(hpa.UID), "the HorizontalPodAutoscaler is updated, not recreated")
		Expect(moved.Labels).To(HaveKeyWithValue(workloadsv1.WorkloadClassLabel, "specific"))
		Expect(moved.Spec.MaxReplicas).To(Equal(int32(20)))
		movedVPA := getVPA("web")
		Expect(movedVPA.UID).To(Equal(vpa.UID))
		Expect(movedVPA.Labels).To(HaveKeyWithValue(workloadsv1.WorkloadClassLabel, "specific"))

		By("deleting the more specific WorkloadClass")
		Expect(k8sClient.Delete(ctx, specific)).To(Succeed())
		Expect(reconcileWorkload("web")).To(Succeed())
		back := getHPA("web")
		Expect(back.UID).To(Equal(hpa.UID))
		Expect(back.Labels).To(HaveKeyWithValue(workloadsv1.WorkloadClassLabel, "general"))
		Expect(back.Spec.MaxReplicas).To(Equal(int32(10)))
	})

	It("follows the namespace's enforced WorkloadClass", func() {
		createValidWorkloadClass("web", map[string]string{"app": "web"}, 10)
		createValidWorkloadClass("enforced", nil, 30)
		Expect(k8sClient.Create(ctx, newTestDeployment(namespace, "web", map[string]string{"app": "web"}))).To(Succeed())
		Expect(reconcileWorkload("web")).To(Succeed())
		Expect(getHPA("web").Labels).To(HaveKeyWithValue(workloadsv1.WorkloadClassLabel, "web"))

		setEnforcedClass := func(class string) {
			GinkgoHelper()
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
			if ns.Labels == nil {
				ns.Labels = map[string]string{}
			}
			if class == "" {
				delete(ns.Labels, workloadsv1.EnforcedClassLabel)
			} else {
				ns.Labels[workloadsv1.EnforcedClassLabel] = class
			}
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())
		}

		By("enforcing a WorkloadClass on the namespace")
		setEnforcedClass("enforced")
		Expect(reconcileWorkload("web")).To(Succeed())
		hpa := getHPA("web")
		Expect(hpa.Labels).To(HaveKeyWithValue(workloadsv1.WorkloadClassLabel, "enforced"))
		Expect(hpa.Spec.MaxReplicas).To(Equal(int32(30)))

		By("removing the enforced WorkloadClass")
		setEnforcedClass("")
		Expect(reconcileWorkload("web")).To(Succeed())
		Expect(getHPA("web").Labels).To(HaveKeyWithValue(workloadsv1.WorkloadClassLabel, "web"))
	})

	It("manages the autoscalers of a workload whose name is too long to prefix", func() {
		wc := createValidWorkloadClass("web", map[string]string{"app": "web"}, 10)
		name := strings.Repeat("a", 240)
		Expect(k8sClient.Create(ctx, newTestDeployment(namespace, name, map[string]string{"app": "web"}))).To(Succeed())
		Expect(reconcileWorkload(name)).To(Succeed())

		hpa := getHPA(name)
		Expect(len(hpa.Name)).To(BeNumerically("<=", validation.DNS1123SubdomainMaxLength))
		Expect(hpa.Name).To(HavePrefix("wc-deployment-aaaa"))
		Expect(hpa.Spec.ScaleTargetRef.Name).To(Equal(name))
		vpa := getVPA(name)
		Expect(vpa.Name).To(Equal(hpa.Name))
		Expect(vpa.Spec.TargetRef.Name).To(Equal(name))

		By("reconciling again without changes")
		resourceVersion := hpa.ResourceVersion
		Expect(reconcileWorkload(name)).To(Succeed())
		Expect(getHPA(name).ResourceVersion).To(Equal(resourceVersion))

		By("invalidating the WorkloadClass")
		meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type: workloadsv1.ConditionTypeResourceManagementValidated, Status: metav1.ConditionFalse,
			Reason: workloadsv1.ReasonValidationFailed, ObservedGeneration: wc.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
		Expect(reconcileWorkload(name)).To(Succeed())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, managedKey(name), &autoscalingv2.HorizontalPodAutoscaler{}))).To(BeTrue())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, managedKey(name), &vpav1.VerticalPodAutoscaler{}))).To(BeTrue())
	})

	It("manages the autoscalers of a workload scaled to zero", func() {
		createValidWorkloadClass("web", map[string]string{"app": "web"}, 10)
		deployment := newTestDeployment(namespace, "web", map[string]string{"app": "web"})
		deployment.Spec.Replicas = ptr.To[int32](0)
		Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
		Expect(reconcileWorkload("web")).To(Succeed())
		Expect(getHPA("web").Spec.MinReplicas).To(HaveValue(Equal(int32(2))))
		getVPA("web")
	})

	It("takes over the autoscalers of a previous incarnation of the workload", func() {
		createValidWorkloadClass("web", map[string]string{"app": "web"}, 10)
		old := newTestDeployment(namespace, "web", map[string]string{"app": "web"})
		Expect(k8sClient.Create(ctx, old)).To(Succeed())
		Expect(reconcileWorkload("web")).To(Succeed())
		Expect(metav1.IsControlledBy(getHPA("web"), old)).To(BeTrue())

		By("recreating the Deployment before its autoscalers are garbage collected")
		Expect(k8sClient.Delete(ctx, old)).To(Succeed())
		recreated := newTestDeployment(namespace, "web", map[string]string{"app": "web"})
		Expect(k8sClient.Create(ctx, recreated)).To(Succeed())
		Expect(recreated.UID).NotTo(Equal(old.UID))
		Expect(reconcileWorkload("web")).To(Succeed())

		hpa := getHPA("web")
		Expect(metav1.IsControlledBy(hpa, recreated)).To(BeTrue())
		Expect(hpa.OwnerReferences).To(HaveLen(1))
		vpa := getVPA("web")
		Expect(metav1.IsControlledBy(vpa, recreated)).To(BeTrue())
		Expect(vpa.OwnerReferences).To(HaveLen(1))
	})

	It("refuses to take over a managed autoscaler controlled by another object", func() {
		createValidWorkloadClass("web", map[string]string{"app": "web"}, 10)
		other := newTestDeployment(namespace, "other", map[string]string{"app": "other"})
		Expect(k8sClient.Create(ctx, other)).To(Succeed())
		Expect(k8sClient.Create(ctx, newTestDeployment(namespace, "web", map[string]string{"app": "web"}))).To(Succeed())
		hpa := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      utils.ManagedAutoscalerName("Deployment", "web"),
				Namespace: namespace,
				Labels:    utils.ManagedAutoscalerLabels("web"),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "Deployment", Name: "other", UID: other.UID, Controller: ptr.To(true),
				}},
			},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"},
				MaxReplicas:    3,
			},
		}
		Expect(k8sClient.Create(ctx, hpa)).To(Succeed())

		Expect(reconcileWorkload("web")).To(MatchError(ContainSubstring("is controlled by Deployment other")))
		Expect(metav1.IsControlledBy(getHPA("web"), other)).To(BeTrue())
	})
})
