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
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// These specs run the controllers in a manager against a real API server that serves the upstream
// VerticalPodAutoscaler CRD (test/testdata/crds). They cover what the unit tests with a fake client cannot:
// that the VPA objects the controller builds are accepted by the real VPA schema, that the stored objects
// compare equal to the desired ones, and that VPAs are detected, cached and watched as unstructured objects.
//
// No VPA components run here, so recommendations are never produced or applied.
var _ = Describe("Workload rightsizing controllers", Ordered, func() {
	const (
		namespace = "workload-rightsizing"
		className = "web-services"
		vpaName   = "wc-deployment-web"
		timeout   = 30 * time.Second
		interval  = 250 * time.Millisecond
	)
	var stopManager context.CancelFunc
	webLabels := map[string]string{"tier": "web"}

	getVPA := func(name string) (*unstructured.Unstructured, error) {
		vpa := newVPA()
		err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, vpa)
		return vpa, err
	}
	// expectVPA waits for the VPA to exist with the update mode, and returns it.
	expectVPA := func(name, updateMode string) *unstructured.Unstructured {
		var vpa *unstructured.Unstructured
		EventuallyWithOffset(1, func(g Gomega) {
			var err error
			vpa, err = getVPA(name)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(utils.VPAUpdateModeOf(vpa)).To(Equal(updateMode))
		}, timeout, interval).Should(Succeed())
		return vpa
	}
	expectNoVPA := func(name string) {
		EventuallyWithOffset(1, func() bool {
			_, err := getVPA(name)
			return apierrors.IsNotFound(err)
		}, timeout, interval).Should(BeTrue(), "expected VerticalPodAutoscaler %s to be deleted", name)
	}
	containerPolicies := func(vpa *unstructured.Unstructured) []any {
		policies, _, err := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		return policies
	}
	updateClass := func(mutate func(wc *workloadsv1.WorkloadClass)) {
		EventuallyWithOffset(1, func() error {
			wc := &workloadsv1.WorkloadClass{}
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: className, Namespace: namespace}, wc); err != nil {
				return err
			}
			mutate(wc)
			return k8sClient.Update(ctx, wc)
		}, timeout, interval).Should(Succeed())
	}

	BeforeAll(func() {
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 scheme.Scheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			// As in cmd/main.go, the cached Deployments and StatefulSets are trimmed.
			Cache: cache.Options{
				DefaultNamespaces: map[string]cache.Config{namespace: {}},
				ByObject:          WorkloadCacheByObject(),
			},
			// As in cmd/main.go: read the unstructured VPAs from the cache.
			Client:     client.Options{Cache: &client.CacheOptions{Unstructured: true}},
			Controller: config.Controller{SkipNameValidation: ptr.To(true)},
		})
		Expect(err).NotTo(HaveOccurred())

		vpaInstalled, err := IsVPAInstalled(mgr.GetRESTMapper())
		Expect(err).NotTo(HaveOccurred())
		Expect(vpaInstalled).To(BeTrue(), "expected the VerticalPodAutoscaler CRD of test/testdata/crds to be detected")

		Expect((&WorkloadClassReconciler{
			Client:       mgr.GetClient(),
			Scheme:       mgr.GetScheme(),
			Recorder:     mgr.GetEventRecorder("workloadclass-controller"),
			VPAInstalled: vpaInstalled,
		}).SetupWithManager(mgr)).To(Succeed())
		for _, kind := range []string{utils.DeploymentKind, utils.StatefulSetKind} {
			Expect((&WorkloadAutoscalingReconciler{
				Client:       mgr.GetClient(),
				Scheme:       mgr.GetScheme(),
				Recorder:     mgr.GetEventRecorder("workloadautoscaling-controller"),
				Kind:         kind,
				VPAInstalled: vpaInstalled,
			}).SetupWithManager(mgr)).To(Succeed())
		}

		var mgrCtx context.Context
		mgrCtx, stopManager = context.WithCancel(ctx)
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()

		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	AfterAll(func() {
		stopManager()
	})

	It("creates a VPA that the VerticalPodAutoscaler CRD accepts", func() {
		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: className, Namespace: namespace},
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector: &metav1.LabelSelector{MatchLabels: webLabels},
				ResourceManagement: &workloadsv1.ResourceManagement{
					Rightsizing: &workloadsv1.Rightsizing{
						Mode:              workloadsv1.RightsizingModeAuto,
						ExcludeContainers: []string{"istio-proxy"},
						Bounds: &workloadsv1.ContainerBounds{
							Min: rmResources("100m", "128Mi"),
							Max: rmResources("4", "8Gi"),
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, wc)).To(Succeed())
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: namespace},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: webLabels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: webLabels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, deploy)).To(Succeed())

		// InPlaceOrRecreate is the update mode for the Auto mode with the default InPlacePreferred strategy.
		vpa := expectVPA(vpaName, utils.VPAUpdateModeInPlaceOrRecreate)
		Expect(utils.VPATargetsWorkload(vpa, utils.WorkloadRef{Kind: utils.DeploymentKind, Name: "web"})).To(BeTrue())
		Expect(containerPolicies(vpa)).To(Equal([]any{
			map[string]any{
				"containerName":       "*",
				"controlledResources": []any{"cpu", "memory"},
				"minAllowed":          map[string]any{"cpu": "100m", "memory": "128Mi"},
				"maxAllowed":          map[string]any{"cpu": "4", "memory": "8Gi"},
			},
			map[string]any{"containerName": "istio-proxy", "mode": "Off"},
		}))
		Expect(metav1.IsControlledBy(vpa, deploy)).To(BeTrue(), "expected the Deployment to control its VPA")
		Expect(vpa.GetLabels()).To(HaveKeyWithValue(utils.ManagedByLabel, utils.ManagedByValue))
		Expect(vpa.GetLabels()).To(HaveKeyWithValue(utils.WorkloadClassLabel, className))

		// The WorkloadClass is valid and does not report a missing VPA installation.
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wc), wc)).To(Succeed())
			cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(cond.Message).NotTo(ContainSubstring(vpaNotInstalledMsg))
		}, timeout, interval).Should(Succeed())
	})

	It("keeps the stored VPA equal to the desired one", func() {
		// If the API server changed the VPA that the controller wrote, every reconcile would update it again.
		before := expectVPA(vpaName, utils.VPAUpdateModeInPlaceOrRecreate)
		r := &WorkloadAutoscalingReconciler{
			Client: k8sClient, Scheme: scheme.Scheme, Recorder: events.NewFakeRecorder(10),
			Kind: utils.DeploymentKind, VPAInstalled: true,
		}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Name: "web", Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		after, err := getVPA(vpaName)
		Expect(err).NotTo(HaveOccurred())
		Expect(after.GetResourceVersion()).To(Equal(before.GetResourceVersion()))
	})

	It("follows the rightsizing mode of the WorkloadClass", func() {
		updateClass(func(wc *workloadsv1.WorkloadClass) {
			wc.Spec.ResourceManagement.Rightsizing.UpdateStrategy = workloadsv1.RightsizingUpdateStrategyRecreate
		})
		expectVPA(vpaName, utils.VPAUpdateModeRecreate)

		updateClass(func(wc *workloadsv1.WorkloadClass) {
			wc.Spec.ResourceManagement.Rightsizing.Mode = workloadsv1.RightsizingModeOnCreate
		})
		expectVPA(vpaName, utils.VPAUpdateModeInitial)

		updateClass(func(wc *workloadsv1.WorkloadClass) {
			wc.Spec.ResourceManagement.Rightsizing.Mode = workloadsv1.RightsizingModeRecommend
		})
		expectVPA(vpaName, utils.VPAUpdateModeOff)

		updateClass(func(wc *workloadsv1.WorkloadClass) {
			wc.Spec.ResourceManagement.Rightsizing.Mode = workloadsv1.RightsizingModeOff
		})
		expectNoVPA(vpaName)
	})

	It("uses the guardrail containerBounds when the WorkloadClass sets none", func() {
		updateClass(func(wc *workloadsv1.WorkloadClass) {
			wc.Spec.ResourceManagement.Rightsizing = &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeRecommend}
		})
		vpa := expectVPA(vpaName, utils.VPAUpdateModeOff)
		Expect(containerPolicies(vpa)).To(Equal([]any{
			map[string]any{"containerName": "*", "controlledResources": []any{"cpu", "memory"}},
		}))

		guardrail := &workloadsv1.WorkloadClassGuardrail{
			ObjectMeta: metav1.ObjectMeta{Name: "rightsizing-bounds"},
			Spec: workloadsv1.WorkloadClassGuardrailSpec{Constraints: workloadsv1.Constraints{
				Disruption: workloadsv1.Disruption{MaxNonDisruptionDurationDays: 30},
				ResourceManagement: &workloadsv1.ResourceManagementConstraints{
					ContainerBounds: &workloadsv1.ContainerBounds{Min: rmResources("10m", "32Mi"), Max: rmResources("16", "64Gi")},
				},
			}},
		}
		Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, guardrail))).To(Succeed())
		})

		// A guardrail change re-queues the workloads, so the VPA picks up the new bounds.
		Eventually(func(g Gomega) {
			vpa, err := getVPA(vpaName)
			g.Expect(err).NotTo(HaveOccurred())
			policies, _, err := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(policies).To(Equal([]any{map[string]any{
				"containerName":       "*",
				"controlledResources": []any{"cpu", "memory"},
				"minAllowed":          map[string]any{"cpu": "10m", "memory": "32Mi"},
				"maxAllowed":          map[string]any{"cpu": "16", "memory": "64Gi"},
			}}))
		}, timeout, interval).Should(Succeed())
	})

	It("leaves a resource to the HPA when autoscaling scales on its utilization", func() {
		updateClass(func(wc *workloadsv1.WorkloadClass) {
			wc.Spec.ResourceManagement.Autoscaling = &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Max: 10},
				Metrics: []autoscalingv2.MetricSpec{{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name:   corev1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](60)},
					},
				}},
			}
		})

		Eventually(func(g Gomega) {
			hpa := &autoscalingv2.HorizontalPodAutoscaler{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: vpaName, Namespace: namespace}, hpa)).To(Succeed())
			vpa, err := getVPA(vpaName)
			g.Expect(err).NotTo(HaveOccurred())
			policies, _, err := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(policies).To(HaveLen(1))
			g.Expect(policies[0]).To(HaveKeyWithValue("controlledResources", []any{"memory"}))
		}, timeout, interval).Should(Succeed())
	})

	It("yields to a user-created VPA and reclaims the workload when it is gone", func() {
		user := newVPA()
		user.SetName("user-vpa")
		user.SetNamespace(namespace)
		Expect(unstructured.SetNestedMap(user.Object, map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"}, "spec", "targetRef")).To(Succeed())
		Expect(k8sClient.Create(ctx, user)).To(Succeed())
		expectNoVPA(vpaName)

		Expect(k8sClient.Delete(ctx, user)).To(Succeed())
		expectVPA(vpaName, utils.VPAUpdateModeOff)
	})

	It("restores a managed VPA that is edited", func() {
		vpa := expectVPA(vpaName, utils.VPAUpdateModeOff)
		Expect(unstructured.SetNestedField(vpa.Object, utils.VPAUpdateModeRecreate, "spec", "updatePolicy", "updateMode")).To(Succeed())
		Expect(k8sClient.Update(ctx, vpa)).To(Succeed())
		expectVPA(vpaName, utils.VPAUpdateModeOff)
	})

	It("deletes the VPA when the WorkloadClass is deleted", func() {
		wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: className, Namespace: namespace}}
		Expect(k8sClient.Delete(ctx, wc)).To(Succeed())
		expectNoVPA(vpaName)
	})
})
