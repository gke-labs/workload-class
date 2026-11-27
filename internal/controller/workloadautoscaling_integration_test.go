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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// These specs run the WorkloadClass controller and the workload autoscaling controllers in a manager against
// a real API server, to cover what the unit tests with a fake client cannot: the watches, predicates and map
// functions that connect a WorkloadClass, its validation and its workloads.
//
// The manager's cache is restricted to one namespace, so that it never reconciles the objects of the other
// specs in this suite, which call the reconcilers directly.
var _ = Describe("Workload autoscaling controllers", Ordered, func() {
	const (
		namespace = "workload-autoscaling"
		timeout   = 30 * time.Second
		interval  = 250 * time.Millisecond
	)
	var stopManager context.CancelFunc

	podTemplate := func(labels map[string]string) corev1.PodTemplateSpec {
		return corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
		}
	}
	webLabels := map[string]string{"tier": "web"}

	getHPA := func(name string) (*autoscalingv2.HorizontalPodAutoscaler, error) {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, hpa)
		return hpa, err
	}
	expectHPA := func(name string, maxReplicas int32) *autoscalingv2.HorizontalPodAutoscaler {
		var hpa *autoscalingv2.HorizontalPodAutoscaler
		EventuallyWithOffset(1, func(g Gomega) {
			var err error
			hpa, err = getHPA(name)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(hpa.Spec.MaxReplicas).To(Equal(maxReplicas))
		}, timeout, interval).Should(Succeed())
		return hpa
	}
	expectNoHPA := func(name string) {
		EventuallyWithOffset(1, func() bool {
			_, err := getHPA(name)
			return apierrors.IsNotFound(err)
		}, timeout, interval).Should(BeTrue(), "expected HorizontalPodAutoscaler %s to be deleted", name)
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
			Controller: config.Controller{SkipNameValidation: ptr.To(true)},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect((&WorkloadClassReconciler{
			Client:   mgr.GetClient(),
			Scheme:   mgr.GetScheme(),
			Recorder: mgr.GetEventRecorder("workloadclass-controller"),
		}).SetupWithManager(mgr)).To(Succeed())
		for _, kind := range []string{utils.DeploymentKind, utils.StatefulSetKind} {
			Expect((&WorkloadAutoscalingReconciler{
				Client:   mgr.GetClient(),
				Scheme:   mgr.GetScheme(),
				Recorder: mgr.GetEventRecorder("workloadautoscaling-controller"),
				Kind:     kind,
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

	It("creates one HPA for each governed Deployment and StatefulSet", func() {
		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "web-services", Namespace: namespace},
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector: &metav1.LabelSelector{MatchLabels: webLabels},
				ResourceManagement: &workloadsv1.ResourceManagement{
					Autoscaling: &workloadsv1.Autoscaling{
						Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](2), Max: 20},
						Metrics: []autoscalingv2.MetricSpec{{
							Type: autoscalingv2.ResourceMetricSourceType,
							Resource: &autoscalingv2.ResourceMetricSource{
								Name:   corev1.ResourceCPU,
								Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](60)},
							},
						}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, wc)).To(Succeed())

		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: namespace},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: webLabels},
				Template: podTemplate(webLabels),
			},
		}
		Expect(k8sClient.Create(ctx, deploy)).To(Succeed())
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: namespace},
			Spec: appsv1.StatefulSetSpec{
				ServiceName: "web",
				Selector:    &metav1.LabelSelector{MatchLabels: webLabels},
				Template:    podTemplate(webLabels),
			},
		}
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		ungoverned := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: namespace},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "db"}},
				Template: podTemplate(map[string]string{"tier": "db"}),
			},
		}
		Expect(k8sClient.Create(ctx, ungoverned)).To(Succeed())

		hpa := expectHPA("wc-deployment-web", 20)
		Expect(hpa.Spec.ScaleTargetRef).To(Equal(autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"}))
		Expect(hpa.Spec.MinReplicas).To(HaveValue(Equal(int32(2))))
		Expect(metav1.IsControlledBy(hpa, deploy)).To(BeTrue(), "expected the Deployment to control its HPA")
		Expect(hpa.Labels).To(HaveKeyWithValue(utils.ManagedByLabel, utils.ManagedByValue))
		Expect(hpa.Labels).To(HaveKeyWithValue(utils.WorkloadClassLabel, "web-services"))

		stsHPA := expectHPA("wc-statefulset-web", 20)
		Expect(stsHPA.Spec.ScaleTargetRef.Kind).To(Equal("StatefulSet"))
		Expect(metav1.IsControlledBy(stsHPA, sts)).To(BeTrue(), "expected the StatefulSet to control its HPA")

		Consistently(func() bool {
			_, err := getHPA("wc-deployment-db")
			return apierrors.IsNotFound(err)
		}, 2*time.Second, interval).Should(BeTrue(), "expected no HPA for a Deployment that no WorkloadClass governs")
	})

	It("keeps the stored HPA equal to the desired one", func() {
		// The API server defaults HPA fields. If the builder left one of them unset, the controller would
		// update the HPA on every reconcile.
		before := expectHPA("wc-deployment-web", 20)
		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "web", Namespace: namespace}, deploy)).To(Succeed())
		r := &WorkloadAutoscalingReconciler{Client: k8sClient, Scheme: scheme.Scheme, Recorder: events.NewFakeRecorder(10), Kind: utils.DeploymentKind}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(deploy)})
		Expect(err).NotTo(HaveOccurred())
		after, err := getHPA("wc-deployment-web")
		Expect(err).NotTo(HaveOccurred())
		Expect(after.ResourceVersion).To(Equal(before.ResourceVersion))
	})

	It("updates the HPAs when the WorkloadClass changes", func() {
		wc := &workloadsv1.WorkloadClass{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "web-services", Namespace: namespace}, wc)).To(Succeed())
		wc.Spec.ResourceManagement.Autoscaling.Replicas.Max = 50
		Expect(k8sClient.Update(ctx, wc)).To(Succeed())

		expectHPA("wc-deployment-web", 50)
		expectHPA("wc-statefulset-web", 50)
	})

	It("restores a managed HPA that is edited or deleted", func() {
		hpa := expectHPA("wc-deployment-web", 50)
		hpa.Spec.MaxReplicas = 999
		Expect(k8sClient.Update(ctx, hpa)).To(Succeed())
		expectHPA("wc-deployment-web", 50)

		Expect(k8sClient.Delete(ctx, hpa)).To(Succeed())
		restored := expectHPA("wc-deployment-web", 50)
		Expect(restored.UID).NotTo(Equal(hpa.UID))
	})

	It("yields to a user-created HPA and reclaims the workload when it is gone", func() {
		user := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "user-hpa", Namespace: namespace},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"},
				MaxReplicas:    3,
			},
		}
		Expect(k8sClient.Create(ctx, user)).To(Succeed())
		expectNoHPA("wc-deployment-web")
		// The StatefulSet with the same name is not affected.
		expectHPA("wc-statefulset-web", 50)

		Expect(k8sClient.Delete(ctx, user)).To(Succeed())
		expectHPA("wc-deployment-web", 50)
	})

	It("moves a workload between WorkloadClasses when its pod template labels change", func() {
		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "db", Namespace: namespace}, deploy)).To(Succeed())
		// Deployment selectors are immutable, so add the label that the WorkloadClass selects.
		deploy.Spec.Template.Labels = map[string]string{"tier": "db", "scaled": "true"}
		Expect(k8sClient.Update(ctx, deploy)).To(Succeed())

		scaled := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "scaled", Namespace: namespace},
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"scaled": "true"}},
				ResourceManagement: &workloadsv1.ResourceManagement{
					Autoscaling: &workloadsv1.Autoscaling{
						Replicas: workloadsv1.ReplicaBounds{Max: 7},
						Metrics: []autoscalingv2.MetricSpec{{
							Type: autoscalingv2.ResourceMetricSourceType,
							Resource: &autoscalingv2.ResourceMetricSource{
								Name:   corev1.ResourceMemory,
								Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](70)},
							},
						}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, scaled)).To(Succeed())
		hpa := expectHPA("wc-deployment-db", 7)
		Expect(hpa.Labels).To(HaveKeyWithValue(utils.WorkloadClassLabel, "scaled"))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(deploy), deploy)).To(Succeed())
		deploy.Spec.Template.Labels = map[string]string{"tier": "db"}
		Expect(k8sClient.Update(ctx, deploy)).To(Succeed())
		expectNoHPA("wc-deployment-db")
	})

	It("applies the namespace's enforced WorkloadClass to every workload", func() {
		// At this point, no PodSelector matches the Deployment "db".
		expectNoHPA("wc-deployment-db")

		ns := &corev1.Namespace{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		ns.Labels[workloadsv1.EnforcedClassLabel] = "web-services"
		Expect(k8sClient.Update(ctx, ns)).To(Succeed())
		hpa := expectHPA("wc-deployment-db", 50)
		Expect(hpa.Labels).To(HaveKeyWithValue(utils.WorkloadClassLabel, "web-services"))

		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
		delete(ns.Labels, workloadsv1.EnforcedClassLabel)
		Expect(k8sClient.Update(ctx, ns)).To(Succeed())
		expectNoHPA("wc-deployment-db")
	})

	It("deletes the HPAs when the WorkloadClass is deleted", func() {
		wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "web-services", Namespace: namespace}}
		Expect(k8sClient.Delete(ctx, wc)).To(Succeed())

		expectNoHPA("wc-deployment-web")
		expectNoHPA("wc-statefulset-web")
	})
})
