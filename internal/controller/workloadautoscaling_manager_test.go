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
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// This spec runs the WorkloadClass and workload autoscaling controllers in a manager, against a second control plane
// that, unlike the suite's, has no VerticalPodAutoscaler CRD. It checks that the manager starts, that autoscaling
// works, and that the WorkloadClass reports that rightsizing is not applied.
var _ = Describe("WorkloadAutoscaling Controller without the VPA", Ordered, func() {
	var (
		env       *envtest.Environment
		c         client.Client
		cancelMgr context.CancelFunc
		mgrDone   chan struct{}
	)

	BeforeAll(func() {
		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(workloadsv1.AddToScheme(scheme)).To(Succeed())
		Expect(vpav1.AddToScheme(scheme)).To(Succeed())
		Expect(apiextensionsv1.AddToScheme(scheme)).To(Succeed())

		env = &envtest.Environment{
			CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
			ErrorIfCRDPathMissing: true,
		}
		if dir := getFirstFoundEnvTestBinaryDir(); dir != "" {
			env.BinaryAssetsDirectory = dir
		}
		cfg, err := env.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Eventually(env.Stop, time.Minute, time.Second).Should(Succeed())
		})

		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 scheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			// The suite's specs may already have registered controllers with the same names in this process.
			Controller: config.Controller{SkipNameValidation: ptr.To(true)},
		})
		Expect(err).NotTo(HaveOccurred())

		var caps utils.AutoscalerCapabilities
		Expect(DetectVPA(ctx, mgr.GetRESTMapper(), mgr.GetAPIReader(), &caps)).To(Succeed())
		Expect(caps.VPAInstalled).To(BeFalse())

		Expect((&WorkloadClassReconciler{
			Client:       mgr.GetClient(),
			APIReader:    mgr.GetAPIReader(),
			Scheme:       mgr.GetScheme(),
			Recorder:     mgr.GetEventRecorder("workloadclass-controller"),
			VPAInstalled: caps.VPAInstalled,
		}).SetupWithManager(mgr)).To(Succeed())
		Expect(SetupWorkloadAutoscalingControllers(mgr, caps)).To(Succeed())

		var mgrCtx context.Context
		mgrCtx, cancelMgr = context.WithCancel(ctx)
		mgrDone = make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(mgrDone)
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
		DeferCleanup(func() {
			cancelMgr()
			Eventually(mgrDone, time.Minute).Should(BeClosed())
		})

		c, err = client.New(cfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())
	})

	It("autoscales governed workloads and reports that rightsizing is not applied", func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "no-vpa"}}
		Expect(c.Create(ctx, ns)).To(Succeed())
		labels := map[string]string{"app": "web"}
		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns.Name},
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector: &metav1.LabelSelector{MatchLabels: labels},
				ResourceManagement: &workloadsv1.ResourceManagement{
					Autoscaling: testAutoscaling(10),
					Rightsizing: &workloadsv1.Rightsizing{},
				},
			},
		}
		Expect(c.Create(ctx, wc)).To(Succeed())
		Expect(c.Create(ctx, newTestDeployment(ns.Name, "web", labels))).To(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(c.Get(ctx, client.ObjectKeyFromObject(wc), wc)).To(Succeed())
			cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(cond.Message).To(ContainSubstring("VPA is not installed; rightsizing is not applied"))
		}, 30*time.Second, 100*time.Millisecond).Should(Succeed())

		Eventually(func(g Gomega) {
			hpa := &autoscalingv2.HorizontalPodAutoscaler{}
			key := client.ObjectKey{Name: utils.ManagedAutoscalerName("Deployment", "web"), Namespace: ns.Name}
			g.Expect(c.Get(ctx, key, hpa)).To(Succeed())
			g.Expect(hpa.Spec.MaxReplicas).To(Equal(int32(10)))
		}, 30*time.Second, 100*time.Millisecond).Should(Succeed())
	})
})
