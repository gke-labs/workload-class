//go:build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/test/utils"
)

// These specs exercise resource management end to end on a Kind cluster, with the manager deployed from its
// manifests and running with its real RBAC:
//   - initial resources are filled in on Pods by the Pod webhook;
//   - one HorizontalPodAutoscaler is created per governed workload, and it scales on CPU utilization from the
//     first Pod, because initial resources gave the Pod a request;
//   - one VerticalPodAutoscaler is created per governed workload.
//
// metrics-server is installed so that the HPA can read CPU usage. Only the VerticalPodAutoscaler CRDs are
// installed, not the VPA components: the specs check the VPA objects that the controller writes, but no
// recommendation is ever produced or applied. Rightsizing that resizes or recreates Pods is not covered here.
//
// The object names, labels and annotations are hardcoded (rather than imported from the controller) so that
// these tests assert the external contract.
const (
	rmNS           = "resource-management-e2e"
	rmLimitRangeNS = "resource-management-limitrange-e2e"

	rmInitialResourcesAnnotation = "workloads.gke.io/initial-resources"
	rmManagedByLabel             = "workloads.gke.io/managed-by"
	rmManagedByValue             = "workload-class"
	rmWorkloadClassLabel         = "workloads.gke.io/workload-class"
	rmValidatedCondition         = "ResourceManagementValidated"

	rmVPACRDs     = "test/testdata/crds/vpa-v1-crd-gen.yaml"
	rmVPAResource = "verticalpodautoscalers.autoscaling.k8s.io"

	rmMetricsServerManifest = "https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml"
	// rmBusyboxImage runs the CPU load of the autoscaling spec.
	rmBusyboxImage = "registry.k8s.io/e2e-test-images/busybox:1.29-4"

	rmTimeout  = 3 * time.Minute
	rmInterval = 3 * time.Second
)

func rmQuantities(cpu, memory string) corev1.ResourceList {
	rl := corev1.ResourceList{}
	if cpu != "" {
		rl[corev1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		rl[corev1.ResourceMemory] = resource.MustParse(memory)
	}
	return rl
}

// newRMWorkloadClass returns a WorkloadClass that governs the Pods labeled app=<app> in the namespace.
func newRMWorkloadClass(ns, app string, rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		TypeMeta:   metav1.TypeMeta{APIVersion: workloadsv1.GroupVersion.String(), Kind: "WorkloadClass"},
		ObjectMeta: metav1.ObjectMeta{Name: app, Namespace: ns},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector:        &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
			ResourceManagement: rm,
		},
	}
}

// applyRMDeployment applies a single-container Deployment labeled app=<app>.
func applyRMDeployment(ns, app string, replicas int32, container corev1.Container) {
	GinkgoHelper()
	zero := int64(0)
	d := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: app, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": app}},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: &zero,
					Containers:                    []corev1.Container{container},
				},
			},
		},
	}
	Expect(applyObject(d)).To(Succeed())
}

func rmPods(ns, app string) ([]corev1.Pod, error) {
	out, err := kubectlJSON(nil, "get", "pods", "-n", ns, "-l", "app="+app, "-o", "json")
	if err != nil {
		return nil, err
	}
	pods := &corev1.PodList{}
	if err := json.Unmarshal(out, pods); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// expectRMPod waits for the Deployment's single Pod to be admitted and scheduled, and returns it.
func expectRMPod(ns, app string) corev1.Pod {
	GinkgoHelper()
	var pod corev1.Pod
	Eventually(func(g Gomega) {
		pods, err := rmPods(ns, app)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods).To(HaveLen(1))
		g.Expect(pods[0].Status.QOSClass).NotTo(BeEmpty(), "the Pod has no QoS class yet")
		pod = pods[0]
	}, rmTimeout, rmInterval).Should(Succeed())
	return pod
}

// rmGet reads an object as JSON. It reports found=false if the object does not exist.
func rmGet(resourceType, ns, name string, into any) (found bool, err error) {
	out, err := kubectlJSON(nil, "get", resourceType, name, "-n", ns, "-o", "json")
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return false, nil
		}
		return false, err
	}
	return true, json.Unmarshal(out, into)
}

func expectRMHPA(name string) *autoscalingv2.HorizontalPodAutoscaler {
	GinkgoHelper()
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	Eventually(func(g Gomega) {
		found, err := rmGet("horizontalpodautoscaler", rmNS, name, hpa)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(found).To(BeTrue(), "HorizontalPodAutoscaler %s does not exist", name)
	}, rmTimeout, rmInterval).Should(Succeed())
	return hpa
}

func expectNoRMAutoscaler(resourceType, name string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		found, err := rmGet(resourceType, rmNS, name, &map[string]any{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(found).To(BeFalse(), "%s %s still exists", resourceType, name)
	}, rmTimeout, rmInterval).Should(Succeed())
}

// expectRMVPA waits for the VPA to exist with the update mode, and returns its spec.
func expectRMVPA(name, updateMode string) map[string]any {
	GinkgoHelper()
	var spec map[string]any
	Eventually(func(g Gomega) {
		vpa := map[string]any{}
		found, err := rmGet(rmVPAResource, rmNS, name, &vpa)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(found).To(BeTrue(), "VerticalPodAutoscaler %s does not exist", name)
		spec, _ = vpa["spec"].(map[string]any)
		g.Expect(spec).To(HaveKeyWithValue("updatePolicy", map[string]any{"updateMode": updateMode}))
	}, rmTimeout, rmInterval).Should(Succeed())
	return spec
}

var _ = Describe("Resource management", Ordered, func() {
	pause := corev1.Container{Name: "pause", Image: pauseImage}

	BeforeAll(func() {
		By("creating manager namespace")
		_, err := utils.Run(exec.Command("kubectl", "create", "ns", namespace))
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		_, err = utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted"))
		Expect(err).NotTo(HaveOccurred())

		By("installing metrics-server, so that HorizontalPodAutoscalers can read CPU usage")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", rmMetricsServerManifest))
		Expect(err).NotTo(HaveOccurred(), "Failed to install metrics-server")
		// Kind's kubelets serve self-signed certificates.
		_, err = utils.Run(exec.Command("kubectl", "patch", "deployment", "metrics-server", "-n", "kube-system", "--type=json",
			`-p=[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]`))
		Expect(err).NotTo(HaveOccurred(), "Failed to configure metrics-server")

		By("installing the VerticalPodAutoscaler CRDs before the manager starts, so that it detects them")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", rmVPACRDs))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the VerticalPodAutoscaler CRDs")
		_, err = utils.Run(exec.Command("kubectl", "wait", "--for=condition=Established", "crd/"+rmVPAResource, "--timeout=60s"))
		Expect(err).NotTo(HaveOccurred())

		By("installing CRDs")
		_, err = utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("creating the test namespaces")
		for _, ns := range []string{rmNS, rmLimitRangeNS} {
			// A previous run's namespace may still be terminating; retry until it can be created.
			Eventually(func() error {
				_, err := utils.Run(exec.Command("kubectl", "create", "ns", ns))
				return err
			}, rmTimeout, 5*time.Second).Should(Succeed())
		}

		By("waiting for the Pod webhook to serve requests")
		Eventually(func(g Gomega) {
			admitProbePod(g, newPlacementPod("webhook-ready", inNamespace(rmNS)))
		}, rmTimeout, 5*time.Second).Should(Succeed())

		By("waiting for metrics-server to be available")
		_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/metrics-server", "-n", "kube-system", "--timeout=180s"))
		Expect(err).NotTo(HaveOccurred(), "metrics-server did not become available")
	})

	AfterAll(func() {
		By("cleaning up test resources")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", rmNS, rmLimitRangeNS, "--ignore-not-found"))

		By("undeploying the controller-manager")
		_, _ = utils.Run(exec.Command("make", "undeploy"))

		By("uninstalling CRDs")
		_, _ = utils.Run(exec.Command("make", "uninstall"))
		// The other suites run without VerticalPodAutoscaler and metrics-server.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", rmVPACRDs, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", rmMetricsServerManifest, "--ignore-not-found"))

		By("removing manager namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		By("Fetching controller manager pod logs")
		if out, err := utils.Run(exec.Command("kubectl", "logs", "-n", namespace,
			"-l", "control-plane=controller-manager", "--tail=300")); err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", out)
		}
		By("Fetching WorkloadClasses, workloads, Pods and autoscalers")
		if out, err := utils.Run(exec.Command("kubectl", "get", "workloadclass,deployments,pods,hpa,"+rmVPAResource,
			"-n", rmNS, "-o", "wide")); err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Resources:\n %s", out)
		}
		if out, err := utils.Run(exec.Command("kubectl", "get", "events", "-n", rmNS, "--sort-by=.lastTimestamp")); err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Events:\n %s", out)
		}
	})

	It("fills in the requests and limits that a governed Pod does not set", func() {
		Expect(applyObject(newRMWorkloadClass(rmNS, "web", &workloadsv1.ResourceManagement{
			InitialResources: &workloadsv1.InitialResources{Requests: rmQuantities("100m", "64Mi")},
		}))).To(Succeed())
		expectWorkloadClassCondition("web", rmNS, rmValidatedCondition, "True")

		applyRMDeployment(rmNS, "web", 1, pause)
		pod := expectRMPod(rmNS, "web")

		// The default limit policy sets the memory limit to the memory request, and no CPU limit.
		resources := pod.Spec.Containers[0].Resources
		Expect(resources.Requests.Cpu().String()).To(Equal("100m"))
		Expect(resources.Requests.Memory().String()).To(Equal("64Mi"))
		Expect(resources.Limits.Memory().String()).To(Equal("64Mi"))
		Expect(resources.Limits).NotTo(HaveKey(corev1.ResourceCPU))
		Expect(pod.Annotations).To(HaveKeyWithValue(rmInitialResourcesAnnotation, "cpu-request,memory-request,memory-limit"))
		// Without initial resources, this Pod would be BestEffort.
		Expect(pod.Status.QOSClass).To(Equal(corev1.PodQOSBurstable))

		By("leaving the Deployment as it was applied, so that GitOps tools see no drift")
		deploy := &appsv1.Deployment{}
		found, err := rmGet("deployment", rmNS, "web", deploy)
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(deploy.Spec.Template.Spec.Containers[0].Resources.Requests).To(BeEmpty())
		Expect(deploy.Spec.Template.Spec.Containers[0].Resources.Limits).To(BeEmpty())
	})

	It("never changes the values that a workload sets", func() {
		Expect(applyObject(newRMWorkloadClass(rmNS, "sized", &workloadsv1.ResourceManagement{
			InitialResources: &workloadsv1.InitialResources{
				Requests:    rmQuantities("100m", "64Mi"),
				LimitPolicy: workloadsv1.LimitPolicyEqualsRequest,
			},
		}))).To(Succeed())
		expectWorkloadClassCondition("sized", rmNS, rmValidatedCondition, "True")

		sized := pause
		sized.Resources = corev1.ResourceRequirements{
			Requests: rmQuantities("20m", "16Mi"),
			Limits:   rmQuantities("", "32Mi"),
		}
		applyRMDeployment(rmNS, "sized", 1, sized)
		pod := expectRMPod(rmNS, "sized")

		// Only the missing CPU limit is filled in, from the CPU request that the workload set.
		resources := pod.Spec.Containers[0].Resources
		Expect(resources.Requests.Cpu().String()).To(Equal("20m"))
		Expect(resources.Requests.Memory().String()).To(Equal("16Mi"))
		Expect(resources.Limits.Memory().String()).To(Equal("32Mi"))
		Expect(resources.Limits.Cpu().String()).To(Equal("20m"))
		Expect(pod.Annotations).To(HaveKeyWithValue(rmInitialResourcesAnnotation, "cpu-limit"))
	})

	It("leaves Pods alone that no WorkloadClass governs", func() {
		applyRMDeployment(rmNS, "ungoverned", 1, pause)
		pod := expectRMPod(rmNS, "ungoverned")
		Expect(pod.Spec.Containers[0].Resources.Requests).To(BeEmpty())
		Expect(pod.Annotations).NotTo(HaveKey(rmInitialResourcesAnnotation))
		Expect(pod.Status.QOSClass).To(Equal(corev1.PodQOSBestEffort))
	})

	It("keeps the defaults of a LimitRange, which the API server applies before the webhook", func() {
		limitRange := &corev1.LimitRange{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "LimitRange"},
			ObjectMeta: metav1.ObjectMeta{Name: "defaults", Namespace: rmLimitRangeNS},
			Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
				Type:           corev1.LimitTypeContainer,
				DefaultRequest: rmQuantities("30m", ""),
			}}},
		}
		Expect(applyObject(limitRange)).To(Succeed())
		Expect(applyObject(newRMWorkloadClass(rmLimitRangeNS, "web", &workloadsv1.ResourceManagement{
			InitialResources: &workloadsv1.InitialResources{
				Requests:    rmQuantities("100m", "64Mi"),
				LimitPolicy: workloadsv1.LimitPolicyNone,
			},
		}))).To(Succeed())
		expectWorkloadClassCondition("web", rmLimitRangeNS, rmValidatedCondition, "True")

		applyRMDeployment(rmLimitRangeNS, "web", 1, pause)
		pod := expectRMPod(rmLimitRangeNS, "web")

		// The LimitRange only defaults the CPU request, so initial resources still fill in the memory request.
		resources := pod.Spec.Containers[0].Resources
		Expect(resources.Requests.Cpu().String()).To(Equal("30m"))
		Expect(resources.Requests.Memory().String()).To(Equal("64Mi"))
		Expect(resources.Limits).To(BeEmpty())
		Expect(pod.Annotations).To(HaveKeyWithValue(rmInitialResourcesAnnotation, "memory-request"))
	})

	It("creates an HPA that scales on CPU utilization from the first Pod", func() {
		minReplicas, maxReplicas, utilization := int32(1), int32(3), int32(50)
		Expect(applyObject(newRMWorkloadClass(rmNS, "burner", &workloadsv1.ResourceManagement{
			// The workload sets no requests. Without this request, the HPA could not compute a utilization.
			// The CPU limit keeps each busy Pod at 100% of its request, so the load does not starve the node.
			InitialResources: &workloadsv1.InitialResources{
				Requests:    rmQuantities("100m", "32Mi"),
				LimitPolicy: workloadsv1.LimitPolicyEqualsRequest,
			},
			Autoscaling: &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Min: &minReplicas, Max: maxReplicas},
				Metrics: []autoscalingv2.MetricSpec{{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name:   corev1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &utilization},
					},
				}},
				Profile: workloadsv1.AutoscalingProfileResponsive,
			},
		}))).To(Succeed())
		expectWorkloadClassCondition("burner", rmNS, rmValidatedCondition, "True")

		// One busy loop uses all of the 100m CPU request, which is twice the target utilization.
		applyRMDeployment(rmNS, "burner", 1, corev1.Container{
			Name:    "burner",
			Image:   rmBusyboxImage,
			Command: []string{"sh", "-c", "while true; do :; done"},
		})

		By("creating the HPA of the Deployment")
		hpa := expectRMHPA("wc-deployment-burner")
		Expect(hpa.Spec.ScaleTargetRef).To(Equal(autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "burner"}))
		Expect(hpa.Spec.MinReplicas).To(HaveValue(Equal(minReplicas)))
		Expect(hpa.Spec.MaxReplicas).To(Equal(maxReplicas))
		Expect(hpa.Labels).To(HaveKeyWithValue(rmManagedByLabel, rmManagedByValue))
		Expect(hpa.Labels).To(HaveKeyWithValue(rmWorkloadClassLabel, "burner"))
		Expect(hpa.OwnerReferences).To(HaveLen(1))
		Expect(hpa.OwnerReferences[0].Kind).To(Equal("Deployment"))
		Expect(hpa.OwnerReferences[0].Name).To(Equal("burner"))
		// The Responsive profile scales down after a 60s stabilization window.
		Expect(hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds).To(HaveValue(Equal(int32(60))))

		By("scaling the Deployment up once the HPA reads the CPU utilization")
		Eventually(func(g Gomega) {
			current := &autoscalingv2.HorizontalPodAutoscaler{}
			found, err := rmGet("horizontalpodautoscaler", rmNS, "wc-deployment-burner", current)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(found).To(BeTrue())
			g.Expect(current.Status.CurrentMetrics).NotTo(BeEmpty(), "the HPA has not read any metric yet")
			g.Expect(current.Status.DesiredReplicas).To(BeNumerically(">", 1))

			deploy := &appsv1.Deployment{}
			found, err = rmGet("deployment", rmNS, "burner", deploy)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(found).To(BeTrue())
			g.Expect(deploy.Spec.Replicas).To(HaveValue(BeNumerically(">", 1)))
		}, 6*time.Minute, 5*time.Second).Should(Succeed())

		By("stopping the load")
		_, err := utils.Run(exec.Command("kubectl", "delete", "deployment", "burner", "-n", rmNS))
		Expect(err).NotTo(HaveOccurred())
	})

	It("creates a VPA, and leaves the resource that the HPA scales on out of it", func() {
		utilization := int32(60)
		wc := newRMWorkloadClass(rmNS, "web", &workloadsv1.ResourceManagement{
			InitialResources: &workloadsv1.InitialResources{Requests: rmQuantities("100m", "64Mi")},
			Autoscaling: &workloadsv1.Autoscaling{
				Replicas: workloadsv1.ReplicaBounds{Max: 2},
				Metrics: []autoscalingv2.MetricSpec{{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name:   corev1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &utilization},
					},
				}},
			},
			Rightsizing: &workloadsv1.Rightsizing{
				Mode:              workloadsv1.RightsizingModeAuto,
				ExcludeContainers: []string{"istio-proxy"},
				Bounds: &workloadsv1.ContainerBounds{
					Min: rmQuantities("50m", "32Mi"),
					Max: rmQuantities("1", "1Gi"),
				},
			},
		})
		Expect(applyObject(wc)).To(Succeed())
		expectWorkloadClassCondition("web", rmNS, rmValidatedCondition, "True")

		expectRMHPA("wc-deployment-web")
		// The real VerticalPodAutoscaler CRD accepts the update mode for in-place rightsizing.
		spec := expectRMVPA("wc-deployment-web", "InPlaceOrRecreate")
		Expect(spec).To(HaveKeyWithValue("targetRef", map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"}))
		Expect(spec).To(HaveKeyWithValue("resourcePolicy", map[string]any{"containerPolicies": []any{
			// CPU is left to the HPA, which scales on its utilization.
			map[string]any{
				"containerName":       "*",
				"controlledResources": []any{"memory"},
				"minAllowed":          map[string]any{"memory": "32Mi"},
				"maxAllowed":          map[string]any{"memory": "1Gi"},
			},
			map[string]any{"containerName": "istio-proxy", "mode": "Off"},
		}}))

		By("recording that CPU is left to horizontal scaling")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "events", "-n", rmNS,
				"--field-selector", "reason=RightsizingSkipped,involvedObject.kind=WorkloadClass,involvedObject.name=web",
				"-o", "jsonpath={.items[*].message}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("leaves cpu to horizontal scaling"))
		}, rmTimeout, rmInterval).Should(Succeed())

		By("following the rightsizing mode of the WorkloadClass")
		wc.Spec.ResourceManagement.Rightsizing.Mode = workloadsv1.RightsizingModeRecommend
		Expect(applyObject(wc)).To(Succeed())
		expectRMVPA("wc-deployment-web", "Off")
	})

	It("defers to a user-created HPA that targets the workload, and reclaims it when it is removed", func() {
		user := &autoscalingv2.HorizontalPodAutoscaler{
			TypeMeta:   metav1.TypeMeta{APIVersion: "autoscaling/v2", Kind: "HorizontalPodAutoscaler"},
			ObjectMeta: metav1.ObjectMeta{Name: "user-hpa", Namespace: rmNS},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"},
				MaxReplicas:    1,
			},
		}
		Expect(applyObject(user)).To(Succeed())
		expectNoRMAutoscaler("horizontalpodautoscaler", "wc-deployment-web")
		// The VPA is managed independently of the HPA.
		expectRMVPA("wc-deployment-web", "Off")

		_, err := utils.Run(exec.Command("kubectl", "delete", "hpa", "user-hpa", "-n", rmNS))
		Expect(err).NotTo(HaveOccurred())
		expectRMHPA("wc-deployment-web")
	})

	It("deletes the autoscalers when the WorkloadClass is deleted, and keeps the workload running", func() {
		_, err := utils.Run(exec.Command("kubectl", "delete", "workloadclass", "web", "-n", rmNS))
		Expect(err).NotTo(HaveOccurred())

		expectNoRMAutoscaler("horizontalpodautoscaler", "wc-deployment-web")
		expectNoRMAutoscaler(rmVPAResource, "wc-deployment-web")

		pods, err := rmPods(rmNS, "web")
		Expect(err).NotTo(HaveOccurred())
		Expect(pods).NotTo(BeEmpty())
		Expect(pods[0].Spec.Containers[0].Resources.Requests.Cpu().String()).To(Equal("100m"))
	})
})
