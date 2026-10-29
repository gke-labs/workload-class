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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/test/utils"
)

// These specs exercise the runtime half of the gke-spot-placement plugin (GKESpotPlacementReconciler) and the
// guardrail budgets, end to end on a Kind cluster:
//
//   - Spot capacity is controlled by writing kube-system/cluster-autoscaler-status ourselves. Kind has no
//     cluster autoscaler, so the reconciler reads whatever we put there. The single Kind node stays an
//     On-Demand node throughout, so Pods that prefer Spot land on it and become fallback Pods.
//   - Workloads are Deployments (pause image), so evicted Pods are recreated by a controller and go back
//     through the webhook, like in production.
//   - WorkloadClasses only set maxNonDisruptionDurationDays (see newRuntimeWorkloadClass). Without
//     allowedDisruptionWindows they are always in an allowed window, so maintenance readiness is Ready, the PDB
//     the core controller creates allows 100% unavailable, and the disruption webhook admits evictions.
//
// Not covered here: reversion stopping once the Spot share meets spotRatio. With a single node that is
// either Spot or On-Demand, a workload cannot have Pods on both at once, so that path is unit-tested only.
const (
	runtimeNS = "spot-runtime-e2e"

	autoscalerStatusNS   = "kube-system"
	autoscalerStatusName = "cluster-autoscaler-status"

	// runtimeGuardrailTmpl is a baseline guardrail whose placement block is filled in per spec.
//
// Its disruption constraints must not invalidate the runtime WorkloadClasses: unset WorkloadClass fields don't
// comply with a guardrail that limits them, so it doesn't restrict daysOfWeek (the WorkloadClasses have no
// disruption windows, which allows every day), and runtimeMaxNonDisruptionDays is shared with
// newRuntimeWorkloadClass.
	runtimeGuardrailTmpl = `
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: default
spec:
  constraints:
    disruption:
      maxAllowedWindows: 2
      maxNonDisruptionDurationDays: %d
  pluginConstraints:
    placement:
      - name: gke-spot-placement
        parameters:
%s
`
)

func runtimeGuardrail(params string) string {
	return fmt.Sprintf(runtimeGuardrailTmpl, runtimeMaxNonDisruptionDays, params)
}

// setSpotCapacity writes the cluster autoscaler status ConfigMap with a Spot node group that is either Healthy
// (capacity available) or in stockout Backoff (unavailable).
func setSpotCapacity(available bool) {
	GinkgoHelper()
	status := `
nodeGroups:
- name: gke-e2e-spot-pool-grp
  health:
    status: Healthy
  scaleUp:
    status: NoActivity
`
	if !available {
		status = `
nodeGroups:
- name: gke-e2e-spot-pool-grp
  health:
    status: Healthy
  scaleUp:
    status: Backoff
    backoffInfo:
      errorCode: ZONE_RESOURCE_POOL_EXHAUSTED
`
	}
	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: autoscalerStatusName, Namespace: autoscalerStatusNS},
		Data:       map[string]string{"status": status},
	}
	Expect(applyObject(cm)).To(Succeed())
}

func clearSpotCapacity() {
	_, _ = utils.Run(exec.Command("kubectl", "delete", "configmap", autoscalerStatusName, "-n", autoscalerStatusNS, "--ignore-not-found"))
}

// runtimeMaxNonDisruptionDays is the guardrail limit and WorkloadClass value for maxNonDisruptionDurationDays.
// A WorkloadClass that leaves it unset (no limit) would fail validation against runtimeGuardrailTmpl.
const runtimeMaxNonDisruptionDays = 30

// newRuntimeWorkloadClass returns a WorkloadClass in runtimeNS selecting app=<app> that references the e2e policy.
// It sets maxNonDisruptionDurationDays so that it complies with runtimeGuardrailTmpl, and leaves out disruption
// windows so that evictions are always allowed.
func newRuntimeWorkloadClass(app string, cs *workloadsv1.CapacityStrategy) *workloadsv1.WorkloadClass {
	wc := newPlacementWorkloadClass(app, cs, withSpotPolicyRef())
	wc.Namespace = runtimeNS
	wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = runtimeMaxNonDisruptionDays
	return wc
}

func runtimePod(app string, opts ...podOpt) *corev1.Pod {
	return newPlacementPod(app, append([]podOpt{inNamespace(runtimeNS)}, opts...)...)
}

func fallbackStrategy() *workloadsv1.CapacityStrategy {
	return &workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}
}

// e2eMinDurationOnFallback is the minDurationOnFallback used by specs that expect Active reversion to evict
// fallback Pods and the Deployment to be replenished.
//
// The Kind cluster has no Spot node, so a Pod recreated after a reversion eviction lands on the On-Demand node
// again as a fallback Pod. With 0s it is evicted again as soon as it binds, in a loop, and the replacements are
// almost always terminating when the test looks for them. A short floor lets the replacements run long enough
// to be observed, while the original Pods (which have been on fallback since the spec set them up) still
// cross it well within the eviction timeout.
var e2eMinDurationOnFallback = &metav1.Duration{Duration: 30 * time.Second}

func activePolicy(minDurationOnFallback *metav1.Duration) *workloadsv1.GKESpotPlacementPolicy {
	return newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{
		Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.ActiveReversionAction, MinDurationOnFallback: minDurationOnFallback},
	})
}

func lazyPolicy() *workloadsv1.GKESpotPlacementPolicy {
	return newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{
		Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.LazyReversionAction},
	})
}

// applyDeployment creates or updates a pause Deployment labeled app=<app> in runtimeNS.
func applyDeployment(app string, replicas int32) {
	GinkgoHelper()
	zero := int64(0)
	d := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: app, Namespace: runtimeNS},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": app}},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: &zero,
					Containers:                    []corev1.Container{{Name: "pause", Image: pauseImage}},
				},
			},
		},
	}
	// Retry: the Pod webhook may still be warming up when the first Pods are admitted.
	Eventually(func() error { return applyObject(d) }).Should(Succeed())
}

type podSummary struct {
	Name     string
	NodeName string
	Phase    corev1.PodPhase
	Spot     bool // carries the Spot toleration, i.e. was targeted for Spot by the webhook
}

// listAppPods returns the active Pods labeled app=<app> in runtimeNS, or all app-labeled Pods when app is empty.
func listAppPods(app string) ([]podSummary, error) {
	selector := "app"
	if app != "" {
		selector = "app=" + app
	}
	out, err := kubectlJSON(nil, "get", "pods", "-n", runtimeNS, "-l", selector, "-o", "json")
	if err != nil {
		return nil, err
	}
	list := &corev1.PodList{}
	if err := json.Unmarshal(out, list); err != nil {
		return nil, err
	}
	var pods []podSummary
	for i := range list.Items {
		p := &list.Items[i]
		if p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		spot := false
		for _, t := range p.Spec.Tolerations {
			if t.Key == gkeSpotLabel {
				spot = true
				break
			}
		}
		pods = append(pods, podSummary{Name: p.Name, NodeName: p.Spec.NodeName, Phase: p.Status.Phase, Spot: spot})
	}
	return pods, nil
}

func podNames(pods []podSummary) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}

// waitForFallbackPods waits until the Deployment has n Running, Spot-targeted Pods on the (On-Demand) Kind node
// and returns their names. These are fallback Pods from the plugin's point of view.
func waitForFallbackPods(app, node string, n int) []string {
	GinkgoHelper()
	var names []string
	Eventually(func(g Gomega) {
		pods, err := listAppPods(app)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods).To(HaveLen(n))
		for _, p := range pods {
			g.Expect(p.Phase).To(Equal(corev1.PodRunning), "pod %s", p.Name)
			g.Expect(p.NodeName).To(Equal(node), "pod %s", p.Name)
			g.Expect(p.Spot).To(BeTrue(), "pod %s should carry the Spot toleration", p.Name)
		}
		names = podNames(pods)
	}, 3*time.Minute, 2*time.Second).Should(Succeed())
	return names
}

// expectPodsUnchanged asserts for consistentlyDuration that the Deployment's Pods are exactly the given ones.
func expectPodsUnchanged(app string, names []string) {
	GinkgoHelper()
	Consistently(func(g Gomega) {
		pods, err := listAppPods(app)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(podNames(pods)).To(ConsistOf(names))
	}, consistentlyDuration, consistentlyInterval).Should(Succeed())
}

// expectPodsEvicted asserts that none of the given Pods remain and the Deployment has been replenished to n Pods.
func expectPodsEvicted(app string, names []string, n int, within time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		pods, err := listAppPods(app)
		g.Expect(err).NotTo(HaveOccurred())
		for _, old := range names {
			g.Expect(podNames(pods)).NotTo(ContainElement(old), "pod %s should have been evicted", old)
		}
		g.Expect(pods).To(HaveLen(n))
	}, within, 2*time.Second).Should(Succeed())
}

var _ = Describe("GKE Spot Placement runtime", Ordered, func() {
	var runtimeNode string

	BeforeAll(func() {
		By("creating manager namespace")
		_, err := utils.Run(exec.Command("kubectl", "create", "ns", namespace))
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		_, err = utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted"))
		Expect(err).NotTo(HaveOccurred())

		By("installing CRDs")
		_, err = utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("creating the test namespace")
		// A previous run's namespace may still be terminating; retry until it can be created.
		Eventually(func() error {
			_, err := utils.Run(exec.Command("kubectl", "create", "ns", runtimeNS))
			return err
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("looking up the Kind node and making sure it is not labeled as Spot")
		out, err := utils.Run(exec.Command("kubectl", "get", "nodes", "-o", "jsonpath={.items[0].metadata.name}"))
		Expect(err).NotTo(HaveOccurred())
		runtimeNode = strings.TrimSpace(out)
		Expect(runtimeNode).NotTo(BeEmpty())
		_, _ = utils.Run(exec.Command("kubectl", "label", "node", runtimeNode, gkeSpotLabel+"-"))

		By("waiting for the Pod placement webhook to serve requests")
		Eventually(func(g Gomega) {
			admitProbePod(g, runtimePod("webhook-ready"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		By("cleaning up test resources")
		clearSpotCapacity()
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", runtimeNS, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "gkespotplacementpolicy", placementPolicyName, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrail", "default", "--ignore-not-found"))

		By("undeploying the controller-manager")
		_, _ = utils.Run(exec.Command("make", "undeploy"))

		By("uninstalling CRDs")
		_, _ = utils.Run(exec.Command("make", "uninstall"))

		By("removing manager namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found"))
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			By("Fetching controller manager pod logs")
			if out, err := utils.Run(exec.Command("kubectl", "logs", "-n", namespace, "-l", "control-plane=controller-manager", "--tail=300")); err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", out)
			}
			By("Fetching WorkloadClasses and Pods")
			if out, err := utils.Run(exec.Command("kubectl", "get", "workloadclass,pods", "-n", runtimeNS, "-o", "wide")); err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "%s\n", out)
			}
		}

		By("Cleaning up runtime test resources")
		// Deployments first so recreated Pods stop flowing through the webhook, then capacity so reversion
		// loops stop, then the policy objects.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "deployment", "--all", "-n", runtimeNS, "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", "--all", "-n", runtimeNS, "--grace-period=0", "--force"))
		clearSpotCapacity()
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", "--all", "-n", runtimeNS))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "gkespotplacementpolicy", placementPolicyName, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrail", "default", "--ignore-not-found"))
		Eventually(func(g Gomega) {
			pods, err := listAppPods("")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pods).To(BeEmpty())
		}).Should(Succeed())
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	// setUpFallbackWorkload creates the policy, a FallbackToOnDemand WorkloadClass, and a Deployment of n
	// replicas, and waits until all n Pods are running as fallback Pods on the On-Demand Kind node with
	// InFallback=True. It returns the Pod names.
	setUpFallbackWorkload := func(app string, policy *workloadsv1.GKESpotPlacementPolicy, cs *workloadsv1.CapacityStrategy, n int) []string {
		GinkgoHelper()
		Expect(applyObject(policy)).To(Succeed())
		Expect(applyObject(newRuntimeWorkloadClass(app, cs))).To(Succeed())
		expectWorkloadClassReady(app, runtimeNS, true)
		applyDeployment(app, int32(n))
		names := waitForFallbackPods(app, runtimeNode, n)
		expectWorkloadClassCondition(app, runtimeNS, workloadsv1.ConditionTypeInFallback, "True")
		return names
	}

	Context("Active reversion", func() {
		It("evicts fallback Pods once Spot capacity returns, and not before", func() {
			const app = "reversion-active"
			setSpotCapacity(false)
			names := setUpFallbackWorkload(app, activePolicy(e2eMinDurationOnFallback), fallbackStrategy(), 2)

			By("verifying nothing is evicted while Spot capacity is unavailable")
			expectPodsUnchanged(app, names)

			By("signalling that Spot capacity is available")
			setSpotCapacity(true)

			By("verifying the fallback Pods are evicted and recreated")
			expectPodsEvicted(app, names, 2, 2*time.Minute)
		})

		It("applies the 5m dampening floor when minDurationOnFallback is unset", func() {
			const app = "reversion-floor"
			setSpotCapacity(true)
			names := setUpFallbackWorkload(app, activePolicy(nil), fallbackStrategy(), 1)

			By("verifying a freshly fallen-back Pod is not evicted even though Spot capacity is available")
			expectPodsUnchanged(app, names)
		})

		It("does not evict under Lazy reversion, but new Pods target Spot again", func() {
			const app = "reversion-lazy"
			setSpotCapacity(true)
			names := setUpFallbackWorkload(app, lazyPolicy(), fallbackStrategy(), 1)

			expectPodsUnchanged(app, names)

			Eventually(func(g Gomega) {
				expectPreferredSpot(g, admitProbePod(g, runtimePod(app)))
			}).Should(Succeed())
		})
	})

	Context("Guardrail maxFallbackRatio", func() {
		It("downgrades new Pods to strict Spot once the fallback budget is exhausted", func() {
			const app = "budget-percent"
			Eventually(func() error {
				return applyManifest(runtimeGuardrail(`          enforcementMode: Allowed
          fallback:
            allowFallbackToOnDemand: true
            maxFallbackRatio: "50%"`))
			}).Should(Succeed())
			setSpotCapacity(false)
			Expect(applyObject(lazyPolicy())).To(Succeed())
			Expect(applyObject(newRuntimeWorkloadClass(app, fallbackStrategy()))).To(Succeed())
			expectWorkloadClassReady(app, runtimeNS, true)

			By("verifying the budget is not enforced before any fallback")
			Eventually(func(g Gomega) {
				expectPreferredSpot(g, admitProbePod(g, runtimePod(app)))
			}).Should(Succeed())

			By("putting the workload into fallback")
			applyDeployment(app, 1)
			waitForFallbackPods(app, runtimeNode, 1)
			expectWorkloadClassCondition(app, runtimeNS, workloadsv1.ConditionTypeInFallback, "True")

			By("verifying a new Pod is pinned to strict Spot (2 of 2 on fallback would exceed 50%)")
			Eventually(func(g Gomega) {
				pod := admitProbePod(g, runtimePod(app))
				expectRequiredSpot(g, pod)
				g.Expect(preferredNodeTerms(pod)).To(BeEmpty())
			}).Should(Succeed())

			By("verifying a FallbackBudgetExhausted event was recorded on the WorkloadClass")
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "events", "-n", runtimeNS,
					"--field-selector=reason=FallbackBudgetExhausted", "-o", "jsonpath={.items[*].involvedObject.name}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring(app))
			}).Should(Succeed())
		})

		It("allows fallback while within an absolute budget", func() {
			const app = "budget-absolute"
			Eventually(func() error {
				return applyManifest(runtimeGuardrail(`          enforcementMode: Allowed
          fallback:
            allowFallbackToOnDemand: true
            maxFallbackRatio: 5`))
			}).Should(Succeed())
			setSpotCapacity(false)
			setUpFallbackWorkload(app, lazyPolicy(), fallbackStrategy(), 1)

			By("verifying a new Pod still prefers Spot (2 fallback Pods fit within 5)")
			Consistently(func(g Gomega) {
				expectPreferredSpot(g, admitProbePod(g, runtimePod(app)))
			}, consistentlyDuration, consistentlyInterval).Should(Succeed())
		})
	})

	Context("Guardrail maxFallbackDuration", func() {
		guardrailWithLimit := func(limit string) {
			GinkgoHelper()
			Eventually(func() error {
				return applyManifest(runtimeGuardrail(fmt.Sprintf(`          enforcementMode: Allowed
          reversion:
            maxFallbackDuration: %q`, limit)))
			}).Should(Succeed())
		}

		It("force-evicts Pods past the limit even under Lazy reversion", func() {
			const app = "duration-lazy"
			guardrailWithLimit("30s")
			setSpotCapacity(true)
			names := setUpFallbackWorkload(app, lazyPolicy(), fallbackStrategy(), 1)

			By("verifying the Pod is left alone while under the limit")
			// The Pod has just started; the consistently window is well under the 30s limit.
			expectPodsUnchanged(app, names)

			By("verifying the Pod is evicted once it has been on fallback for 30s")
			expectPodsEvicted(app, names, 1, 90*time.Second)
		})

		It("does not force-evict while Spot capacity is unavailable", func() {
			const app = "duration-no-capacity"
			guardrailWithLimit("5s")
			setSpotCapacity(false)
			names := setUpFallbackWorkload(app, lazyPolicy(), fallbackStrategy(), 1)

			By("verifying the Pod stays past the limit because there is nowhere to revert to")
			Consistently(func(g Gomega) {
				pods, err := listAppPods(app)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(podNames(pods)).To(ConsistOf(names))
			}, 20*time.Second, 2*time.Second).Should(Succeed())
		})
	})

	Context("Full stack", func() {
		It("walks a workload through handshake, placement, fallback, budget, reversion, and invalidation", func() {
			const app = "full-stack"

			By("installing a guardrail that requires Spot, allows bounded fallback, and requires Active reversion")
			// An absolute fallback budget of 2 admits both replicas below as fallback Pods regardless of whether
			// the ReplicaSet's Pods are admitted before or after the first one binds, and still pins a 3rd Pod to
			// strict Spot. A percentage budget can't do both: once 1 Pod is on fallback, a 2nd would be 2 of 2.
			Eventually(func() error {
				return applyManifest(runtimeGuardrail(`          enforcementMode: Required
          minSpotRatio: "50%"
          fallback:
            allowFallbackToOnDemand: true
            maxFallbackRatio: 2
          reversion:
            requiredReversionAction: Active
            maxFallbackDuration: "10m"`))
			}).Should(Succeed())
			setSpotCapacity(false)

			By("creating a GKESpotPlacementPolicy that satisfies the guardrail")
			Expect(applyObject(activePolicy(e2eMinDurationOnFallback))).To(Succeed())
			Eventually(func(g Gomega) {
				status, err := getConditionField("gkespotplacementpolicy", placementPolicyName, "", workloadsv1.ConditionTypeValidated, "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("True"))
			}).Should(Succeed())

			By("creating a WorkloadClass with an 80% Spot ratio and FallbackToOnDemand, and waiting for the handshake")
			cs := fallbackStrategy()
			cs.SpotRatio = "80%"
			Expect(applyObject(newRuntimeWorkloadClass(app, cs))).To(Succeed())
			expectWorkloadClassReady(app, runtimeNS, true)
			Eventually(func(g Gomega) {
				reason, err := getConditionField("workloadclass", app, runtimeNS, workloadsv1.ConditionTypePlacementPluginAttached, "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).To(Equal(workloadsv1.ReasonProfileResolved))
			}).Should(Succeed())

			By("verifying the webhook gives a new Pod a Spot preference")
			Eventually(func(g Gomega) {
				expectPreferredSpot(g, admitProbePod(g, runtimePod(app)))
			}).Should(Succeed())

			By("running the workload: 2 replicas land on the On-Demand node as fallback Pods")
			applyDeployment(app, 2)
			names := waitForFallbackPods(app, runtimeNode, 2)

			By("verifying the controller reports InFallback=True")
			expectWorkloadClassCondition(app, runtimeNS, workloadsv1.ConditionTypeInFallback, "True")

			By("verifying the fallback budget pins further Pods to strict Spot (a 3rd fallback Pod would exceed the budget of 2)")
			Eventually(func(g Gomega) {
				expectRequiredSpot(g, admitProbePod(g, runtimePod(app)))
			}).Should(Succeed())

			By("verifying nothing is evicted while Spot capacity is unavailable")
			expectPodsUnchanged(app, names)

			By("signalling Spot capacity and verifying Active reversion evicts the fallback Pods")
			setSpotCapacity(true)
			expectPodsEvicted(app, names, 2, 2*time.Minute)

			By("scaling the workload to zero and verifying InFallback is released")
			setSpotCapacity(false)
			applyDeployment(app, 0)
			expectWorkloadClassCondition(app, runtimeNS, workloadsv1.ConditionTypeInFallback, "False")

			By("tightening the guardrail to Forbidden and verifying the stack backs off")
			Eventually(func() error {
				return applyManifest(runtimeGuardrail(`          enforcementMode: Forbidden`))
			}).Should(Succeed())
			expectWorkloadClassCondition(app, runtimeNS, workloadsv1.ConditionTypeValidated, "False")
			Eventually(func(g Gomega) {
				// The plugin removes its InFallback condition from an invalid WorkloadClass.
				status, err := getConditionField("workloadclass", app, runtimeNS, workloadsv1.ConditionTypeInFallback, "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(BeEmpty())
			}).Should(Succeed())
			Consistently(func(g Gomega) {
				expectNotMutated(g, admitProbePod(g, runtimePod(app)))
			}, consistentlyDuration, consistentlyInterval).Should(Succeed())
		})
	})
})
