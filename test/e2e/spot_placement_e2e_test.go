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
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gke-labs/workload-class/test/utils"
)

func kubectlApplyYAML(manifest string) error {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	return err
}

var _ = Describe("WorkloadClass Spot Placement & Reversion", Ordered, func() {
	const testNS = "spot-e2e"
	var controllerPodName string

	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, _ = utils.Run(cmd)

		By("labeling the manager namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("creating test namespace")
		cmd = exec.Command("kubectl", "create", "ns", testNS)
		_, _ = utils.Run(cmd)

		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrail", "--all", "--ignore-not-found"))

		By("waiting for webhooks to become ready by applying a baseline WorkloadClassGuardrail")
		Eventually(func() error {
			return kubectlApplyYAML(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: spot-e2e-guardrail
spec:
  constraints:
    disruption:
      maxNonDisruptionDurationDays: 30
      emergencyOverride: true
    placement:
      spotPlacement:
        enforcementMode: Allowed
`)
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Failed to apply baseline WorkloadClassGuardrail")
	})

	AfterAll(func() {
		By("cleaning up test namespace and cluster resources")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", testNS, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrail", "spot-e2e-guardrail", "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "configmap", "cluster-autoscaler-status", "-n", "kube-system", "--ignore-not-found"))

		By("undeploying the controller-manager")
		_, _ = utils.Run(exec.Command("make", "undeploy"))

		By("uninstalling CRDs")
		_, _ = utils.Run(exec.Command("make", "uninstall"))

		By("removing manager namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found"))
	})

	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			podCmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-l", "control-plane=controller-manager", "-o", "jsonpath={.items[0].metadata.name}")
			if podOutput, err := utils.Run(podCmd); err == nil && podOutput != "" {
				controllerPodName = strings.TrimSpace(podOutput)
			}
			if controllerPodName != "" {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				if logs, err := utils.Run(cmd); err == nil {
					_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n%s\n", logs)
				}
			}
		}

		_, _ = utils.Run(exec.Command("kubectl", "delete", "deployments", "--all", "-n", testNS, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pods", "--all", "-n", testNS, "--force", "--grace-period=0"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclasses", "--all", "-n", testNS))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "configmap", "cluster-autoscaler-status", "-n", "kube-system", "--ignore-not-found"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	Context("Placement Admission & Guardrail Webhooks", func() {
		It("should reject invalid WorkloadClass placement policy configurations", func() {
			err := kubectlApplyYAML(fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: invalid-ondemand-wc
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: invalid-ondemand
  placementPolicy:
    spotPlacement:
      type: OnDemand
      spotRatio: "80%%"
`, testNS))
			Expect(err).To(HaveOccurred(), "Expected OnDemand WorkloadClass with non-zero spotRatio to be rejected")
		})

		It("should block Guardrail updates that would invalidate an existing WorkloadClass placement policy", func() {
			By("Creating a Spot WorkloadClass")
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: spot-wc
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: spot-worker
  placementPolicy:
    spotPlacement:
      type: Spot
      spotRatio: "100%%"
`, testNS))).To(Succeed())

			By("Attempting to update the Guardrail to Forbidden enforcementMode while Spot WorkloadClass exists")
			err := kubectlApplyYAML(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: spot-e2e-guardrail
spec:
  constraints:
    disruption:
      maxNonDisruptionDurationDays: 30
    placement:
      spotPlacement:
        enforcementMode: Forbidden
`)
			Expect(err).To(HaveOccurred(), "Expected Guardrail update to Forbidden to be rejected by webhook")
		})
	})

	Context("PodPlacementWebhook Mutation", func() {
		It("should mutate Pods for Spot with Fail fallback (nodeSelector, required affinity, and toleration)", func() {
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: wc-spot-fail
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: spot-fail
  placementPolicy:
    spotPlacement:
      type: Spot
      spotRatio: "100%%"
      fallback:
        action: Fail
`, testNS))).To(Succeed())

			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: pod-spot-fail
  namespace: %s
  labels:
    app: spot-fail
spec:
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.9
`, testNS))).To(Succeed())

			cmd := exec.Command("kubectl", "get", "pod", "pod-spot-fail", "-n", testNS,
				"-o", `jsonpath={.spec.nodeSelector.cloud\.google\.com/gke-spot}`)
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("true"))

			cmd = exec.Command("kubectl", "get", "pod", "pod-spot-fail", "-n", testNS,
				"-o", `jsonpath={.spec.tolerations[?(@.key=='cloud.google.com/gke-spot')].effect}`)
			out, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("NoSchedule"))
		})

		It("should mutate Pods for FallbackToOnDemand with allowedMachineFamilies", func() {
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: wc-spot-fallback
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: spot-fallback
  placementPolicy:
    spotPlacement:
      type: Spot
      spotRatio: "100%%"
      fallback:
        action: FallbackToOnDemand
        allowedMachineFamilies:
        - n2d
        - t2d
`, testNS))).To(Succeed())

			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: pod-spot-fallback
  namespace: %s
  labels:
    app: spot-fallback
spec:
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.9
`, testNS))).To(Succeed())

			cmd := exec.Command("kubectl", "get", "pod", "pod-spot-fallback", "-n", testNS,
				"-o", `jsonpath={.spec.affinity.nodeAffinity.preferredDuringSchedulingIgnoredDuringExecution[0].weight}`)
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("100"))

			cmd = exec.Command("kubectl", "get", "pod", "pod-spot-fallback", "-n", testNS,
				"-o", `jsonpath={.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[1].matchExpressions[1].values}`)
			out, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring("n2d"))
			Expect(out).To(ContainSubstring("t2d"))
		})

		It("should distribute Pods according to 80% spotRatio (4 Spot, 1 OnDemand)", func() {
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: wc-spot-ratio-80
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: ratio-worker
  placementPolicy:
    spotPlacement:
      type: Spot
      spotRatio: "80%%"
      fallback:
        action: Fail
`, testNS))).To(Succeed())

			for i := 1; i <= 5; i++ {
				Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: ratio-pod-%d
  namespace: %s
  labels:
    app: ratio-worker
spec:
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.9
`, i, testNS))).To(Succeed())
			}

			for i := 1; i <= 4; i++ {
				cmd := exec.Command("kubectl", "get", "pod", fmt.Sprintf("ratio-pod-%d", i), "-n", testNS,
					"-o", `jsonpath={.spec.nodeSelector.cloud\.google\.com/gke-spot}`)
				out, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
				Expect(strings.TrimSpace(out)).To(Equal("true"))
			}

			cmd := exec.Command("kubectl", "get", "pod", "ratio-pod-5", "-n", testNS,
				"-o", `jsonpath={.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator}`)
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("DoesNotExist"))
		})
	})

	Context("Spot Capacity Detection & Reversion (Active, Lazy, None)", func() {
		It("should proactively evict OnDemand fallback Pods on Active reversion when cluster-autoscaler-status reports Spot capacity is Healthy", func() {
			By("Setting cluster-autoscaler-status ConfigMap to Backoff (Spot stockout)")
			Expect(kubectlApplyYAML(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-autoscaler-status
  namespace: kube-system
data:
  status: |
    nodeGroups:
    - name: gke-cluster-spot-pool-grp
      health:
        status: Healthy
      scaleUp:
        status: Backoff
        backoffInfo:
          errorCode: ZONE_RESOURCE_POOL_EXHAUSTED
`)).To(Succeed())

			By("Creating a WorkloadClass with FallbackToOnDemand and Active reversion")
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: wc-active-reversion
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: active-rev
  disruptionPolicy:
    minInitialRunDurationDays: 0
    maxNonDisruptionDurationDays: 1
    allowedDisruptionWindows:
    - name: always-open
      daysOfWeek:
      - Monday
      - Tuesday
      - Wednesday
      - Thursday
      - Friday
      - Saturday
      - Sunday
      startTime: "00:00"
      endTime: "23:59"
      timeZone: "Etc/UTC"
  placementPolicy:
    spotPlacement:
      type: Spot
      spotRatio: "100%%"
      fallback:
        action: FallbackToOnDemand
      reversion:
        action: Active
        minDurationOnFallback: "0s"
`, testNS))).To(Succeed())

			By("Creating a Deployment whose Pod schedules onto the OnDemand Kind node")
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: active-fallback-deploy
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: active-rev
  template:
    metadata:
      labels:
        app: active-rev
    spec:
      terminationGracePeriodSeconds: 0
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.9
`, testNS))).To(Succeed())

			var initialPodName string
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "-n", testNS, "-l", "app=active-rev", "-o", "jsonpath={.items[0].status.phase}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("Running"))

				nameCmd := exec.Command("kubectl", "get", "pods", "-n", testNS, "-l", "app=active-rev", "-o", "jsonpath={.items[0].metadata.name}")
				nameOut, err := utils.Run(nameCmd)
				g.Expect(err).NotTo(HaveOccurred())
				initialPodName = strings.TrimSpace(nameOut)
				g.Expect(initialPodName).NotTo(BeEmpty())
			}, 1*time.Minute, 2*time.Second).Should(Succeed())

			By("Transitioning cluster-autoscaler-status Spot node pool out of Backoff back to Healthy")
			Expect(kubectlApplyYAML(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-autoscaler-status
  namespace: kube-system
data:
  status: |
    nodeGroups:
    - name: gke-cluster-spot-pool-grp
      health:
        status: Healthy
      scaleUp:
        status: NoActivity
`)).To(Succeed())

			By("Verifying the controller evicts the initial fallback Pod once Spot capacity returns")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", initialPodName, "-n", testNS, "--ignore-not-found", "-o", "jsonpath={.metadata.name}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(BeEmpty())
			}, 1*time.Minute, 2*time.Second).Should(Succeed())
		})

		It("should keep OnDemand fallback Pods running under Lazy reversion and mutate recreated Pods for Spot", func() {
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: wc-lazy-reversion
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: lazy-rev
  disruptionPolicy:
    emergencyOverride: true
  placementPolicy:
    spotPlacement:
      type: Spot
      spotRatio: "100%%"
      fallback:
        action: FallbackToOnDemand
      reversion:
        action: Lazy
`, testNS))).To(Succeed())

			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: lazy-fallback-pod
  namespace: %s
  labels:
    app: lazy-rev
spec:
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.9
`, testNS))).To(Succeed())

			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "workloadclass", "wc-lazy-reversion", "-n", testNS,
					"-o", "jsonpath={.status.conditions[?(@.type=='InFallback')].status}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("True"))
			}, 1*time.Minute, 2*time.Second).Should(Succeed())

			By("Verifying the recreated Pod under Lazy reversion is mutated for Spot")
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: lazy-recreated-pod
  namespace: %s
  labels:
    app: lazy-rev
spec:
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.9
`, testNS))).To(Succeed())

			cmd := exec.Command("kubectl", "get", "pod", "lazy-recreated-pod", "-n", testNS,
				"-o", `jsonpath={.spec.tolerations[?(@.key=='cloud.google.com/gke-spot')].effect}`)
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("NoSchedule"))
		})

		It("should mark WorkloadClass InFallback and mutate subsequent Pods for OnDemand under None reversion", func() {
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: wc-none-reversion
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: none-rev
  placementPolicy:
    spotPlacement:
      type: Spot
      spotRatio: "100%%"
      fallback:
        action: FallbackToOnDemand
      reversion:
        action: None
`, testNS))).To(Succeed())

			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: none-fallback-pod-1
  namespace: %s
  labels:
    app: none-rev
spec:
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.9
`, testNS))).To(Succeed())

			By("Waiting for the controller to mark wc-none-reversion with InFallback=True")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "workloadclass", "wc-none-reversion", "-n", testNS,
					"-o", "jsonpath={.status.conditions[?(@.type=='InFallback')].status}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("True"))
			}, 1*time.Minute, 2*time.Second).Should(Succeed())

			By("Creating a second Pod after fallback and verifying it is mutated for OnDemand permanently")
			Expect(kubectlApplyYAML(fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: none-fallback-pod-2
  namespace: %s
  labels:
    app: none-rev
spec:
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.9
`, testNS))).To(Succeed())

			cmd := exec.Command("kubectl", "get", "pod", "none-fallback-pod-2", "-n", testNS,
				"-o", `jsonpath={.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator}`)
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("DoesNotExist"))
		})
	})
})
