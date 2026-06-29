//go:build e2e
// +build e2e

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
	"os"
	"os/exec"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/yaml"

	"github.com/gke-labs/workload-class/test/utils"
)

var _ = Describe("WorkloadClass Eviction Webhook", Ordered, func() {
	var controllerPodName string
	var namespace = "workload-class-system"

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("Applying a namespace")
		cmd = exec.Command("kubectl", "apply", "-f", "config/samples/sample_namespace.yaml")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply Namespace")

		By("Applying the WorkloadClassGuardrail sample (retrying until webhook is ready)")
		Eventually(func() error {
			cmd = exec.Command("kubectl", "apply", "-f", "config/samples/workloads_v1_workloadclassguardrail.yaml")
			_, err = utils.Run(cmd)
			return err
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Failed to apply WorkloadClassGuardrail")

		By("Applying the WorkloadClass sample")
		cmd = exec.Command("kubectl", "apply", "-f", "config/samples/workloads_v1_workloadclass.yaml")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply WorkloadClass")

		By("Creating a dummy Pod matching the WorkloadClass selector (retrying until webhook is ready)")
		Eventually(func() error {
			cmd = exec.Command("kubectl", "apply", "-f", "config/samples/dummy_pod.yaml")
			_, err = utils.Run(cmd)
			return err
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Failed to create test pod")

		By("Waiting for the Pod to be ready")
		verifyPodReady := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "pod", "test-pod", "-n", "sample", "-o", "jsonpath={.status.phase}")
			out, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}
		Eventually(verifyPodReady, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the dummy pod")
		cmd := exec.Command("kubectl", "delete", "pod", "test-pod", "-n", "sample", "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}

		// Reset the WorkloadClass so the next test may make its own modifications
		By("Resetting the WorkloadClass")
		cmd := exec.Command("kubectl", "apply", "-f", "config/samples/workloads_v1_workloadclass.yaml")
		_, _ = utils.Run(cmd)

		// If test-pod was evicted, recreate it
		By("Ensuring dummy pod exists")
		cmd = exec.Command("kubectl", "delete", "pod", "test-pod", "-n", "sample", "--force", "--grace-period=0", "--ignore-not-found")
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "apply", "-f", "config/samples/dummy_pod.yaml")
		_, _ = utils.Run(cmd)

		verifyPodReady := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "pod", "test-pod", "-n", "sample", "-o", "jsonpath={.status.phase}")
			out, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}
		Eventually(verifyPodReady, 2*time.Minute, 2*time.Second).Should(Succeed())
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Eviction Webhook Validation", func() {
		It("should allow eviction of a pod because it is within the disruption window", func() {
			currentDay := time.Now().UTC().Weekday().String()

			By("Patching the WorkloadClass to be within the disruption window and zero out MinInitialRunDurationDays")
			patchWC := fmt.Sprintf(`{"spec": {"disruptionPolicy": {"minInitialRunDurationDays": 0, "allowedDisruptionWindows": [{"name": "weekend-maintenance", "daysOfWeek": ["%s"], "startTime": "00:00", "endTime": "23:59", "timeZone": "Etc/UTC"}]}}}`, currentDay)
			cmd := exec.Command("kubectl", "patch", "workloadclass", "critical-batch", "-n", "sample", "--type", "merge", "-p", patchWC)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Patching the Guardrail to allow today")
			patchGR := fmt.Sprintf(`{"spec": {"constraints": {"disruption": {"allowedDisruptionDays": ["%s"]}}}}`, currentDay)
			cmd = exec.Command("kubectl", "patch", "workloadclassguardrail", "default", "--type", "merge", "-p", patchGR)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Attempting to evict the Pod via the eviction subresource")
			yamlData, err := os.ReadFile("config/samples/eviction.yaml")
			Expect(err).NotTo(HaveOccurred())

			jsonData, err := yaml.YAMLToJSON(yamlData)
			Expect(err).NotTo(HaveOccurred())

			evictionFile := filepath.Join("/tmp", "eviction.json")
			err = os.WriteFile(evictionFile, jsonData, 0644)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "create", "--raw", "/api/v1/namespaces/sample/pods/test-pod/eviction", "-f", evictionFile)
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Eviction should be allowed but failed: %s", out))
		})

		It("should deny eviction of a pod because it is outside of the window", func() {
			notToday := time.Now().UTC().AddDate(0, 0, 2).Weekday().String()

			By("Patching the WorkloadClass to simulate being outside a disruption window")
			workloadPatch := fmt.Sprintf(`{"spec": {"disruptionPolicy": {"maxNonDisruptionDurationDays": 10, "minInitialRunDurationDays": 0, "allowedDisruptionWindows": [{"name": "weekend-maintenance", "daysOfWeek": ["%s"], "startTime": "00:00", "endTime": "23:59", "timeZone": "Etc/UTC"}]}}}`, notToday)
			cmd := exec.Command("kubectl", "patch", "workloadclass", "critical-batch", "-n", "sample", "--type", "merge", "-p", workloadPatch)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Patching the Guardrail to allow tomorrow")
			guardrailPatch := fmt.Sprintf(`{"spec": {"constraints": {"disruption": {"allowedDisruptionDays": ["%s"], "maxNonDisruptionDurationDays": 30}}}}`, notToday)
			cmd = exec.Command("kubectl", "patch", "workloadclassguardrail", "default", "--type", "merge", "-p", guardrailPatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Attempting to evict the Pod via the eviction subresource")
			yamlData, err := os.ReadFile("config/samples/eviction.yaml")
			Expect(err).NotTo(HaveOccurred())

			jsonData, err := yaml.YAMLToJSON(yamlData)
			Expect(err).NotTo(HaveOccurred())

			evictionFile := filepath.Join("/tmp", "eviction.json")
			err = os.WriteFile(evictionFile, jsonData, 0644)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "create", "--raw", "/api/v1/namespaces/sample/pods/test-pod/eviction", "-f", evictionFile)
			out, err := utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "Eviction should be blocked by the WorkloadClass policy")
			Expect(string(out)).To(ContainSubstring("Eviction blocked"), "Expected webhook to deny the request")
		})

		It("should allow eviction of a pod outside of the window if requested by an allowed user", func() {
			notToday := time.Now().UTC().AddDate(0, 0, 2).Weekday().String()

			By("Patching the WorkloadClass to simulate being outside a disruption window AND allowing cluster-autoscaler")
			workloadPatch := fmt.Sprintf(`{"spec": {"disruptionPolicy": {"maxNonDisruptionDurationDays": 10, "minInitialRunDurationDays": 0, "allowedDisruptionWindows": [{"name": "weekend-maintenance", "daysOfWeek": ["%s"], "startTime": "00:00", "endTime": "23:59", "timeZone": "Etc/UTC"}], "allowedDisruptionsOutsideOfWindow": [{"kind": "ServiceAccount", "name": "cluster-autoscaler", "namespace": "kube-system"}]}}}`, notToday)
			cmd := exec.Command("kubectl", "patch", "workloadclass", "critical-batch", "-n", "sample", "--type", "merge", "-p", workloadPatch)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Patching the Guardrail to allow tomorrow")
			guardrailPatch := fmt.Sprintf(`{"spec": {"constraints": {"disruption": {"allowedDisruptionDays": ["%s"], "maxNonDisruptionDurationDays": 30}}}}`, notToday)
			cmd = exec.Command("kubectl", "patch", "workloadclassguardrail", "default", "--type", "merge", "-p", guardrailPatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Setting up RBAC so the impersonated autoscaler has permission to call the eviction API")
			rbacCmd := exec.Command("kubectl", "create", "clusterrolebinding", "test-autoscaler-admin",
				"--clusterrole=cluster-admin",
				"--user=system:serviceaccount:kube-system:cluster-autoscaler")
			_ = rbacCmd.Run() // Ignore error in case the binding already exists from previous runs

			By("Attempting to evict the Pod via the eviction subresource as the cluster autoscaler")
			yamlData, err := os.ReadFile("config/samples/eviction.yaml")
			Expect(err).NotTo(HaveOccurred())

			jsonData, err := yaml.YAMLToJSON(yamlData)
			Expect(err).NotTo(HaveOccurred())

			evictionFile := filepath.Join("/tmp", "eviction.json")
			err = os.WriteFile(evictionFile, jsonData, 0644)
			Expect(err).NotTo(HaveOccurred())

			// Append the --as flag to impersonate the autoscaler for the raw API request
			cmd = exec.Command("kubectl", "create", "--raw", "/api/v1/namespaces/sample/pods/test-pod/eviction", "-f", evictionFile, "--as=system:serviceaccount:kube-system:cluster-autoscaler")
			_, err = utils.Run(cmd)

			// Because the cluster-autoscaler is in the allowedDisruptionsOutsideOfWindow list,
			// the webhook should NOT block the request, and err should be nil.
			Expect(err).NotTo(HaveOccurred(), "Eviction should be permitted for the autoscaler despite being outside the window")
		})
	})
})
