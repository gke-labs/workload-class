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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gke-labs/workload-class/test/utils"
)

var _ = Describe("WorkloadClass Validation Against Guardrail", Ordered, func() {
	var controllerPodName string

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

		By("Applying the WorkloadClassGuardrail sample")
		cmd = exec.Command("kubectl", "apply", "-f", "config/samples/workloads_v1_workloadclassguardrail.yaml")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply WorkloadClassGuardrail")

		By("Applying the WorkloadClass sample")
		cmd = exec.Command("kubectl", "apply", "-f", "config/samples/workloads_v1_workloadclass.yaml")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply WorkloadClass")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the clusterrolebinding")
		cmd := exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("cleaning up the curl pod for metrics")
		cmd = exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
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

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
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
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("WorkloadClass Validation", func() {
		It("should successfully validate a WorkloadClass against the Guardrail", func() {
			By("Verifying the Guardrail controller reconciles the WorkloadClass and updates its status")

			// Example test case checking the WorkloadClass status
			verifyWorkloadClassValidated := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].status}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("True"))

				cmd = exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].reason}")
				outReason, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(outReason).To(Equal("ValidationPassed"))
			}
			Eventually(verifyWorkloadClassValidated, 2*time.Minute, 2*time.Second).Should(Succeed())
		})
		It("should fail validation of a WorkloadClass with too many Disruption Windows", func() {
			By("Updating the WorkloadClass with two additional Disruption Windows")
			patch := `[{"op": "add", "path": "/spec/disruptionPolicy/allowedDisruptionWindows/-", "value": {"name": "extra1", "daysOfWeek": ["Monday"], "startTime": "00:00", "endTime": "01:00", "timeZone": "America/Toronto"}}, {"op": "add", "path": "/spec/disruptionPolicy/allowedDisruptionWindows/-", "value": {"name": "extra2", "daysOfWeek": ["Tuesday"], "startTime": "00:00", "endTime": "01:00", "timeZone": "America/Toronto"}}]`
			cmd := exec.Command("kubectl", "patch", "workloadclass", "critical-batch", "-n", "sample", "--type", "json", "-p", patch)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the Guardrail controller reconciles the WorkloadClass and updates its status")

			verifyWorkloadClassValidated := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].status}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("False"))

				cmd = exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].reason}")
				outReason, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(outReason).To(Equal("ValidationFailed"))
			}
			Eventually(verifyWorkloadClassValidated, 2*time.Minute, 2*time.Second).Should(Succeed())
		})
		It("should fail validation of a WorkloadClass with a day that is not in the allowedDisruptionDays", func() {
			By("Updating the WorkloadClass with a Monday DisruptionWindow")
			patch := `[{"op": "add", "path": "/spec/disruptionPolicy/allowedDisruptionWindows/-", "value": {"name": "bad-day", "daysOfWeek": ["Monday"], "startTime": "00:00", "endTime": "01:00", "timeZone": "America/Toronto"}}]`
			cmd := exec.Command("kubectl", "patch", "workloadclass", "critical-batch", "-n", "sample", "--type", "json", "-p", patch)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the Guardrail controller reconciles the WorkloadClass and updates its status")

			verifyWorkloadClassValidated := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].status}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("False"))

				cmd = exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].reason}")
				outReason, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(outReason).To(Equal("ValidationFailed"))
			}
			Eventually(verifyWorkloadClassValidated, 2*time.Minute, 2*time.Second).Should(Succeed())
		})
		It("should fail validation of a WorkloadClass with maxNonDisruptionDurationDays greater than guardrail limit", func() {
			By("Updating the WorkloadClass with 31 maxNonDisruptionDurationdays")
			patch := `{"spec": {"disruptionPolicy": {"maxNonDisruptionDurationDays": 31}}}`
			cmd := exec.Command("kubectl", "patch", "workloadclass", "critical-batch", "-n", "sample", "--type", "merge", "-p", patch)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the Guardrail controller reconciles the WorkloadClass and updates its status")

			verifyWorkloadClassValidated := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].status}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("False"))

				cmd = exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].reason}")
				outReason, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(outReason).To(Equal("ValidationFailed"))
			}
			Eventually(verifyWorkloadClassValidated, 2*time.Minute, 2*time.Second).Should(Succeed())
		})
		It("should fail validation of a WorkloadClass with an invalid time zone", func() {
			By("Updating the WorkloadClass with an invalid time zone")
			patch := `{"spec": {"disruptionPolicy": {"allowedDisruptionWindows": [{"name": "weekend-maintenance", "daysOfWeek": ["Saturday", "Sunday"], "startTime": "00:00", "endTime": "04:00", "timeZone": "Invalid/TimeZone"}]}}}`
			cmd := exec.Command("kubectl", "patch", "workloadclass", "critical-batch", "-n", "sample", "--type", "merge", "-p", patch)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the Guardrail controller reconciles the WorkloadClass and updates its status")

			verifyWorkloadClassValidated := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].status}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("False"))

				cmd = exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample", "-o", "jsonpath={.status.conditions[?(@.type=='Validated')].reason}")
				outReason, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(outReason).To(Equal("ValidationFailed"))
			}
			Eventually(verifyWorkloadClassValidated, 2*time.Minute, 2*time.Second).Should(Succeed())
		})
	})
})
