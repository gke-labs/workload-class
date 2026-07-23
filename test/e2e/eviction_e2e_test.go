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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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
			cmd = exec.Command("kubectl", "apply", "-f", "config/samples/dummy_deployment.yaml")
			_, err = utils.Run(cmd)
			return err
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Failed to create test pod")

		By("Waiting for the Pod to be ready")
		verifyPodReady := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=batch-processor", "-o", "jsonpath={.items[0].status.phase}")
			out, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}
		Eventually(verifyPodReady, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the deployment")
		cmd := exec.Command("kubectl", "delete", "deployment", "test-deployment", "-n", "sample", "--ignore-not-found")
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
			By("Fetching controller manager pod name")
			// Depending on your kubebuilder setup, the label might be 'control-plane=controller-manager'
			// or 'app.kubernetes.io/name=workload-class'. We grab the first matching pod name.
			podCmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-l", "control-plane=controller-manager", "-o", "jsonpath={.items[0].metadata.name}")
			if podOutput, err := utils.Run(podCmd); err == nil && podOutput != "" {
				controllerPodName = strings.TrimSpace(podOutput)
			}

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

		// Forcefully delete any existing pods for the deployment; the deployment controller will automatically spin up a fresh one
		By("Ensuring a fresh dummy pod exists")
		cmd = exec.Command("kubectl", "delete", "pods", "-n", "sample", "-l", "role=batch-processor", "--force", "--grace-period=0", "--ignore-not-found")
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "apply", "-f", "config/samples/dummy_deployment.yaml")
		_, _ = utils.Run(cmd)

		verifyPodReady := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=batch-processor", "-o", "jsonpath={.items[0].status.phase}")
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

			By("Validating the PDB allows disruptions")
			Eventually(func() (string, error) {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample", "-o", "jsonpath={.spec.maxUnavailable}")
				output, err := utils.Run(cmd)
				if err != nil {
					return "", err
				}
				return strings.TrimSpace(output), nil
			}, time.Minute, 2*time.Second).Should(Equal("100%"), "The PDB should allow 100% unavailable pods within the disruption window")

			By("Fetching the dynamically generated pod name")
			cmd = exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=batch-processor", "-o", "jsonpath={.items[0].metadata.name}")
			podNameOutput, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to get the pod name")
			podName := strings.TrimSpace(podNameOutput)
			Expect(podName).NotTo(BeEmpty())

			By("Attempting to evict the Pod via the eviction subresource")
			evictionURL := fmt.Sprintf("/api/v1/namespaces/sample/pods/%s/eviction", podName)
			evictionJSON := fmt.Sprintf(`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"%s","namespace":"sample"}}`, podName)
			evictionFile := filepath.Join("/tmp", "eviction.json")
			err = os.WriteFile(evictionFile, []byte(evictionJSON), 0644)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				cmd := exec.Command("kubectl", "create", "--raw", evictionURL, "-f", evictionFile)
				_, err := utils.Run(cmd)
				return err
			}, time.Minute, 2*time.Second).Should(Succeed(), "Eviction should eventually succeed within the disruption window")
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
			nameCmd := exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=batch-processor", "-o", "jsonpath={.items[0].metadata.name}")
			podNameOutput, err := utils.Run(nameCmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to fetch dynamic pod name")
			podName := strings.TrimSpace(podNameOutput)

			evictionURL := fmt.Sprintf("/api/v1/namespaces/sample/pods/%s/eviction", podName)
			evictionJSON := fmt.Sprintf(`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"%s","namespace":"sample"}}`, podName)
			evictionFile := filepath.Join("/tmp", "eviction.json")
			err = os.WriteFile(evictionFile, []byte(evictionJSON), 0644)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "create", "--raw", evictionURL, "-f", evictionFile)
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

			By("Verifying the PDB is generated, fully synced by the PDB controller, and blocks disruptions outside the window")
			Eventually(func() (bool, error) {
				// We check both maxUnavailable and observedGeneration.
				// In CI environments, kube-apiserver's internal informer cache might lag.
				// Waiting for observedGeneration ensures the Kubernetes PDB controller has processed it,
				// giving the apiserver's cache time to sync, preventing the eviction API from missing the PDB.
				cmd := exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample", "-o", "jsonpath={.spec.maxUnavailable},{.status.observedGeneration}")
				output, err := utils.Run(cmd)
				if err != nil {
					return false, err
				}
				parts := strings.Split(strings.TrimSpace(output), ",")
				if len(parts) == 2 && parts[0] == "0" && parts[1] != "" {
					return true, nil
				}
				return false, nil
			}, time.Minute, 2*time.Second).Should(BeTrue(), "The PDB should have maxUnavailable=0 and a populated observedGeneration")

			By("Setting up RBAC so the impersonated autoscaler has permission to call the eviction API")
			rbacCmd := exec.Command("kubectl", "create", "clusterrolebinding", "test-autoscaler-admin",
				"--clusterrole=cluster-admin",
				"--user=system:serviceaccount:kube-system:cluster-autoscaler")
			_ = rbacCmd.Run() // Ignore error in case the binding already exists from previous runs

			By("Attempting to evict the Pod via the eviction subresource as the cluster autoscaler")
			nameCmd := exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=batch-processor", "-o", "jsonpath={.items[0].metadata.name}")
			podNameOutput, err := utils.Run(nameCmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to fetch dynamic pod name")
			podName := strings.TrimSpace(podNameOutput)

			evictionURL := fmt.Sprintf("/api/v1/namespaces/sample/pods/%s/eviction", podName)
			evictionJSON := fmt.Sprintf(`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"%s","namespace":"sample"}}`, podName)
			evictionFile := filepath.Join("/tmp", "eviction.json")
			err = os.WriteFile(evictionFile, []byte(evictionJSON), 0644)
			Expect(err).NotTo(HaveOccurred())

			// Append the --as flag to impersonate the autoscaler for the raw API request
			// Because the cluster-autoscaler is in the allowedDisruptionsOutsideOfWindow list,
			// the webhook should NOT block the request, and err should be nil.
			Eventually(func() error {
				cmd = exec.Command("kubectl", "create", "--raw", evictionURL, "-f", evictionFile, "--as=system:serviceaccount:kube-system:cluster-autoscaler")
				_, err := utils.Run(cmd)
				return err
			}, "10s", "1s").ShouldNot(HaveOccurred(), "Eviction should be permitted for the autoscaler despite being outside the window")
		})

		It("should protect all pods in a namespace if a default WorkloadClass is configured", func() {
			notToday := time.Now().UTC().AddDate(0, 0, 2).Weekday().String()

			By("Creating a second non-default WorkloadClass")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: non-default-batch
  namespace: sample
spec:
  podSelector:
    matchLabels:
      role: non-default-processor
  disruptionPolicy:
    minInitialRunDurationDays: 0
    maxNonDisruptionDurationDays: 10
    allowedDisruptionWindows:
      - name: weekend-maintenance
        daysOfWeek:
          - ` + notToday + `
        startTime: "00:00"
        endTime: "23:59"
        timeZone: "Etc/UTC"
`)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			defer func() {
				By("Cleaning up the second WorkloadClass")
				cleanupCmd := exec.Command("kubectl", "delete", "workloadclass", "non-default-batch", "-n", "sample", "--ignore-not-found")
				_, _ = utils.Run(cleanupCmd)
			}()

			By("Patching the original WorkloadClass to simulate being outside a disruption window")
			workloadPatch := fmt.Sprintf(`{"spec": {"disruptionPolicy": {"maxNonDisruptionDurationDays": 10, "minInitialRunDurationDays": 0, "allowedDisruptionWindows": [{"name": "weekend-maintenance", "daysOfWeek": ["%s"], "startTime": "00:00", "endTime": "23:59", "timeZone": "Etc/UTC"}]}}}`, notToday)
			cmd = exec.Command("kubectl", "patch", "workloadclass", "critical-batch", "-n", "sample", "--type", "merge", "-p", workloadPatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Labeling the namespace to make critical-batch the default WorkloadClass")
			cmd = exec.Command("kubectl", "label", "namespace", "sample", "workloads.gke.io/default-class=critical-batch", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			defer func() {
				By("Cleaning up the namespace label")
				cleanupCmd := exec.Command("kubectl", "label", "namespace", "sample", "workloads.gke.io/default-class-", "--overwrite")
				_, _ = utils.Run(cleanupCmd)
			}()

			By("Patching the Guardrail to allow tomorrow")
			guardrailPatch := fmt.Sprintf(`{"spec": {"constraints": {"disruption": {"allowedDisruptionDays": ["%s"], "maxNonDisruptionDurationDays": 30}}}}`, notToday)
			cmd = exec.Command("kubectl", "patch", "workloadclassguardrail", "default", "--type", "merge", "-p", guardrailPatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Validating that ONLY the PDB for the default WorkloadClass exists")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-non-default-batch", "-n", "sample")
				_, err := utils.Run(cmd)
				if err == nil {
					return fmt.Errorf("PDB workload-non-default-batch still exists")
				}
				return nil
			}, time.Minute, 2*time.Second).Should(Succeed(), "The non-default WorkloadClass PDB should be deleted")

			By("Validating that the default WorkloadClass PDB has no label selectors and maxUnavailable is 0")
			Eventually(func() (bool, error) {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample", "-o", "jsonpath={.spec.selector},{.spec.maxUnavailable},{.status.observedGeneration}")
				output, err := utils.Run(cmd)
				if err != nil {
					return false, err
				}
				parts := strings.Split(strings.TrimSpace(output), ",")
				if len(parts) == 3 && parts[0] == "{}" && parts[1] == "0" && parts[2] != "" {
					return true, nil
				}
				return false, nil
			}, time.Minute, 2*time.Second).Should(BeTrue(), "The default WorkloadClass PDB should have an empty label selector and maxUnavailable=0")

			By("Attempting to evict a Pod that matches the original critical-batch WorkloadClass")
			nameCmd := exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=batch-processor", "-o", "jsonpath={.items[0].metadata.name}")
			podNameOutput, err := utils.Run(nameCmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to fetch dynamic pod name")
			podName := strings.TrimSpace(podNameOutput)

			evictionURL := fmt.Sprintf("/api/v1/namespaces/sample/pods/%s/eviction", podName)
			evictionJSON := fmt.Sprintf(`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"%s","namespace":"sample"}}`, podName)
			evictionFile := filepath.Join("/tmp", "eviction.json")
			err = os.WriteFile(evictionFile, []byte(evictionJSON), 0644)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "create", "--raw", evictionURL, "-f", evictionFile)
			out, err := utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "Eviction should be blocked by the default WorkloadClass policy")
			Expect(string(out)).To(ContainSubstring("Eviction blocked"), "Expected webhook to deny the request")

			By("Creating a new Pod that matches the non-default WorkloadClass")
			cmd = exec.Command("kubectl", "create", "deployment", "non-default-pod", "-n", "sample", "--image=nginx", "--replicas=1")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			// add the role label to the deployment's pod template
			cmd = exec.Command("kubectl", "patch", "deployment", "non-default-pod", "-n", "sample", "-p", `{"spec": {"template": {"metadata": {"labels": {"role": "non-default-processor"}}}}}`)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			defer func() {
				By("Cleaning up the non-default deployment")
				cleanupCmd := exec.Command("kubectl", "delete", "deployment", "non-default-pod", "-n", "sample", "--ignore-not-found")
				_, _ = utils.Run(cleanupCmd)
			}()

			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=non-default-processor", "-o", "jsonpath={.items[0].status.phase}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if strings.TrimSpace(out) != "Running" {
					return fmt.Errorf("Pod not running yet")
				}
				return nil
			}, time.Minute, 2*time.Second).Should(Succeed())

			By("Validating that the PDB status updates to cover both pods in the namespace")
			Eventually(func() (string, error) {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample", "-o", "jsonpath={.status.expectedPods}")
				output, err := utils.Run(cmd)
				if err != nil {
					return "", err
				}
				val := strings.TrimSpace(output)
				if val != "2" {
					fmt.Printf("DEBUG: expectedPods is %s. Current pods:\n", val)
					utils.Run(exec.Command("kubectl", "get", "pods", "-n", "sample"))
					utils.Run(exec.Command("kubectl", "get", "deployments", "-n", "sample"))
					utils.Run(exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample", "-o", "yaml"))
				}
				return val, nil
			}, time.Minute, 2*time.Second).Should(Equal("2"), "The PDB should report 2 expected pods, proving it protects all pods in the namespace")

			By("Attempting to evict the non-default Pod")
			nameCmd = exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=non-default-processor", "-o", "jsonpath={.items[0].metadata.name}")
			podNameOutput, err = utils.Run(nameCmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to fetch dynamic non-default pod name")
			nonDefaultPodName := strings.TrimSpace(podNameOutput)

			evictionURL2 := fmt.Sprintf("/api/v1/namespaces/sample/pods/%s/eviction", nonDefaultPodName)
			evictionJSON2 := fmt.Sprintf(`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"%s","namespace":"sample"}}`, nonDefaultPodName)
			evictionFile2 := filepath.Join("/tmp", "eviction2.json")
			err = os.WriteFile(evictionFile2, []byte(evictionJSON2), 0644)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "create", "--raw", evictionURL2, "-f", evictionFile2)
			out, err = utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "Eviction should be blocked by the default WorkloadClass policy (it covers all pods)")
			Expect(string(out)).To(ContainSubstring("Eviction blocked"), "Expected webhook to deny the request")
		})
	})
})
