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

	"github.com/gke-labs/workload-class/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("WorkloadClass PDB Reconciliation", Ordered, func() {
	var controllerPodName string
	var namespace = "workload-class-system"
	var metricsRoleBindingName = "workload-class-metrics-binding"
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

		By("Creating a dummy Deployment matching the WorkloadClass selector (retrying until webhook is ready)")
		Eventually(func() error {
			cmd = exec.Command("kubectl", "apply", "-f", "config/samples/dummy_deployment.yaml")
			_, err = utils.Run(cmd)
			return err
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Failed to create test deployment")

		By("Waiting for the Pod to be ready")
		verifyPodReady := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=batch-processor", "-o", "jsonpath={.items[0].status.phase}")
			out, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}
		Eventually(verifyPodReady, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		By("cleaning up the clusterrolebinding")
		cmd := exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("cleaning up the deployment")
		cmd = exec.Command("kubectl", "delete", "deployment", "test-deployment", "-n", "sample", "--ignore-not-found")
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

	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod name")
			podCmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-l", "control-plane=controller-manager", "-o", "jsonpath={.items[0].metadata.name}")
			if podOutput, err := utils.Run(podCmd); err == nil && podOutput != "" {
				controllerPodName = strings.TrimSpace(podOutput)
			}

			By("Fetching controller manager pod logs")
			// Now that we have the name, this will succeed
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
	})
	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)
	Context("PDB Reonciliation", func() {
		// WC/PDB creation/deletion
		It("should create a PDB for a WC that is the namespace default", func() {
			By("Verifying the PDB exists")
			verifyPDBExists := func(g Gomega) {
				pdbName := "workload-critical-batch"
				cmd := exec.Command("kubectl", "get", "pdb", pdbName, "-n", "sample")
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyPDBExists, 2*time.Minute, 2*time.Second).Should(Succeed())
		})

		It("should not create PDBs for other WorkloadClasses in the same namespace when there exists a namespace default", func() {
			By("Verifying that the namespace default WC exists")
			cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that the namespace default PDB exists")
			cmd = exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Creating other WCs in the same namespace")
			yaml := `
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: secondary-wc-e2e-1
  namespace: sample
spec:
  podSelector:
    matchLabels:
      role: unique-role-1
  disruptionPolicy:
    minInitialRunDurationDays: 1
`
			cmd = exec.Command("sh", "-c", fmt.Sprintf("echo '%s' | kubectl apply -f -", yaml))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", "secondary-wc-e2e-1", "-n", "sample", "--ignore-not-found"))
			}()

			By("Verifying that PDBs were not generated for the new WCs")
			Consistently(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-1", "-n", "sample")
				_, err := utils.Run(cmd)
				return err
			}, 10*time.Second, 2*time.Second).Should(HaveOccurred())
		})

		It("should create PDBs for all other WorkloadClasses in a namespace when the namespace default WC is deleted", func() {
			By("Verifying that the namespace default WC exists")
			cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that the namespace default PDB exists")
			cmd = exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Creating other WCs in the same namespace")
			yaml := `
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: secondary-wc-e2e-2
  namespace: sample
spec:
  podSelector:
    matchLabels:
      role: unique-role-2
  disruptionPolicy:
    minInitialRunDurationDays: 1
`
			cmd = exec.Command("sh", "-c", fmt.Sprintf("echo '%s' | kubectl apply -f -", yaml))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", "secondary-wc-e2e-2", "-n", "sample", "--ignore-not-found"))
			}()

			By("Verifying that PDBs were not generated for the new WCs")
			Consistently(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-2", "-n", "sample")
				_, err := utils.Run(cmd)
				return err
			}, 10*time.Second, 2*time.Second).Should(HaveOccurred())

			By("Deleting the default WC")
			cmd = exec.Command("kubectl", "delete", "workloadclass", "critical-batch", "-n", "sample")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDBs are eventually generated for the other WCs in the namespace")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-2", "-n", "sample")
				_, err := utils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
		})

		// Namespace modifications
		It("should create PDBs for all WorkloadClasses in a namespace when the default label is removed from the namespace", func() {
			By("Verifying that the namespace default WC exists")
			cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that the namespace default PDB exists")
			cmd = exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Creating other WCs in the same namespace")
			yaml := `
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: secondary-wc-e2e-3
  namespace: sample
spec:
  podSelector:
    matchLabels:
      role: unique-role-3
  disruptionPolicy:
    minInitialRunDurationDays: 1
`
			cmd = exec.Command("sh", "-c", fmt.Sprintf("echo '%s' | kubectl apply -f -", yaml))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", "secondary-wc-e2e-3", "-n", "sample", "--ignore-not-found"))
				_, _ = utils.Run(exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class=critical-batch", "--overwrite"))
			}()

			By("Verifying that PDBs were not generated for the new WCs")
			Consistently(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-3", "-n", "sample")
				_, err := utils.Run(cmd)
				return err
			}, 10*time.Second, 2*time.Second).Should(HaveOccurred())

			By("Update the namespace, removing the default label")
			cmd = exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class-")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDBs are eventually generated for the other WCs in the namespace")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-3", "-n", "sample")
				_, err := utils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
		})

		It("should delete PDBs when the namespace declares a default", func() {
			By("Update the namespace, removing the default label")
			cmd := exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class-")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", "secondary-wc-e2e-4", "-n", "sample", "--ignore-not-found"))
				_, _ = utils.Run(exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class=critical-batch", "--overwrite"))
			}()

			By("Verifying that the namespace default WC exists")
			cmd = exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Creating other WCs in the same namespace")
			yaml := `
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: secondary-wc-e2e-4
  namespace: sample
spec:
  podSelector:
    matchLabels:
      role: unique-role-4
  disruptionPolicy:
    minInitialRunDurationDays: 1
`
			cmd = exec.Command("sh", "-c", fmt.Sprintf("echo '%s' | kubectl apply -f -", yaml))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDBs WERE generated for the new WCs")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-4", "-n", "sample")
				_, err := utils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By("Update the namespace, adding the default label and setting the value to an existing WC")
			cmd = exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class=critical-batch", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDBs are eventually deleted for the other WCs in the namespace")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-4", "-n", "sample")
				_, err := utils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(HaveOccurred())
		})
	})
})
