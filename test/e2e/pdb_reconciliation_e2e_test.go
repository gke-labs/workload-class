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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	utils "github.com/gke-labs/workload-class/internal/utils"
	testUtils "github.com/gke-labs/workload-class/test/utils"
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
		_, err := testUtils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = testUtils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = testUtils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = testUtils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("Applying a namespace")
		cmd = exec.Command("kubectl", "apply", "-f", "config/samples/sample_namespace.yaml")
		_, err = testUtils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply Namespace")

		By("Applying the WorkloadClassGuardrail sample (retrying until webhook is ready)")
		Eventually(func() error {
			cmd = exec.Command("kubectl", "apply", "-f", "config/samples/workloads_v1_workloadclassguardrail.yaml")
			_, err = testUtils.Run(cmd)
			return err
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Failed to apply WorkloadClassGuardrail")

		By("Applying the WorkloadClass sample")
		cmd = exec.Command("kubectl", "apply", "-f", "config/samples/workloads_v1_workloadclass.yaml")
		_, err = testUtils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply WorkloadClass")

		By("Creating a dummy Deployment matching the WorkloadClass selector (retrying until webhook is ready)")
		Eventually(func() error {
			cmd = exec.Command("kubectl", "apply", "-f", "config/samples/dummy_deployment.yaml")
			_, err = testUtils.Run(cmd)
			return err
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Failed to create test deployment")

		By("Waiting for the Pod to be ready")
		verifyPodReady := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "pods", "-n", "sample", "-l", "role=batch-processor", "-o", "jsonpath={.items[0].status.phase}")
			out, err := testUtils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}
		Eventually(verifyPodReady, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		By("cleaning up the clusterrolebinding")
		cmd := exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
		_, _ = testUtils.Run(cmd)

		By("cleaning up the deployment")
		cmd = exec.Command("kubectl", "delete", "deployment", "test-deployment", "-n", "sample", "--ignore-not-found")
		_, _ = testUtils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = testUtils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = testUtils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = testUtils.Run(cmd)
	})

	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod name")
			podCmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-l", "control-plane=controller-manager", "-o", "jsonpath={.items[0].metadata.name}")
			if podOutput, err := testUtils.Run(podCmd); err == nil && podOutput != "" {
				controllerPodName = strings.TrimSpace(podOutput)
			}

			By("Fetching controller manager pod logs")
			// Now that we have the name, this will succeed
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := testUtils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := testUtils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}
			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := testUtils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
		// Reset the WorkloadClass so the next test may make its own modifications
		By("Resetting the WorkloadClass")
		cmd := exec.Command("kubectl", "apply", "-f", "config/samples/workloads_v1_workloadclass.yaml")
		_, _ = testUtils.Run(cmd)
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
				_, err := testUtils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyPDBExists, 2*time.Minute, 2*time.Second).Should(Succeed())
		})

		It("should not create PDBs for other WorkloadClasses in the same namespace when there exists a namespace default", func() {
			By("Verifying that the namespace default WC exists")
			cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample")
			_, err := testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that the namespace default PDB exists")
			cmd = exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample")
			_, err = testUtils.Run(cmd)
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
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = testUtils.Run(exec.Command("kubectl", "delete", "workloadclass", "secondary-wc-e2e-1", "-n", "sample", "--ignore-not-found"))
			}()

			By("Verifying that PDBs were not generated for the new WCs")
			Consistently(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-1", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 10*time.Second, 2*time.Second).Should(HaveOccurred())
		})

		It("should create PDBs for all other WorkloadClasses in a namespace when the namespace default WC is deleted", func() {
			By("Verifying that the namespace default WC exists")
			cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample")
			_, err := testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that the namespace default PDB exists")
			cmd = exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample")
			_, err = testUtils.Run(cmd)
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
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = testUtils.Run(exec.Command("kubectl", "delete", "workloadclass", "secondary-wc-e2e-2", "-n", "sample", "--ignore-not-found"))
			}()

			By("Verifying that PDBs were not generated for the new WCs")
			Consistently(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-2", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 10*time.Second, 2*time.Second).Should(HaveOccurred())

			By("Deleting the default WC")
			cmd = exec.Command("kubectl", "delete", "workloadclass", "critical-batch", "-n", "sample")
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDBs are eventually generated for the other WCs in the namespace")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-2", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
		})

		// Namespace modifications
		It("should create PDBs for all WorkloadClasses in a namespace when the default label is removed from the namespace", func() {
			By("Verifying that the namespace default WC exists")
			cmd := exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample")
			_, err := testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that the namespace default PDB exists")
			cmd = exec.Command("kubectl", "get", "pdb", "workload-critical-batch", "-n", "sample")
			_, err = testUtils.Run(cmd)
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
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = testUtils.Run(exec.Command("kubectl", "delete", "workloadclass", "secondary-wc-e2e-3", "-n", "sample", "--ignore-not-found"))
				_, _ = testUtils.Run(exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class=critical-batch", "--overwrite"))
			}()

			By("Verifying that PDBs were not generated for the new WCs")
			Consistently(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-3", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 10*time.Second, 2*time.Second).Should(HaveOccurred())

			By("Update the namespace, removing the default label")
			cmd = exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class-")
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDBs are eventually generated for the other WCs in the namespace")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-3", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
		})

		It("should delete PDBs when the namespace declares a default", func() {
			By("Update the namespace, removing the default label")
			cmd := exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class-")
			_, err := testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = testUtils.Run(exec.Command("kubectl", "delete", "workloadclass", "secondary-wc-e2e-4", "-n", "sample", "--ignore-not-found"))
				_, _ = testUtils.Run(exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class=critical-batch", "--overwrite"))
			}()

			By("Verifying that the namespace default WC exists")
			cmd = exec.Command("kubectl", "get", "workloadclass", "critical-batch", "-n", "sample")
			_, err = testUtils.Run(cmd)
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
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDBs WERE generated for the new WCs")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-4", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By("Update the namespace, adding the default label and setting the value to an existing WC")
			cmd = exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class=critical-batch", "--overwrite")
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDBs are eventually deleted for the other WCs in the namespace")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-secondary-wc-e2e-4", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(HaveOccurred())
		})

		It("should not delete a PDB if it has an active lease and should delete upon expiration", func() {
			By("Update the namespace, removing the default label")
			cmd := exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class-")
			_, err := testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = testUtils.Run(exec.Command("kubectl", "delete", "workloadclass", "lease-wc-e2e-1", "-n", "sample", "--ignore-not-found"))
				_, _ = testUtils.Run(exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class=critical-batch", "--overwrite"))
			}()

			By("Creating a WC in the namespace")
			yaml := `
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: lease-wc-e2e-1
  namespace: sample
spec:
  podSelector:
    matchLabels:
      role: unique-lease-role
  disruptionPolicy:
    minInitialRunDurationDays: 1
`
			cmd = exec.Command("sh", "-c", fmt.Sprintf("echo '%s' | kubectl apply -f -", yaml))
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDB WAS generated for the new WC")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-lease-wc-e2e-1", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By("Getting target Pod name and UID")
			cmd = exec.Command("kubectl", "get", "pod", "-l", "role=batch-processor", "-n", "sample", "-o", "jsonpath={.items[0].metadata.name}")
			podNameOut, err := testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			podName := strings.TrimSpace(podNameOut)

			cmd = exec.Command("kubectl", "get", "pod", "-l", "role=batch-processor", "-n", "sample", "-o", "jsonpath={.items[0].metadata.uid}")
			podUIDOut, err := testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			podUID := strings.TrimSpace(podUIDOut)

			By("Annotating the PDB with a valid lease")
			expiration := time.Now().Add(time.Hour).Format(utils.ExpirationFormat)
			cmd = exec.Command("kubectl", "annotate", "pdb", "workload-lease-wc-e2e-1", "-n", "sample",
				fmt.Sprintf("%s=%s", utils.BypassPod, podName),
				fmt.Sprintf("%s=%s", utils.BypassPodUID, podUID),
				fmt.Sprintf("%s=%s", utils.BypassOwner, "e2e-test"),
				fmt.Sprintf("%s=%s", utils.BypassExpiration, expiration),
			)
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Update the namespace, adding the default label and setting the value to an existing WC (should trigger PDB deletion for all other WCs)")
			cmd = exec.Command("kubectl", "label", "ns", "sample", "workloads.gke.io/default-class=critical-batch", "--overwrite")
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that the PDB is NOT deleted because of the active lease")
			Consistently(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-lease-wc-e2e-1", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 20*time.Second, 2*time.Second).Should(Succeed())

			By("Expiring the lease to verify the PDB gets deleted")
			expirationExpired := time.Now().Add(-time.Hour).Format(utils.ExpirationFormat)
			cmd = exec.Command("kubectl", "annotate", "pdb", "workload-lease-wc-e2e-1", "-n", "sample",
				fmt.Sprintf("%s=%s", utils.BypassExpiration, expirationExpired),
				"--overwrite",
			)
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that PDB is eventually deleted now that the lease is expired")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-lease-wc-e2e-1", "-n", "sample")
				_, err := testUtils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(HaveOccurred())
		})
	})
	Context("PDB Defaults overrides and eviction leases", func() {
		It("should demonstrate full lifecycle of PDBs, namespace defaults, and leases", func() {
			testNs := "e2e-complex-" + fmt.Sprintf("%d", time.Now().UnixNano())

			By("Creating a fresh namespace without defaults")
			cmd := exec.Command("kubectl", "create", "ns", testNs)
			_, err := testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = testUtils.Run(exec.Command("kubectl", "delete", "ns", testNs, "--ignore-not-found"))
			}()

			By("Labeling the namespace")
			cmd = exec.Command("kubectl", "label", "ns", testNs, "pod-security.kubernetes.io/enforce=restricted")
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Creating the first WorkloadClass wc-1 (no defaults)")
			wc1Yaml := fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: wc-1
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: test-app
  disruptionPolicy:
    minInitialRunDurationDays: 1
    maxNonDisruptionDurationDays: 14
    allowedDisruptionWindows:
      - name: "never"
        daysOfWeek: [Saturday]
        startTime: "00:00"
        endTime: "00:01"
        timeZone: "America/Toronto"
`, testNs)
			cmd = exec.Command("sh", "-c", fmt.Sprintf("echo '%s' | kubectl apply -f -", wc1Yaml))
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying PDB is generated for wc-1")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-wc-1", "-n", testNs)
				_, err := testUtils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By("Creating pods manually to control their names")
			deployYaml := fmt.Sprintf(`
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: test-sts
  namespace: %s
spec:
  replicas: 2
  selector:
    matchLabels:
      app: test-app
  template:
    metadata:
      labels:
        app: test-app
    spec:
      terminationGracePeriodSeconds: 600
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.9
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
`, testNs)
			cmd = exec.Command("sh", "-c", fmt.Sprintf("echo '%s' | kubectl apply -f -", deployYaml))
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for pods to be ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "-n", testNs, "-l", "app=test-app", "-o", "jsonpath={.items[*].status.phase}")
				out, err := testUtils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				phases := strings.Split(strings.TrimSpace(string(out)), " ")
				g.Expect(len(phases)).To(Equal(2))
				g.Expect(phases[0]).To(Equal("Running"))
				g.Expect(phases[1]).To(Equal("Running"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("Verifying that all pods are protected by the PDB")
			cmd = exec.Command("kubectl", "get", "pods", "-n", testNs, "-l", "app=test-app", "-o", "jsonpath={.items[0].metadata.name}")
			out, err := testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			podToEvict := string(out)

			evictionURL := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/eviction", testNs, podToEvict)
			evictionJSON := fmt.Sprintf(`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"%s","namespace":"%s"}}`, podToEvict, testNs)
			evictionFile := filepath.Join("/tmp", "eviction-complex1.json")
			_ = os.WriteFile(evictionFile, []byte(evictionJSON), 0644)

			cmd = exec.Command("kubectl", "create", "--raw", evictionURL, "-f", evictionFile)
			out, err = testUtils.Run(cmd)
			Expect(err).To(HaveOccurred())
			Expect(string(out)).To(ContainSubstring("admission webhook \"vpoddisruption.gke.io\" denied the request: Eviction blocked"))

			By("Creating another WorkloadClass (wc-2) in the same namespace")
			wc2Yaml := fmt.Sprintf(`
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: wc-2
  namespace: %s
spec:
  podSelector:
    matchLabels:
      app: other-app
  disruptionPolicy:
    minInitialRunDurationDays: 1
    maxNonDisruptionDurationDays: 14
    allowedDisruptionsOutsideOfWindow:
      - kind: ServiceAccount
        name: cluster-autoscaler
        namespace: kube-system
    allowedDisruptionWindows:
      - name: "never"
        daysOfWeek: [Saturday]
        startTime: "00:00"
        endTime: "00:01"
        timeZone: "America/Toronto"
`, testNs)
			cmd = exec.Command("sh", "-c", fmt.Sprintf("echo '%s' | kubectl apply -f -", wc2Yaml))
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying PDB is generated for wc-2")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-wc-2", "-n", testNs)
				_, err := testUtils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By("Marking wc-2 as the namespace default")
			cmd = exec.Command("kubectl", "label", "ns", testNs, "workloads.gke.io/default-class=wc-2", "--overwrite")
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying wc-1's PDB is deleted")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-wc-1", "-n", testNs)
				_, err := testUtils.Run(cmd)
				return err
			}, 2*time.Minute, 2*time.Second).Should(HaveOccurred())

			By("Verifying the pods are still protected (now by wc-2/default PDB)")
			cmd = exec.Command("kubectl", "create", "--raw", evictionURL, "-f", evictionFile)
			out, err = testUtils.Run(cmd)
			Expect(err).To(HaveOccurred())
			Expect(string(out)).To(ContainSubstring("admission webhook \"vpoddisruption.gke.io\" denied the request: Eviction blocked"))

			By("Evicting the pod as the allowed user (cluster-autoscaler)")
			Eventually(func() error {
				cmd = exec.Command("kubectl", "create", "--raw", evictionURL, "-f", evictionFile, "--as=system:serviceaccount:kube-system:cluster-autoscaler")
				_, err = testUtils.Run(cmd)
				return err
			}, time.Minute, 2*time.Second).Should(Succeed())

			By("Waiting for the PDB lease to be cleaned up because the Pod is now Terminating with DeletionTimestamp")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", "workload-wc-2", "-n", testNs, "-o", "jsonpath={.metadata.annotations}")
				anOut, err := testUtils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(anOut).NotTo(ContainSubstring(podToEvict))
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By("Simulating full pod removal before 600s")
			cmd = exec.Command("kubectl", "delete", "pod", podToEvict, "-n", testNs, "--force", "--grace-period=0")
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Ensure StatefulSet automatically recreates the pod and it is ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", podToEvict, "-n", testNs, "-o", "jsonpath={.status.phase}")
				anOut, err := testUtils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(anOut).To(Equal("Running"))
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By("Attempting to evict it using someone else's lease should fail")
			// Get the NEW Pod's UID
			cmd = exec.Command("kubectl", "get", "pod", podToEvict, "-n", testNs, "-o", "jsonpath={.metadata.uid}")
			newUID, _ := testUtils.Run(cmd)

			// Hack the PDB to insert a lease that belongs to someone else but is for the CURRENT pod UID
			// The controller will see it is for the current pod UID and NOT delete it.
			// Then the webhook will see it belongs to someone else and DENY the eviction.
			expiration := time.Now().Add(time.Hour).Format(utils.ExpirationFormat)
			cmd = exec.Command("kubectl", "annotate", "pdb", "workload-wc-2", "-n", testNs,
				fmt.Sprintf("%s=%s", utils.BypassPod, podToEvict),
				fmt.Sprintf("%s=%s", utils.BypassPodUID, string(newUID)),
				fmt.Sprintf("%s=%s", utils.BypassExpiration, expiration),
				fmt.Sprintf("%s=%s", utils.BypassOwner, "system:serviceaccount:kube-system:someone-else"),
				"--overwrite",
			)
			_, err = testUtils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			// Wait a brief moment to ensure the Webhook's informer cache synchronizes the annotation.
			time.Sleep(3 * time.Second)

			Eventually(func(g Gomega) {
				cmd = exec.Command("kubectl", "create", "--raw", evictionURL, "-f", evictionFile, "--as=system:serviceaccount:kube-system:cluster-autoscaler")
				out, err := testUtils.Run(cmd)
				g.Expect(err).To(HaveOccurred())
				g.Expect(string(out)).To(ContainSubstring("Disruption denied, PDB workload-wc-2 has an ongoing lease"))
			}, 15*time.Second, 1*time.Second).Should(Succeed())
		})
	})
})
