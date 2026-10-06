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

const (
	spotGuardrailYAML = `
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: default
spec:
  constraints:
    disruption:
      allowedDisruptionDays:
        - Saturday
        - Sunday
      maxAllowedWindows: 2
      maxNonDisruptionDurationDays: 30
  pluginConstraints:
    placement:
      - pluginName: gke-spot-placement
        parameters:
          enforcementMode: Required
          minSpotRatio: "60%"
          fallback:
            allowFallbackToOnDemand: true
            maxFallbackRatio: "25%"
          reversion:
            requiredReversionAction: Active
            maxFallbackDuration: "2h"
`

	validSpotPolicyYAML = `
apiVersion: workloads.gke.io/v1
kind: GKESpotPlacementPolicy
metadata:
  name: gke-t2d-spot
spec:
  customComputeClass: t2d-spot
  fallbackAllowedMachineFamilies:
    - e2
    - n2d
  reversion:
    action: Active
    minDurationOnFallback: "30m"
`

	invalidSpotPolicyYAML = `
apiVersion: workloads.gke.io/v1
kind: GKESpotPlacementPolicy
metadata:
  name: gke-t2d-spot
spec:
  customComputeClass: t2d-spot
  fallbackAllowedMachineFamilies:
    - e2
  reversion:
    action: Lazy
`

	validSpotWorkloadClassYAML = `
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: spot-batch
  namespace: sample
spec:
  podSelector:
    matchLabels:
      role: spot-batch-processor
  disruptionPolicy:
    allowedDisruptionWindows:
      - name: weekend-maintenance
        daysOfWeek:
          - Saturday
          - Sunday
        startTime: "00:00"
        endTime: "04:00"
        timeZone: America/Toronto
    minInitialRunDurationDays: 2
    maxNonDisruptionDurationDays: 1
  capacityStrategy:
    type: Spot
    spotRatio: "80%"
    action: FallbackToOnDemand
  infrastructureProfileRef:
    group: workloads.gke.io
    kind: GKESpotPlacementPolicy
    name: gke-t2d-spot
`
)

func applyManifest(manifest string) error {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	return err
}

func getConditionField(resource, name, ns, condType, field string) (string, error) {
	args := []string{"get", resource, name}
	if ns != "" {
		args = append(args, "-n", ns)
	}
	jsonPath := fmt.Sprintf("jsonpath={.status.conditions[?(@.type=='%s')].%s}", condType, field)
	args = append(args, "-o", jsonPath)
	cmd := exec.Command("kubectl", args...)
	out, err := utils.Run(cmd)
	return strings.TrimSpace(out), err
}

var _ = Describe("WorkloadClass and GKESpotPlacementPolicy Handshake", Ordered, func() {
	var controllerPodName string

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

		By("Applying sample namespace")
		cmd = exec.Command("kubectl", "apply", "-f", "config/samples/sample_namespace.yaml")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply sample namespace")

		By("Waiting for webhook to be ready by applying the WorkloadClassGuardrail")
		Eventually(func() error {
			return applyManifest(spotGuardrailYAML)
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Failed to apply WorkloadClassGuardrail")
	})

	AfterAll(func() {
		By("cleaning up test resources")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", "spot-batch", "-n", "sample", "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "gkespotplacementpolicy", "gke-t2d-spot", "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrail", "default", "--ignore-not-found"))

		By("undeploying the controller-manager")
		cmd := exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)
	})

	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			podCmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-l", "control-plane=controller-manager", "-o", "jsonpath={.items[0].metadata.name}")
			if podOutput, err := utils.Run(podCmd); err == nil && podOutput != "" {
				controllerPodName = strings.TrimSpace(podOutput)
			}

			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			if controllerLogs, err := utils.Run(cmd); err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			if eventsOutput, err := utils.Run(cmd); err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}
		}

		By("Cleaning up WorkloadClass and GKESpotPlacementPolicy between tests")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", "spot-batch", "-n", "sample", "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "gkespotplacementpolicy", "gke-t2d-spot", "--ignore-not-found"))

		By("Resetting WorkloadClassGuardrail to default spot constraints")
		Expect(applyManifest(spotGuardrailYAML)).To(Succeed())
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Spot Placement Policy Handshake", func() {
		It("should attach the plugin and validate WorkloadClass when guardrail, spot policy, and capacity strategy are valid", func() {
			By("Creating a valid GKESpotPlacementPolicy")
			Expect(applyManifest(validSpotPolicyYAML)).To(Succeed())

			By("Verifying the GKESpotPlacementPolicy is Validated=True")
			Eventually(func(g Gomega) {
				status, err := getConditionField("gkespotplacementpolicy", "gke-t2d-spot", "", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("True"))

				reason, err := getConditionField("gkespotplacementpolicy", "gke-t2d-spot", "", "Validated", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).To(Equal("ValidationPassed"))
			}).Should(Succeed())

			By("Creating a WorkloadClass with valid CapacityStrategy referencing the GKESpotPlacementPolicy")
			Expect(applyManifest(validSpotWorkloadClassYAML)).To(Succeed())

			By("Verifying PlacementPluginAttached=True and Validated=True on the WorkloadClass")
			Eventually(func(g Gomega) {
				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("True"))

				attachedReason, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedReason).To(Equal("ProfileResolved"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("True"))

				validatedReason, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedReason).To(Equal("ValidationPassed"))
			}).Should(Succeed())
		})

		It("should set PlacementPluginAttached=False when spot policy is invalid, and update to True once the spot policy becomes valid", func() {
			By("Creating an invalid GKESpotPlacementPolicy (reversion action Lazy violates guardrail RequiredReversionAction Active)")
			Expect(applyManifest(invalidSpotPolicyYAML)).To(Succeed())

			By("Verifying the GKESpotPlacementPolicy is Validated=False")
			Eventually(func(g Gomega) {
				status, err := getConditionField("gkespotplacementpolicy", "gke-t2d-spot", "", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("False"))

				reason, err := getConditionField("gkespotplacementpolicy", "gke-t2d-spot", "", "Validated", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).To(Equal("ValidationFailed"))
			}).Should(Succeed())

			By("Creating a WorkloadClass with a valid CapacityStrategy referencing the invalid GKESpotPlacementPolicy")
			Expect(applyManifest(validSpotWorkloadClassYAML)).To(Succeed())

			By("Verifying PlacementPluginAttached=False and Validated=False on the WorkloadClass")
			Eventually(func(g Gomega) {
				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("False"))

				attachedReason, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedReason).To(Equal("PluginResolutionFailed"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("False"))

				validatedReason, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedReason).To(Equal("ValidationFailed"))
			}).Should(Succeed())

			By("Updating the GKESpotPlacementPolicy to be valid")
			Expect(applyManifest(validSpotPolicyYAML)).To(Succeed())

			By("Verifying the GKESpotPlacementPolicy becomes Validated=True and the WorkloadClass transitions to PlacementPluginAttached=True and Validated=True")
			Eventually(func(g Gomega) {
				policyStatus, err := getConditionField("gkespotplacementpolicy", "gke-t2d-spot", "", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(policyStatus).To(Equal("True"))

				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("True"))

				attachedReason, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedReason).To(Equal("ProfileResolved"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("True"))

				validatedReason, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedReason).To(Equal("ValidationPassed"))
			}).Should(Succeed())
		})

		It("should handle a missing GKESpotPlacementPolicy, attach when created, and detach when deleted", func() {
			By("Creating a WorkloadClass referencing a non-existent GKESpotPlacementPolicy")
			Expect(applyManifest(validSpotWorkloadClassYAML)).To(Succeed())

			By("Verifying PlacementPluginAttached=False with PluginResolutionFailed and Validated=False")
			Eventually(func(g Gomega) {
				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("False"))

				attachedReason, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedReason).To(Equal("PluginResolutionFailed"))

				attachedMsg, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "message")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedMsg).To(ContainSubstring("not found"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("False"))
			}).Should(Succeed())

			By("Creating the referenced GKESpotPlacementPolicy")
			Expect(applyManifest(validSpotPolicyYAML)).To(Succeed())

			By("Verifying PlacementPluginAttached=True and Validated=True after policy creation")
			Eventually(func(g Gomega) {
				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("True"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("True"))
			}).Should(Succeed())

			By("Deleting the referenced GKESpotPlacementPolicy")
			_, err := utils.Run(exec.Command("kubectl", "delete", "gkespotplacementpolicy", "gke-t2d-spot"))
			Expect(err).NotTo(HaveOccurred())

			By("Verifying PlacementPluginAttached=False and Validated=False after policy deletion")
			Eventually(func(g Gomega) {
				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("False"))

				attachedReason, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedReason).To(Equal("PluginResolutionFailed"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("False"))
			}).Should(Succeed())
		})

		It("should keep PlacementPluginAttached=True but mark Validated=False when WorkloadClass CapacityStrategy violates guardrail, then pass when fixed", func() {
			By("Creating a valid GKESpotPlacementPolicy")
			Expect(applyManifest(validSpotPolicyYAML)).To(Succeed())

			By("Creating a WorkloadClass with valid CapacityStrategy referencing the GKESpotPlacementPolicy")
			Expect(applyManifest(validSpotWorkloadClassYAML)).To(Succeed())

			By("Patching WorkloadClass CapacityStrategy with spotRatio below guardrail minSpotRatio (20% < 60%)")
			patch := `{"spec": {"capacityStrategy": {"spotRatio": "20%"}}}`
			_, err := utils.Run(exec.Command("kubectl", "patch", "workloadclass", "spot-batch", "-n", "sample", "--type", "merge", "-p", patch))
			Expect(err).NotTo(HaveOccurred())

			By("Verifying PlacementPluginAttached=True while Validated=False due to CapacityStrategy violation")
			Eventually(func(g Gomega) {
				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("True"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("False"))

				validatedMsg, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "message")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedMsg).To(ContainSubstring("spotRatio 20% is less than guardrail minSpotRatio 60%"))
			}).Should(Succeed())

			By("Patching WorkloadClass CapacityStrategy back to a valid spotRatio (80%)")
			patchValid := `{"spec": {"capacityStrategy": {"spotRatio": "80%"}}}`
			_, err = utils.Run(exec.Command("kubectl", "patch", "workloadclass", "spot-batch", "-n", "sample", "--type", "merge", "-p", patchValid))
			Expect(err).NotTo(HaveOccurred())

			By("Verifying both PlacementPluginAttached=True and Validated=True")
			Eventually(func(g Gomega) {
				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("True"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("True"))
			}).Should(Succeed())
		})

		It("should re-evaluate both GKESpotPlacementPolicy and WorkloadClass when WorkloadClassGuardrail is updated", func() {
			By("Creating a valid GKESpotPlacementPolicy and WorkloadClass")
			Expect(applyManifest(validSpotPolicyYAML)).To(Succeed())
			Expect(applyManifest(validSpotWorkloadClassYAML)).To(Succeed())

			By("Verifying initial state is PlacementPluginAttached=True and Validated=True")
			Eventually(func(g Gomega) {
				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("True"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("True"))
			}).Should(Succeed())

			By("Updating the WorkloadClassGuardrail to forbid Spot placement")
			forbiddenGuardrailYAML := `
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: default
spec:
  constraints:
    disruption:
      allowedDisruptionDays:
        - Saturday
        - Sunday
      maxAllowedWindows: 2
      maxNonDisruptionDurationDays: 30
  pluginConstraints:
    placement:
      - pluginName: gke-spot-placement
        parameters:
          enforcementMode: Forbidden
`
			Expect(applyManifest(forbiddenGuardrailYAML)).To(Succeed())

			By("Verifying GKESpotPlacementPolicy becomes Validated=False and WorkloadClass becomes PlacementPluginAttached=False and Validated=False")
			Eventually(func(g Gomega) {
				policyStatus, err := getConditionField("gkespotplacementpolicy", "gke-t2d-spot", "", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(policyStatus).To(Equal("False"))

				attachedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedStatus).To(Equal("False"))

				attachedReason, err := getConditionField("workloadclass", "spot-batch", "sample", "PlacementPluginAttached", "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(attachedReason).To(Equal("PluginResolutionFailed"))

				validatedStatus, err := getConditionField("workloadclass", "spot-batch", "sample", "Validated", "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(validatedStatus).To(Equal("False"))
			}).Should(Succeed())
		})
	})
})
