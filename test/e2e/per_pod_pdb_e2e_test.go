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
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/test/utils"
)

// These specs exercise Per-Pod PodDisruptionBudgets end to end on a Kind cluster: the WorkloadClass controller
// creates one PDB per governed Pod, keeps it in sync with the WorkloadClass, defers to user-created PDBs, hands
// PDBs over between WorkloadClasses, and leases them to subjects allowed to disrupt outside of a window.
//
// The specs share one WorkloadClass and Deployment and run in order. Each spec restores the state it changes.
//
// The PDB, label, and annotation names are hardcoded (rather than imported from the controller) so that these
// tests assert the external contract.
const (
	ppNS = "per-pod-pdb-e2e"

	// ppApp is the workload governed by ppWC. ppOtherApp matches no WorkloadClass selector.
	ppApp      = "web"
	ppOtherApp = "other"
	ppReplicas = 3

	ppWC        = "web"
	ppDefaultWC = "ns-default"

	// ppGuardrailMaxNonDisruptionDays is the guardrail's limit. The WorkloadClasses use a lower value (10).
	ppGuardrailMaxNonDisruptionDays = 30

	ppPDBPrefix        = "workload-"
	ppPodUIDLabel      = "workloads.gke.io/pod-uid"
	ppBypassOwner      = "workloads.gke.io/bypass-owner"
	ppDefaultClassLbl  = "workloads.gke.io/default-class"
	ppAutoscalerUser   = "system:serviceaccount:kube-system:cluster-autoscaler"
	ppVPAUser          = "system:serviceaccount:kube-system:vpa-updater"
	ppClosed           = "0"
	ppOpen             = "100%"
	ppPDBBudgetMessage = "disruption budget"

	ppGuardrailTmpl = `
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: default
spec:
  constraints:
    disruption:
      allowedDisruptionDays: [Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday]
      maxAllowedWindows: 2
      maxNonDisruptionDurationDays: %d
`
)

// ppImpersonatedUsers are the service accounts the specs impersonate to evict Pods. Each is bound to cluster-admin
// for the duration of the suite so that it can call the Eviction API.
var ppImpersonatedUsers = map[string]string{
	"per-pod-pdb-e2e-autoscaler": ppAutoscalerUser,
	"per-pod-pdb-e2e-vpa":        ppVPAUser,
}

// ---------------------------------------------------------------------------
// Manifest builders
// ---------------------------------------------------------------------------

// applyPPGuardrail applies the "default" guardrail. Allowing every day keeps it compatible with any disruption
// window.
func applyPPGuardrail() error {
	_, err := kubectlJSON(fmt.Appendf(nil, ppGuardrailTmpl, ppGuardrailMaxNonDisruptionDays), "apply", "-f", "-")
	return err
}

func ppWindow(day string) workloadsv1.DisruptionWindow {
	return workloadsv1.DisruptionWindow{
		Name:       "e2e-window",
		DaysOfWeek: []string{day},
		StartTime:  "00:00",
		EndTime:    "23:59",
		TimeZone:   "Etc/UTC",
	}
}

// ppPolicy returns a disruption policy whose window is open today (open=true) or two days from now. Outside of the
// window, the cluster autoscaler and VPA may still disrupt Pods.
func ppPolicy(open bool) workloadsv1.DisruptionPolicy {
	day := time.Now().UTC().AddDate(0, 0, 2).Weekday().String()
	if open {
		day = time.Now().UTC().Weekday().String()
	}
	return workloadsv1.DisruptionPolicy{
		AllowedDisruptionWindows:     []workloadsv1.DisruptionWindow{ppWindow(day)},
		MaxNonDisruptionDurationDays: 10,
		AllowedDisruptionsOutsideOfWindow: []workloadsv1.Subject{
			{Kind: "ServiceAccount", Name: "cluster-autoscaler", Namespace: "kube-system"},
			{Kind: "ServiceAccount", Name: "vpa-updater", Namespace: "kube-system"},
		},
	}
}

// newPPWorkloadClass returns a WorkloadClass in ppNS. A nil selector leaves PodSelector unset.
func newPPWorkloadClass(name string, selector map[string]string, open bool) *workloadsv1.WorkloadClass {
	wc := &workloadsv1.WorkloadClass{
		TypeMeta:   metav1.TypeMeta{APIVersion: workloadsv1.GroupVersion.String(), Kind: "WorkloadClass"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ppNS},
		Spec:       workloadsv1.WorkloadClassSpec{DisruptionPolicy: ppPolicy(open)},
	}
	if selector != nil {
		wc.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: selector}
	}
	return wc
}

// applyPPDeployment creates or updates a pause Deployment labeled app=<app> in ppNS.
func applyPPDeployment(app string, replicas int32) {
	GinkgoHelper()
	zero := int64(0)
	d := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: app, Namespace: ppNS},
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
	Expect(applyObject(d)).To(Succeed())
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

// ppActivePods returns the non-terminating Pods labeled app=<app> in ppNS.
func ppActivePods(app string) ([]corev1.Pod, error) {
	out, err := kubectlJSON(nil, "get", "pods", "-n", ppNS, "-l", "app="+app, "-o", "json")
	if err != nil {
		return nil, err
	}
	list := &corev1.PodList{}
	if err := json.Unmarshal(out, list); err != nil {
		return nil, err
	}
	var pods []corev1.Pod
	for _, p := range list.Items {
		if p.DeletionTimestamp == nil && p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
			pods = append(pods, p)
		}
	}
	return pods, nil
}

// ppPDBs returns the PDBs in ppNS by name.
func ppPDBs() (map[string]policyv1.PodDisruptionBudget, error) {
	out, err := kubectlJSON(nil, "get", "pdb", "-n", ppNS, "-o", "json")
	if err != nil {
		return nil, err
	}
	list := &policyv1.PodDisruptionBudgetList{}
	if err := json.Unmarshal(out, list); err != nil {
		return nil, err
	}
	pdbs := make(map[string]policyv1.PodDisruptionBudget, len(list.Items))
	for _, pdb := range list.Items {
		pdbs[pdb.Name] = pdb
	}
	return pdbs, nil
}

// ppController returns the name of the WorkloadClass that controls the PDB, or "" if none does.
func ppController(pdb *policyv1.PodDisruptionBudget) string {
	if owner := metav1.GetControllerOf(pdb); owner != nil && owner.Kind == "WorkloadClass" {
		return owner.Name
	}
	return ""
}

func ppPodNames(pods []corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}

// ppEvictAs evicts the Pod in ppNS through the Eviction API, impersonating the given user.
func ppEvictAs(pod, user string) error {
	eviction, err := json.Marshal(&policyv1.Eviction{
		TypeMeta:   metav1.TypeMeta{APIVersion: "policy/v1", Kind: "Eviction"},
		ObjectMeta: metav1.ObjectMeta{Name: pod, Namespace: ppNS},
	})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/eviction", ppNS, pod)
	_, err = kubectlJSON(eviction, "create", "--raw", url, "-f", "-", "--as="+user)
	return err
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

// expectPerPodPDBs waits until app has n active Pods, each labeled with its UID and covered by its own PDB that
// selects only that Pod, is controlled by the owner WorkloadClass, and has the given maxUnavailable. It returns the
// Pod names.
func expectPerPodPDBs(app, owner, maxUnavailable string, n int) []string {
	GinkgoHelper()
	var names []string
	Eventually(func(g Gomega) {
		pods, err := ppActivePods(app)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods).To(HaveLen(n))
		pdbs, err := ppPDBs()
		g.Expect(err).NotTo(HaveOccurred())

		for _, pod := range pods {
			uid := string(pod.UID)
			g.Expect(pod.Labels).To(HaveKeyWithValue(ppPodUIDLabel, uid), "Pod %s should be labeled with its UID", pod.Name)

			pdb, ok := pdbs[ppPDBPrefix+pod.Name]
			g.Expect(ok).To(BeTrue(), "Pod %s should have a PDB", pod.Name)
			g.Expect(ppController(&pdb)).To(Equal(owner), "PDB %s controller", pdb.Name)
			g.Expect(pdb.Labels).To(HaveKeyWithValue(ppPodUIDLabel, uid), "PDB %s label", pdb.Name)
			g.Expect(pdb.Spec.Selector).NotTo(BeNil())
			g.Expect(pdb.Spec.Selector.MatchLabels).To(Equal(map[string]string{ppPodUIDLabel: uid}), "PDB %s selector", pdb.Name)
			g.Expect(pdb.Spec.Selector.MatchExpressions).To(BeEmpty())
			g.Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
			g.Expect(pdb.Spec.MaxUnavailable.String()).To(Equal(maxUnavailable), "PDB %s maxUnavailable", pdb.Name)
			g.Expect(pdb.Spec.UnhealthyPodEvictionPolicy).NotTo(BeNil())
			g.Expect(*pdb.Spec.UnhealthyPodEvictionPolicy).To(Equal(policyv1.IfHealthyBudget))
			// The disruption controller counts exactly one healthy Pod matching the PDB's selector. (status.expectedPods
			// isn't useful here: with maxUnavailable it reports the scale of the Pod's owning workload, not the
			// number of selected Pods.)
			g.Expect(pdb.Status.CurrentHealthy).To(Equal(int32(1)), "PDB %s should cover exactly one healthy Pod", pdb.Name)
		}
		names = ppPodNames(pods)
	}).Should(Succeed())
	return names
}

// expectNoPerPodPDBs waits until none of app's active Pods has a PDB named after it.
func expectNoPerPodPDBs(app string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		pods, err := ppActivePods(app)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods).NotTo(BeEmpty())
		pdbs, err := ppPDBs()
		g.Expect(err).NotTo(HaveOccurred())
		for _, pod := range pods {
			g.Expect(pdbs).NotTo(HaveKey(ppPDBPrefix+pod.Name), "Pod %s should not have a PDB", pod.Name)
		}
	}).Should(Succeed())
}

// expectNoStalePDBs waits until every WorkloadClass-managed PDB in ppNS belongs to an active Pod with the same UID,
// i.e. PDBs of deleted Pods have been cleaned up.
func expectNoStalePDBs() {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		uids := map[string]string{}
		for _, app := range []string{ppApp, ppOtherApp} {
			pods, err := ppActivePods(app)
			g.Expect(err).NotTo(HaveOccurred())
			for _, p := range pods {
				uids[ppPDBPrefix+p.Name] = string(p.UID)
			}
		}

		pdbs, err := ppPDBs()
		g.Expect(err).NotTo(HaveOccurred())
		for name, pdb := range pdbs {
			if ppController(&pdb) == "" {
				continue
			}
			g.Expect(uids).To(HaveKey(name), "PDB %s should belong to an active Pod", name)
			g.Expect(pdb.Labels[ppPodUIDLabel]).To(Equal(uids[name]), "PDB %s should belong to the current Pod", name)
		}
	}).Should(Succeed())
}

// ---------------------------------------------------------------------------
// Specs
// ---------------------------------------------------------------------------

var _ = Describe("Per-Pod PodDisruptionBudgets", Ordered, func() {
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
			_, err := utils.Run(exec.Command("kubectl", "create", "ns", ppNS))
			return err
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("allowing the impersonated cluster autoscaler and VPA to call the Eviction API")
		for binding, user := range ppImpersonatedUsers {
			_, _ = utils.Run(exec.Command("kubectl", "create", "clusterrolebinding", binding,
				"--clusterrole=cluster-admin", "--user="+user))
		}

		By("applying the guardrail (retrying until the guardrail webhook is ready)")
		Eventually(applyPPGuardrail, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("waiting for the Pod webhooks to serve requests")
		Eventually(func(g Gomega) {
			admitProbePod(g, newPlacementPod("webhook-ready", inNamespace(ppNS)))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("creating a WorkloadClass outside of its disruption window, and its workload")
		Expect(applyObject(newPPWorkloadClass(ppWC, map[string]string{"app": ppApp}, false))).To(Succeed())
		expectWorkloadClassReady(ppWC, ppNS, false)
		applyPPDeployment(ppApp, ppReplicas)

		By("creating a workload that no WorkloadClass selects")
		applyPPDeployment(ppOtherApp, 1)
	})

	AfterAll(func() {
		By("cleaning up test resources")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", ppNS, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrail", "default", "--ignore-not-found"))
		for binding := range ppImpersonatedUsers {
			_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding", binding, "--ignore-not-found"))
		}

		By("undeploying the controller-manager")
		_, _ = utils.Run(exec.Command("make", "undeploy"))

		By("uninstalling CRDs")
		_, _ = utils.Run(exec.Command("make", "uninstall"))

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
		By("Fetching WorkloadClasses, Pods, and PDBs")
		if out, err := utils.Run(exec.Command("kubectl", "get", "workloadclass,pods,pdb", "-n", ppNS,
			"-o", "wide", "--show-labels")); err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "%s\n", out)
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("creates one closed PDB per governed Pod, covering only that Pod", func() {
		By("verifying each governed Pod has its own closed PDB")
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)

		By("verifying Pods that no WorkloadClass governs have no PDB")
		expectNoPerPodPDBs(ppOtherApp)
	})

	It("opens and closes the PDBs with the WorkloadClass' disruption window", func() {
		By("moving the disruption window to today")
		Expect(applyObject(newPPWorkloadClass(ppWC, map[string]string{"app": ppApp}, true))).To(Succeed())
		expectPerPodPDBs(ppApp, ppWC, ppOpen, ppReplicas)

		By("moving the disruption window out of today")
		Expect(applyObject(newPPWorkloadClass(ppWC, map[string]string{"app": ppApp}, false))).To(Succeed())
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)
	})

	It("replaces a deleted Pod's PDB and follows the workload as it scales", func() {
		pods := expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)

		By("deleting a Pod")
		deleted := pods[0]
		_, err := utils.Run(exec.Command("kubectl", "delete", "pod", deleted, "-n", ppNS, "--wait=false"))
		Expect(err).NotTo(HaveOccurred())

		By("verifying its PDB is deleted and its replacement gets a PDB")
		Eventually(func(g Gomega) {
			pdbs, err := ppPDBs()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pdbs).NotTo(HaveKey(ppPDBPrefix + deleted))
		}).Should(Succeed())
		Expect(expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)).NotTo(ContainElement(deleted))
		expectNoStalePDBs()

		By("scaling the workload up")
		applyPPDeployment(ppApp, ppReplicas+2)
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas+2)

		By("scaling the workload back down")
		applyPPDeployment(ppApp, ppReplicas)
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)
		expectNoStalePDBs()

		By("completing a one-shot Pod and verifying its Per-Pod PDB is cleaned up")
		jobPod := createExistingPod(newPlacementPod(ppApp, inNamespace(ppNS), asPendingOnDemandPod()))
		DeferCleanup(func() { forceDeletePod(ppNS, jobPod.Name) })
		Eventually(func(g Gomega) {
			pdbs, err := ppPDBs()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pdbs).To(HaveKey(ppPDBPrefix + jobPod.Name))
		}).Should(Succeed())

		_, err = kubectlJSON(nil, "patch", "pod", jobPod.Name, "-n", ppNS,
			"--subresource=status", "--type=merge", "-p", `{"status":{"phase":"Succeeded"}}`)
		Expect(err).NotTo(HaveOccurred())
		expectNoStalePDBs()
		forceDeletePod(ppNS, jobPod.Name)
	})

	It("defers to a user-created PDB that covers the Pods, and reclaims them when it is removed", func() {
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)

		By("creating a user PDB that selects the workload")
		minAvailable := intstr.FromInt32(1)
		userPDB := &policyv1.PodDisruptionBudget{
			TypeMeta:   metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
			ObjectMeta: metav1.ObjectMeta{Name: "user-web", Namespace: ppNS},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minAvailable,
				Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": ppApp}},
			},
		}
		Expect(applyObject(userPDB)).To(Succeed())

		By("verifying the WorkloadClass deletes its PDBs for the covered Pods")
		expectNoPerPodPDBs(ppApp)

		By("verifying the user PDB is left untouched")
		Consistently(func(g Gomega) {
			pdbs, err := ppPDBs()
			g.Expect(err).NotTo(HaveOccurred())
			pdb, ok := pdbs["user-web"]
			g.Expect(ok).To(BeTrue())
			g.Expect(ppController(&pdb)).To(BeEmpty())
			g.Expect(pdb.Labels).NotTo(HaveKey(ppPodUIDLabel))
			g.Expect(pdb.Spec.MinAvailable.String()).To(Equal("1"))
		}, consistentlyDuration, consistentlyInterval).Should(Succeed())

		By("deleting the user PDB")
		_, err := utils.Run(exec.Command("kubectl", "delete", "pdb", "user-web", "-n", ppNS))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the WorkloadClass recreates the Pods' PDBs")
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)
	})

	It("opens the PDBs instead of deleting them while the WorkloadClass fails guardrail validation", func() {
		pods := expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)
		valid := newPPWorkloadClass(ppWC, map[string]string{"app": ppApp}, false)
		DeferCleanup(func() { Expect(applyObject(valid)).To(Succeed()) })

		By("raising the WorkloadClass' maxNonDisruptionDurationDays above the guardrail's limit")
		invalid := newPPWorkloadClass(ppWC, map[string]string{"app": ppApp}, false)
		invalid.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = ppGuardrailMaxNonDisruptionDays + 10
		Expect(applyObject(invalid)).To(Succeed())
		expectWorkloadClassCondition(ppWC, ppNS, workloadsv1.ConditionTypeValidated, "False")

		By("verifying the same PDBs are opened rather than deleted")
		Expect(expectPerPodPDBs(ppApp, ppWC, ppOpen, ppReplicas)).To(ConsistOf(pods))

		By("restoring a valid WorkloadClass")
		Expect(applyObject(valid)).To(Succeed())
		expectWorkloadClassCondition(ppWC, ppNS, workloadsv1.ConditionTypeValidated, "True")
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)
	})

	It("hands the PDBs over to the namespace default WorkloadClass and back", func() {
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)

		By("creating a WorkloadClass without a podSelector and making it the namespace default")
		Expect(applyObject(newPPWorkloadClass(ppDefaultWC, nil, false))).To(Succeed())
		expectWorkloadClassReady(ppDefaultWC, ppNS, false)
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command("kubectl", "label", "ns", ppNS, ppDefaultClassLbl+"-"))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", ppDefaultWC, "-n", ppNS, "--ignore-not-found"))
		})
		_, err := utils.Run(exec.Command("kubectl", "label", "ns", ppNS, ppDefaultClassLbl+"="+ppDefaultWC, "--overwrite"))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the namespace default governs every Pod in the namespace")
		expectPerPodPDBs(ppApp, ppDefaultWC, ppClosed, ppReplicas)
		expectPerPodPDBs(ppOtherApp, ppDefaultWC, ppClosed, 1)

		By("removing the namespace default")
		_, err = utils.Run(exec.Command("kubectl", "label", "ns", ppNS, ppDefaultClassLbl+"-"))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the original WorkloadClass takes its Pods' PDBs back and the other Pod's PDB is deleted")
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)
		expectNoPerPodPDBs(ppOtherApp)

		By("deleting the former namespace default, which no longer owns any PDB")
		_, err = utils.Run(exec.Command("kubectl", "delete", "workloadclass", ppDefaultWC, "-n", ppNS))
		Expect(err).NotTo(HaveOccurred())
		Consistently(func(g Gomega) {
			pdbs, err := ppPDBs()
			g.Expect(err).NotTo(HaveOccurred())
			for _, pdb := range pdbs {
				g.Expect(ppController(&pdb)).NotTo(Equal(ppDefaultWC), "PDB %s", pdb.Name)
			}
		}, consistentlyDuration, consistentlyInterval).Should(Succeed())
		expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)
	})

	It("leases each Pod's PDB separately, so an allowed subject can evict several Pods of a workload", func() {
		pods := expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)
		evicted, untouched := pods[:2], pods[2]

		By("evicting two Pods as the cluster autoscaler: the webhook admits them, but their closed PDBs reject them")
		for _, pod := range evicted {
			Expect(ppEvictAs(pod, ppAutoscalerUser)).To(MatchError(ContainSubstring(ppPDBBudgetMessage)))
		}

		By("waiting for the controller to lease both Pods' PDBs to the cluster autoscaler")
		Eventually(func(g Gomega) {
			pdbs, err := ppPDBs()
			g.Expect(err).NotTo(HaveOccurred())
			for _, pod := range evicted {
				g.Expect(pdbs).To(HaveKey(ppPDBPrefix + pod))
				pdb := pdbs[ppPDBPrefix+pod]
				g.Expect(pdb.Annotations).To(HaveKeyWithValue(ppBypassOwner, ppAutoscalerUser), "PDB of %s", pod)
				g.Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
				g.Expect(pdb.Spec.MaxUnavailable.String()).To(Equal(ppOpen), "PDB of %s", pod)
			}
			g.Expect(pdbs).To(HaveKey(ppPDBPrefix + untouched))
			pdb := pdbs[ppPDBPrefix+untouched]
			g.Expect(pdb.Annotations).NotTo(HaveKey(ppBypassOwner), "PDB of %s should not be leased", untouched)
			g.Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
			g.Expect(pdb.Spec.MaxUnavailable.String()).To(Equal(ppClosed), "PDB of %s should stay closed", untouched)
		}).Should(Succeed())

		By("retrying the evictions, which succeed once the leased PDBs allow them")
		for _, pod := range evicted {
			Eventually(func() error { return ppEvictAs(pod, ppAutoscalerUser) }, time.Minute, 2*time.Second).
				Should(Succeed(), "evicting %s", pod)
		}

		By("verifying the replacement Pods get closed PDBs and the untouched Pod keeps its own")
		current := expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)
		Expect(current).To(ContainElement(untouched))
		for _, pod := range evicted {
			Expect(current).NotTo(ContainElement(pod))
		}
		expectNoStalePDBs()
	})

	It("denies evictions by another allowed subject while a Pod's PDB is leased", func() {
		pod := expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)[0]

		By("evicting the Pod as the cluster autoscaler, which requests a lease")
		Expect(ppEvictAs(pod, ppAutoscalerUser)).To(MatchError(ContainSubstring(ppPDBBudgetMessage)))
		Eventually(func(g Gomega) {
			pdbs, err := ppPDBs()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pdbs[ppPDBPrefix+pod].Annotations).To(HaveKeyWithValue(ppBypassOwner, ppAutoscalerUser))
		}).Should(Succeed())

		By("evicting the same Pod as VPA, which the webhook denies because of the active lease")
		Expect(ppEvictAs(pod, ppVPAUser)).To(MatchError(And(
			ContainSubstring("ongoing lease held by"),
			ContainSubstring(ppAutoscalerUser),
		)))

		By("retrying as the cluster autoscaler, which holds the lease")
		Eventually(func() error { return ppEvictAs(pod, ppAutoscalerUser) }, time.Minute, 2*time.Second).Should(Succeed())
		Expect(expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)).NotTo(ContainElement(pod))
	})

	It("deletes the PDBs when the WorkloadClass is deleted", func() {
		pods := expectPerPodPDBs(ppApp, ppWC, ppClosed, ppReplicas)

		By("deleting the WorkloadClass")
		_, err := utils.Run(exec.Command("kubectl", "delete", "workloadclass", ppWC, "-n", ppNS))
		Expect(err).NotTo(HaveOccurred())

		By("verifying its PDBs are garbage collected and the Pods keep running")
		Eventually(func(g Gomega) {
			pdbs, err := ppPDBs()
			g.Expect(err).NotTo(HaveOccurred())
			for name := range pdbs {
				g.Expect(strings.HasPrefix(name, ppPDBPrefix)).To(BeFalse(), "PDB %s should be deleted", name)
			}
			current, err := ppActivePods(ppApp)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ppPodNames(current)).To(ConsistOf(pods))
		}).Should(Succeed())
	})
})
