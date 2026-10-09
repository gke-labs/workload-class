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
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/test/utils"
)

// The GKE label keys are hardcoded (rather than imported from the webhook package) so that these tests
// assert the external contract of the mutation.
const (
	gkeSpotLabel          = "cloud.google.com/gke-spot"
	gkeMachineFamilyLabel = "cloud.google.com/machine-family"
	gkeComputeClassLabel  = "cloud.google.com/compute-class"

	// placementNS holds WorkloadClasses selected by podSelector.
	placementNS = "spot-placement-e2e"
	// placementDefaultNS is labeled with a namespace-default WorkloadClass.
	placementDefaultNS    = "spot-placement-default-e2e"
	placementDefaultClass = "ns-default"

	placementPolicyName = "e2e-spot-policy"
	pauseImage          = "registry.k8s.io/pause:3.10"

	consistentlyDuration = 8 * time.Second
	consistentlyInterval = 2 * time.Second

	// unschedulableLabel is used as a nodeSelector to keep "existing" Pods Pending without a node.
	unschedulableLabel = "e2e.workloads.gke.io/unschedulable"

	placementForbiddenGuardrailYAML = `
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
      - name: gke-spot-placement
        parameters:
          enforcementMode: Forbidden
`
)

// ---------------------------------------------------------------------------
// Manifest builders
// ---------------------------------------------------------------------------

type wcOpt func(*workloadsv1.WorkloadClass)

// newPlacementWorkloadClass builds a WorkloadClass in placementNS that selects Pods labeled app=<app>.
// Each test uses its own app label so that stale objects from earlier tests (e.g. still in the webhook's
// informer cache) never match the Pods of the current test.
func newPlacementWorkloadClass(app string, cs *workloadsv1.CapacityStrategy, opts ...wcOpt) *workloadsv1.WorkloadClass {
	wc := &workloadsv1.WorkloadClass{
		TypeMeta:   metav1.TypeMeta{APIVersion: workloadsv1.GroupVersion.String(), Kind: "WorkloadClass"},
		ObjectMeta: metav1.ObjectMeta{Name: app, Namespace: placementNS},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector:      &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
			CapacityStrategy: cs,
		},
	}
	for _, o := range opts {
		o(wc)
	}
	return wc
}

func withSpotPolicyRef() wcOpt {
	return func(wc *workloadsv1.WorkloadClass) {
		wc.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{
			Group: workloadsv1.GroupVersion.Group,
			Kind:  workloadsv1.GKESpotPlacementPolicyKind,
			Name:  placementPolicyName,
		}
	}
}

func withProfileRef(group, kind, name string) wcOpt {
	return func(wc *workloadsv1.WorkloadClass) {
		wc.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{Group: group, Kind: kind, Name: name}
	}
}

func newPlacementPolicy(spec workloadsv1.GKESpotPlacementPolicySpec) *workloadsv1.GKESpotPlacementPolicy {
	return &workloadsv1.GKESpotPlacementPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: workloadsv1.GroupVersion.String(), Kind: "GKESpotPlacementPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: placementPolicyName},
		Spec:       spec,
	}
}

type podOpt func(*corev1.Pod)

// newPlacementPod builds a Pod in placementNS labeled app=<app>.
func newPlacementPod(app string, opts ...podOpt) *corev1.Pod {
	var zero int64
	p := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: app + "-",
			Namespace:    placementNS,
			Labels:       map[string]string{"app": app},
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &zero,
			Containers:                    []corev1.Container{{Name: "pause", Image: pauseImage}},
		},
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func inNamespace(ns string) podOpt {
	return func(p *corev1.Pod) { p.Namespace = ns }
}

func onNode(nodeName string) podOpt {
	return func(p *corev1.Pod) { p.Spec.NodeName = nodeName }
}

func withNodeSelector(key, value string) podOpt {
	return func(p *corev1.Pod) {
		if p.Spec.NodeSelector == nil {
			p.Spec.NodeSelector = map[string]string{}
		}
		p.Spec.NodeSelector[key] = value
	}
}

func withSpotTolerationOpt() podOpt {
	return func(p *corev1.Pod) { p.Spec.Tolerations = append(p.Spec.Tolerations, spotToleration()) }
}

// asPendingSpotPod makes the Pod look like an unscheduled Pod that was targeted for Spot.
func asPendingSpotPod() podOpt {
	return func(p *corev1.Pod) {
		withNodeSelector(gkeSpotLabel, "true")(p)
		withSpotTolerationOpt()(p)
	}
}

// asPendingOnDemandPod makes the Pod look like an unscheduled Pod that was not targeted for Spot.
func asPendingOnDemandPod() podOpt {
	return withNodeSelector(unschedulableLabel, "true")
}

func withRequiredNodeAffinity(terms ...corev1.NodeSelectorTerm) podOpt {
	return func(p *corev1.Pod) {
		p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms},
		}}
	}
}

func spotToleration() corev1.Toleration {
	return corev1.Toleration{Key: gkeSpotLabel, Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule}
}

func spotInReq() corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: gkeSpotLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{"true"}}
}

func spotDoesNotExistReq() corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{Key: gkeSpotLabel, Operator: corev1.NodeSelectorOpDoesNotExist}
}

// ---------------------------------------------------------------------------
// kubectl helpers
// ---------------------------------------------------------------------------

// kubectlJSON runs kubectl with the given stdin and returns stdout only, so that warnings written to
// stderr do not corrupt JSON output.
func kubectlJSON(stdin []byte, args ...string) ([]byte, error) {
	cmd := exec.Command("kubectl", args...)
	if dir, err := utils.GetProjectDir(); err == nil {
		cmd.Dir = dir
	}
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("kubectl %s failed: %w: %s", strings.Join(args, " "), err, stderr.String())
	}
	return out, nil
}

func applyObject(obj any) error {
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = kubectlJSON(b, "apply", "-f", "-")
	return err
}

// createPod creates the Pod and returns the object as admitted by the API server (i.e. after mutation).
func createPod(pod *corev1.Pod) (*corev1.Pod, error) {
	b, err := json.Marshal(pod)
	if err != nil {
		return nil, err
	}
	out, err := kubectlJSON(b, "create", "-f", "-", "-o", "json")
	if err != nil {
		return nil, err
	}
	created := &corev1.Pod{}
	if err := json.Unmarshal(out, created); err != nil {
		return nil, fmt.Errorf("failed to decode created Pod: %w", err)
	}
	return created, nil
}

func forceDeletePod(ns, name string) {
	_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", name, "-n", ns,
		"--grace-period=0", "--force", "--ignore-not-found", "--wait=false"))
}

// createExistingPod creates a Pod that is expected to stay around for the duration of the test and is
// counted by the webhook as an existing Pod of the workload. It returns the created Pod.
func createExistingPod(pod *corev1.Pod) *corev1.Pod {
	GinkgoHelper()
	var created *corev1.Pod
	Eventually(func() error {
		var err error
		created, err = createPod(pod)
		return err
	}).Should(Succeed())
	return created
}

// admitProbePod creates the Pod, returns the admitted (mutated) object, and immediately deletes it so it
// is not counted as an existing Pod by later admissions.
func admitProbePod(g Gomega, pod *corev1.Pod) *corev1.Pod {
	created, err := createPod(pod)
	g.Expect(err).NotTo(HaveOccurred())
	forceDeletePod(created.Namespace, created.Name)
	return created
}

func expectWorkloadClassReady(name, ns string, withProfile bool) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		validated, err := getConditionField("workloadclass", name, ns, workloadsv1.ConditionTypeValidated, "status")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(validated).To(Equal("True"))
		if withProfile {
			attached, err := getConditionField("workloadclass", name, ns, workloadsv1.ConditionTypePlacementPluginAttached, "status")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(attached).To(Equal("True"))
		}
	}).Should(Succeed())
}

func expectWorkloadClassCondition(name, ns, condType, status string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		got, err := getConditionField("workloadclass", name, ns, condType, "status")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got).To(Equal(status))
	}).Should(Succeed())
}

// ---------------------------------------------------------------------------
// Placement assertions
// ---------------------------------------------------------------------------

func requiredNodeTerms(pod *corev1.Pod) []corev1.NodeSelectorTerm {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nil
	}
	return pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
}

func preferredNodeTerms(pod *corev1.Pod) []corev1.PreferredSchedulingTerm {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return nil
	}
	return pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
}

func expectNotMutated(g Gomega, pod *corev1.Pod) {
	g.Expect(pod.Spec.NodeSelector).NotTo(HaveKey(gkeSpotLabel))
	g.Expect(pod.Spec.NodeSelector).NotTo(HaveKey(gkeComputeClassLabel))
	g.Expect(pod.Spec.Tolerations).NotTo(ContainElement(spotToleration()))
	g.Expect(pod.Spec.Affinity).To(BeNil())
}

// expectRequiredSpot asserts the Pod may only schedule onto Spot nodes (FallbackAction Fail).
func expectRequiredSpot(g Gomega, pod *corev1.Pod) {
	g.Expect(pod.Spec.NodeSelector).To(HaveKeyWithValue(gkeSpotLabel, "true"))
	g.Expect(pod.Spec.Tolerations).To(ContainElement(spotToleration()))
	g.Expect(preferredNodeTerms(pod)).To(BeEmpty())
}

// expectPreferredSpot asserts the Pod prefers Spot nodes but may fall back (FallbackAction FallbackToOnDemand).
func expectPreferredSpot(g Gomega, pod *corev1.Pod) {
	g.Expect(pod.Spec.NodeSelector).NotTo(HaveKey(gkeSpotLabel))
	g.Expect(pod.Spec.Tolerations).To(ContainElement(spotToleration()))
	g.Expect(preferredNodeTerms(pod)).To(ContainElement(corev1.PreferredSchedulingTerm{
		Weight:     100,
		Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{spotInReq()}},
	}))
}

// expectOnDemandPlacement asserts the Pod is pinned to non-Spot nodes and carries no Spot targeting.
func expectOnDemandPlacement(g Gomega, pod *corev1.Pod) {
	g.Expect(pod.Spec.NodeSelector).NotTo(HaveKey(gkeSpotLabel))
	g.Expect(pod.Spec.Tolerations).NotTo(ContainElement(spotToleration()))
	terms := requiredNodeTerms(pod)
	g.Expect(terms).NotTo(BeEmpty())
	for _, t := range terms {
		g.Expect(t.MatchExpressions).To(ContainElement(spotDoesNotExistReq()))
	}
}

// ---------------------------------------------------------------------------
// Specs
// ---------------------------------------------------------------------------

var _ = Describe("Pod Placement Webhook", Ordered, func() {
	var nodeName string

	BeforeAll(func() {
		By("creating manager namespace")
		_, err := utils.Run(exec.Command("kubectl", "create", "ns", namespace))
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		_, err = utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted"))
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		_, err = utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("creating the test namespaces")
		_, err = utils.Run(exec.Command("kubectl", "create", "ns", placementNS))
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(exec.Command("kubectl", "create", "ns", placementDefaultNS))
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", placementDefaultNS,
			"workloads.gke.io/default-class="+placementDefaultClass))
		Expect(err).NotTo(HaveOccurred())

		By("looking up a node to bind existing Pods to")
		out, err := utils.Run(exec.Command("kubectl", "get", "nodes", "-o", "jsonpath={.items[0].metadata.name}"))
		Expect(err).NotTo(HaveOccurred())
		nodeName = strings.TrimSpace(out)
		Expect(nodeName).NotTo(BeEmpty())

		By("waiting for the Pod placement webhook to serve requests")
		Eventually(func(g Gomega) {
			admitProbePod(g, newPlacementPod("webhook-ready"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		By("cleaning up test resources")
		_, _ = utils.Run(exec.Command("kubectl", "label", "node", nodeName, gkeSpotLabel+"-"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", placementNS, placementDefaultNS, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "gkespotplacementpolicy", "--all"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrail", "--all"))

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
			out, err := utils.Run(exec.Command("kubectl", "logs", "-n", namespace, "-l", "control-plane=controller-manager", "--tail=-1"))
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", out)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching WorkloadClasses")
			if out, err := utils.Run(exec.Command("kubectl", "get", "workloadclass", "-A", "-o", "yaml")); err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "WorkloadClasses:\n%s", out)
			}
		}

		By("Cleaning up placement test resources")
		for _, ns := range []string{placementNS, placementDefaultNS} {
			_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", "--all", "-n", ns))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", "--all", "-n", ns, "--grace-period=0", "--force"))
		}
		_, _ = utils.Run(exec.Command("kubectl", "delete", "gkespotplacementpolicy", "--all"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrail", "--all"))
		_, _ = utils.Run(exec.Command("kubectl", "label", "node", nodeName, gkeSpotLabel+"-"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)
	// expectPodsNotMutated repeatedly admits Pods and asserts none are mutated.
	expectPodsNotMutated := func(newPod func() *corev1.Pod) {
		GinkgoHelper()
		Consistently(func(g Gomega) {
			expectNotMutated(g, admitProbePod(g, newPod()))
		}, consistentlyDuration, consistentlyInterval).Should(Succeed())
	}

	// expectPreferredSpotConsistently waits until Pods prefer Spot, then asserts they keep doing so while
	// the webhook's cache has time to observe the existing Pods that might (incorrectly) flip them to
	// On-Demand.
	expectPreferredSpotConsistently := func(app string) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			expectPreferredSpot(g, admitProbePod(g, newPlacementPod(app)))
		}).Should(Succeed())
		Consistently(func(g Gomega) {
			expectPreferredSpot(g, admitProbePod(g, newPlacementPod(app)))
		}, consistentlyDuration, consistentlyInterval).Should(Succeed())
	}

	Context("When the Pod should not be mutated", func() {
		It("does not mutate Pods that match no WorkloadClass", func() {
			expectPodsNotMutated(func() *corev1.Pod { return newPlacementPod("no-match") })
		})

		It("does not mutate Pods whose WorkloadClass has no CapacityStrategy", func() {
			const app = "no-capacity-strategy"
			Expect(applyObject(newPlacementWorkloadClass(app, nil))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, false)

			expectPodsNotMutated(func() *corev1.Pod { return newPlacementPod(app) })
		})

		It("does not mutate Pods whose WorkloadClass references a different infrastructure profile kind", func() {
			const app = "other-profile"
			wc := newPlacementWorkloadClass(app, &workloadsv1.CapacityStrategy{},
				withProfileRef("karpenter.example.io", "KarpenterSpotPolicy", "other"))
			Expect(applyObject(wc)).To(Succeed())

			expectPodsNotMutated(func() *corev1.Pod { return newPlacementPod(app) })
		})

		It("does not mutate Pods while the referenced GKESpotPlacementPolicy is missing, then mutates once it is created", func() {
			const app = "missing-policy"
			Expect(applyObject(newPlacementWorkloadClass(app, &workloadsv1.CapacityStrategy{}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassCondition(app, placementNS, workloadsv1.ConditionTypePlacementPluginAttached, "False")

			expectPodsNotMutated(func() *corev1.Pod { return newPlacementPod(app) })

			By("creating the referenced GKESpotPlacementPolicy")
			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{}))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				expectRequiredSpot(g, admitProbePod(g, newPlacementPod(app)))
			}).Should(Succeed())
		})

		It("does not mutate Pods while a guardrail invalidates the GKESpotPlacementPolicy, then mutates once it is removed", func() {
			const app = "guardrail-forbidden"
			Eventually(func() error { return applyManifest(placementForbiddenGuardrailYAML) }).Should(Succeed())
			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app, &workloadsv1.CapacityStrategy{}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassCondition(app, placementNS, workloadsv1.ConditionTypePlacementPluginAttached, "False")
			expectWorkloadClassCondition(app, placementNS, workloadsv1.ConditionTypeValidated, "False")

			expectPodsNotMutated(func() *corev1.Pod { return newPlacementPod(app) })

			By("deleting the WorkloadClassGuardrail")
			_, err := utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrail", "default"))
			Expect(err).NotTo(HaveOccurred())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				expectRequiredSpot(g, admitProbePod(g, newPlacementPod(app)))
			}).Should(Succeed())
		})
	})

	Context("When the WorkloadClass targets Spot with a GKESpotPlacementPolicy", func() {
		It("requires Spot and targets the CustomComputeClass when FallbackAction is Fail", func() {
			const app = "spot-fail-ccc"
			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{CustomComputeClass: "spot-first"}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFail}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				pod := admitProbePod(g, newPlacementPod(app))
				expectRequiredSpot(g, pod)
				g.Expect(pod.Spec.NodeSelector).To(HaveKeyWithValue(gkeComputeClassLabel, "spot-first"))
				g.Expect(requiredNodeTerms(pod)).To(BeEmpty())
			}).Should(Succeed())
		})

		It("does not override a compute class the Pod already selects", func() {
			const app = "spot-user-ccc"
			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{CustomComputeClass: "spot-first"}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app, &workloadsv1.CapacityStrategy{}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				pod := admitProbePod(g, newPlacementPod(app, withNodeSelector(gkeComputeClassLabel, "user-choice")))
				expectRequiredSpot(g, pod)
				g.Expect(pod.Spec.NodeSelector).To(HaveKeyWithValue(gkeComputeClassLabel, "user-choice"))
			}).Should(Succeed())
		})

		It("prefers Spot and restricts On-Demand fallback to the allowed machine families when FallbackAction is FallbackToOnDemand", func() {
			const app = "spot-fallback-families"
			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{
				FallbackAllowedMachineFamilies: []string{"e2", "n2d"},
			}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				pod := admitProbePod(g, newPlacementPod(app))
				expectPreferredSpot(g, pod)
				g.Expect(requiredNodeTerms(pod)).To(ConsistOf(
					corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{spotInReq()}},
					corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{
						spotDoesNotExistReq(),
						{Key: gkeMachineFamilyLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{"e2", "n2d"}},
					}},
				))
			}).Should(Succeed())
		})

		It("ANDs the placement requirements with the Pod's existing required node affinity", func() {
			const app = "spot-and-affinity"
			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{
				FallbackAllowedMachineFamilies: []string{"e2"},
			}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			zone := corev1.NodeSelectorRequirement{Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"zone-a"}}
			Eventually(func(g Gomega) {
				pod := admitProbePod(g, newPlacementPod(app,
					withRequiredNodeAffinity(corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{zone}})))
				expectPreferredSpot(g, pod)
				g.Expect(requiredNodeTerms(pod)).To(ConsistOf(
					corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{zone, spotInReq()}},
					corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{
						zone,
						spotDoesNotExistReq(),
						{Key: gkeMachineFamilyLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{"e2"}},
					}},
				))
			}).Should(Succeed())
		})
	})

	Context("When the WorkloadClass targets On-Demand", func() {
		It("pins the Pod to On-Demand and strips any Spot targeting when Type is OnDemand", func() {
			const app = "on-demand"
			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{CustomComputeClass: "spot-first"}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				pod := admitProbePod(g, newPlacementPod(app, asPendingSpotPod()))
				expectOnDemandPlacement(g, pod)
				g.Expect(pod.Spec.NodeSelector).NotTo(HaveKey(gkeComputeClassLabel))
			}).Should(Succeed())
		})

		It("pins the Pod to On-Demand when SpotRatio is 0%", func() {
			const app = "spot-ratio-zero"
			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{SpotRatio: "0%"}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				expectOnDemandPlacement(g, admitProbePod(g, newPlacementPod(app)))
			}).Should(Succeed())
		})
	})

	Context("When the WorkloadClass has a SpotRatio below 100%", func() {
		It("places the Pod on Spot while the Spot share is below the target ratio", func() {
			const app = "spot-ratio-below"
			// Existing Pods are created before the WorkloadClass so they are not mutated themselves.
			By("creating 1 Spot and 1 On-Demand existing Pod (next Pod: 1/3 Spot < 50%)")
			createExistingPod(newPlacementPod(app, asPendingSpotPod()))
			createExistingPod(newPlacementPod(app, asPendingOnDemandPod()))

			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{SpotRatio: "50%"}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				expectRequiredSpot(g, admitProbePod(g, newPlacementPod(app)))
			}).Should(Succeed())
		})

		It("places the Pod on On-Demand once the target ratio is met", func() {
			const app = "spot-ratio-met"
			By("creating 2 Spot and 1 On-Demand existing Pods (next Pod: 2/4 Spot is not < 50%)")
			createExistingPod(newPlacementPod(app, asPendingSpotPod()))
			createExistingPod(newPlacementPod(app, asPendingSpotPod()))
			createExistingPod(newPlacementPod(app, asPendingOnDemandPod()))

			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{SpotRatio: "50%"}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				expectOnDemandPlacement(g, admitProbePod(g, newPlacementPod(app)))
			}).Should(Succeed())
		})

		It("judges a scheduled Pod by its node rather than its Spot targeting", func() {
			const app = "spot-ratio-node"
			By("creating a Spot-targeted Pod bound to a non-Spot node (counts as On-Demand; next Pod: 0/2 Spot < 50%)")
			createExistingPod(newPlacementPod(app, withSpotTolerationOpt(), onNode(nodeName)))

			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.LazyReversionAction},
			}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{SpotRatio: "50%"}, withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				expectRequiredSpot(g, admitProbePod(g, newPlacementPod(app)))
			}).Should(Succeed())
		})
	})

	Context("When the workload has fallen back to On-Demand", func() {
		fallbackStrategy := func() *workloadsv1.CapacityStrategy {
			return &workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}
		}

		It("keeps new Pods on On-Demand when reversion is None", func() {
			const app = "fallback-none"
			By("creating a Spot-targeted Pod running on a non-Spot node")
			createExistingPod(newPlacementPod(app, withSpotTolerationOpt(), onNode(nodeName)))

			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.NoneReversionAction},
			}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app, fallbackStrategy(), withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			Eventually(func(g Gomega) {
				expectOnDemandPlacement(g, admitProbePod(g, newPlacementPod(app)))
			}).Should(Succeed())
		})

		It("targets Spot again when reversion is Lazy", func() {
			const app = "fallback-lazy"
			By("creating a Spot-targeted Pod running on a non-Spot node")
			createExistingPod(newPlacementPod(app, withSpotTolerationOpt(), onNode(nodeName)))

			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.LazyReversionAction},
			}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app, fallbackStrategy(), withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			expectPreferredSpotConsistently(app)
		})

		It("does not treat the workload as in fallback when Spot-targeted Pods run on Spot nodes", func() {
			const app = "fallback-on-spot"
			By("labeling the node as a Spot node")
			_, err := utils.Run(exec.Command("kubectl", "label", "--overwrite", "node", nodeName, gkeSpotLabel+"=true"))
			Expect(err).NotTo(HaveOccurred())

			By("creating a Spot-targeted Pod running on the Spot node")
			createExistingPod(newPlacementPod(app, withSpotTolerationOpt(), onNode(nodeName)))

			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.NoneReversionAction},
			}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app, fallbackStrategy(), withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			expectPreferredSpotConsistently(app)
		})

		It("latches InFallback for reversion None after the fallback Pod is gone, and releases it at zero Pods", func() {
			const app = "fallback-condition"
			By("creating a Spot-targeted Pod on a non-Spot node and another active Pod of the workload")
			fallbackPod := createExistingPod(newPlacementPod(app, withSpotTolerationOpt(), onNode(nodeName)))
			remainingPod := createExistingPod(newPlacementPod(app, asPendingOnDemandPod()))

			Expect(applyObject(newPlacementPolicy(workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{Action: workloadsv1.NoneReversionAction},
			}))).To(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app, fallbackStrategy(), withSpotPolicyRef()))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, true)

			By("verifying the controller reports InFallback=True")
			expectWorkloadClassCondition(app, placementNS, workloadsv1.ConditionTypeInFallback, "True")

			By("deleting the fallback Pod, so only the InFallback condition records the fallback")
			forceDeletePod(placementNS, fallbackPod.Name)

			By("verifying InFallback stays latched and new Pods stay on On-Demand")
			Consistently(func(g Gomega) {
				status, err := getConditionField("workloadclass", app, placementNS, workloadsv1.ConditionTypeInFallback, "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("True"))
				expectOnDemandPlacement(g, admitProbePod(g, newPlacementPod(app)))
			}, consistentlyDuration, consistentlyInterval).Should(Succeed())

			By("deleting the remaining Pod so the workload scales to zero")
			forceDeletePod(placementNS, remainingPod.Name)
			expectWorkloadClassCondition(app, placementNS, workloadsv1.ConditionTypeInFallback, "False")

			Eventually(func(g Gomega) {
				expectPreferredSpot(g, admitProbePod(g, newPlacementPod(app)))
			}).Should(Succeed())
		})
	})

	Context("When the WorkloadClass has no InfrastructureProfileRef", func() {
		It("pins the Pod to On-Demand when Type is OnDemand", func() {
			const app = "noref-on-demand"
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand}))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, false)

			Eventually(func(g Gomega) {
				expectOnDemandPlacement(g, admitProbePod(g, newPlacementPod(app, asPendingSpotPod())))
			}).Should(Succeed())
		})

		It("requires Spot with default settings when FallbackAction is Fail", func() {
			const app = "noref-spot-fail"
			Expect(applyObject(newPlacementWorkloadClass(app, &workloadsv1.CapacityStrategy{}))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, false)

			Eventually(func(g Gomega) {
				pod := admitProbePod(g, newPlacementPod(app))
				expectRequiredSpot(g, pod)
				g.Expect(pod.Spec.NodeSelector).NotTo(HaveKey(gkeComputeClassLabel))
				g.Expect(requiredNodeTerms(pod)).To(BeEmpty())
			}).Should(Succeed())
		})

		It("prefers Spot without machine family restrictions when FallbackAction is FallbackToOnDemand", func() {
			const app = "noref-spot-fallback"
			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, false)

			Eventually(func(g Gomega) {
				pod := admitProbePod(g, newPlacementPod(app))
				expectPreferredSpot(g, pod)
				g.Expect(requiredNodeTerms(pod)).To(BeEmpty())
			}).Should(Succeed())
		})

		It("treats reversion as Lazy and targets Spot even when the workload is in fallback", func() {
			const app = "noref-fallback"
			By("creating a Spot-targeted Pod running on a non-Spot node")
			createExistingPod(newPlacementPod(app, withSpotTolerationOpt(), onNode(nodeName)))

			Expect(applyObject(newPlacementWorkloadClass(app,
				&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, false)

			expectPreferredSpotConsistently(app)
		})

		It("applies the SpotRatio using existing Pods", func() {
			const app = "noref-ratio"
			By("creating 1 Spot existing Pod (next Pod: 1/2 Spot is not < 50%)")
			createExistingPod(newPlacementPod(app, asPendingSpotPod()))

			Expect(applyObject(newPlacementWorkloadClass(app, &workloadsv1.CapacityStrategy{SpotRatio: "50%"}))).To(Succeed())
			expectWorkloadClassReady(app, placementNS, false)

			Eventually(func(g Gomega) {
				expectOnDemandPlacement(g, admitProbePod(g, newPlacementPod(app)))
			}).Should(Succeed())
		})

		It("does not mutate Pods when a guardrail invalidates the CapacityStrategy", func() {
			const app = "noref-forbidden"
			Eventually(func() error { return applyManifest(placementForbiddenGuardrailYAML) }).Should(Succeed())
			Expect(applyObject(newPlacementWorkloadClass(app, &workloadsv1.CapacityStrategy{}))).To(Succeed())
			expectWorkloadClassCondition(app, placementNS, workloadsv1.ConditionTypeValidated, "False")

			expectPodsNotMutated(func() *corev1.Pod { return newPlacementPod(app) })
		})
	})

	Context("When the namespace has a default WorkloadClass", func() {
		It("applies the namespace default to Pods regardless of their labels and counts all Pods in the namespace", func() {
			wc := &workloadsv1.WorkloadClass{
				TypeMeta:   metav1.TypeMeta{APIVersion: workloadsv1.GroupVersion.String(), Kind: "WorkloadClass"},
				ObjectMeta: metav1.ObjectMeta{Name: placementDefaultClass, Namespace: placementDefaultNS},
				Spec: workloadsv1.WorkloadClassSpec{
					CapacityStrategy: &workloadsv1.CapacityStrategy{SpotRatio: "50%"},
				},
			}
			Expect(applyObject(wc)).To(Succeed())
			expectWorkloadClassReady(placementDefaultClass, placementDefaultNS, false)

			By("admitting a Pod with arbitrary labels into an empty namespace (0/1 Spot < 50%)")
			Eventually(func(g Gomega) {
				expectRequiredSpot(g, admitProbePod(g, newPlacementPod("arbitrary", inNamespace(placementDefaultNS))))
			}).Should(Succeed())

			By("creating an existing Spot Pod with different labels (next Pod: 1/2 Spot is not < 50%)")
			createExistingPod(newPlacementPod("other-app", inNamespace(placementDefaultNS), asPendingSpotPod()))

			Eventually(func(g Gomega) {
				expectOnDemandPlacement(g, admitProbePod(g, newPlacementPod("arbitrary", inNamespace(placementDefaultNS))))
			}).Should(Succeed())
		})
	})
})
