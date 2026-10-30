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

// Package invariants implements stateless runtime safety invariant evaluators (I1-I5)
// for WorkloadClass controllers and webhooks (see docs/invariants.md).
package invariants

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// RuleID identifies a core runtime safety invariant (I1-I5).
type RuleID string

const (
	// RuleI1GuardrailValidationGating ensures unvalidated WorkloadClasses (Validated=False) fail open.
	RuleI1GuardrailValidationGating RuleID = "I1"
	// RuleI2PerPodPDBFidelityAndPrecedence ensures Per-Pod PDB UID scoping, user-PDB precedence, and readiness sync.
	RuleI2PerPodPDBFidelityAndPrecedence RuleID = "I2"
	// RuleI3BoundedAuthorizedLease ensures PDB bypass leases belong to authorized subjects and have valid, non-expired bounds.
	RuleI3BoundedAuthorizedLease RuleID = "I3"
	// RuleI4DeterministicClassPrecedence ensures Pods resolve to the highest-precedence WorkloadClass.
	RuleI4DeterministicClassPrecedence RuleID = "I4"
	// RuleI5SpotPlacementAndReversionSafety ensures Spot placement and active reversion honor guardrails and stockout status.
	RuleI5SpotPlacementAndReversionSafety RuleID = "I5"
)

// Violation represents a detected invariant violation on a point-in-time Snapshot.
type Violation struct {
	Rule    RuleID
	Object  string
	Message string
}

func (v Violation) Error() string {
	return fmt.Sprintf("[%s] %s: %s", v.Rule, v.Object, v.Message)
}

// ReversionDecision records a candidate Active Spot reversion eviction evaluated by the controller.
type ReversionDecision struct {
	PodName           string
	PodNamespace      string
	WorkloadClassName string
	PolicyName        string
	TimeOnFallback    time.Duration
}

// Snapshot holds an in-memory point-in-time view of cluster resources for invariant verification.
type Snapshot struct {
	Now                     time.Time
	Namespaces              []corev1.Namespace
	WorkloadClasses         []workloadsv1.WorkloadClass
	Guardrails              []workloadsv1.WorkloadClassGuardrail
	SpotPlacementPolicies   []workloadsv1.GKESpotPlacementPolicy
	Pods                    []corev1.Pod
	PDBs                    []policyv1.PodDisruptionBudget
	ActiveReversionEviction *ReversionDecision
}

// EvaluateAll runs all five core safety invariants (I1-I5) against snap.
func EvaluateAll(snap Snapshot) []Violation {
	if snap.Now.IsZero() {
		snap.Now = time.Now().UTC()
	}
	return slices.Concat(
		EvaluateI1GuardrailValidationGating(snap),
		EvaluateI2PerPodPDBFidelityAndPrecedence(snap),
		EvaluateI3BoundedAuthorizedLease(snap),
		EvaluateI4DeterministicClassPrecedence(snap),
		EvaluateI5SpotPlacementAndReversionSafety(snap),
	)
}

// EvaluateI1GuardrailValidationGating verifies that no WorkloadClass with Validated=False maintains a closed
// Per-Pod PDB (maxUnavailable == 0).
func EvaluateI1GuardrailValidationGating(snap Snapshot) []Violation {
	var violations []Violation
	podsByName := indexPodsByKey(snap.Pods)
	nsByName := indexNamespacesByName(snap.Namespaces)

	for i := range snap.PDBs {
		pdb := &snap.PDBs[i]
		if !isControllerManagedPDB(pdb) {
			continue
		}
		podKey := pdb.Namespace + "/" + utils.PodNameFromPDBName(pdb.Name)
		pod, ok := podsByName[podKey]
		if !ok {
			continue
		}
		wc := resolveBestWorkloadClass(pod, nsByName[pod.Namespace], snap.WorkloadClasses)
		if wc == nil {
			continue
		}
		if isValidationFailed(wc) && isPDBClosed(pdb) {
			violations = append(violations, Violation{
				Rule:   RuleI1GuardrailValidationGating,
				Object: fmt.Sprintf("%s/%s", pdb.Namespace, pdb.Name),
				Message: fmt.Sprintf(
					"Per-Pod PDB has maxUnavailable=0 while governing WorkloadClass %q has Validated=False",
					wc.Name,
				),
			})
		}
	}
	return violations
}

// EvaluateI2PerPodPDBFidelityAndPrecedence verifies Per-Pod PDB UID scoping, user-PDB precedence,
// absence of orphaned controller PDBs, and readiness-synchronized maxUnavailable budgets.
func EvaluateI2PerPodPDBFidelityAndPrecedence(snap Snapshot) []Violation {
	var violations []Violation
	podsByName := indexPodsByKey(snap.Pods)
	nsByName := indexNamespacesByName(snap.Namespaces)

	for i := range snap.PDBs {
		pdb := &snap.PDBs[i]
		if !isControllerManagedPDB(pdb) {
			continue
		}
		pdbRef := fmt.Sprintf("%s/%s", pdb.Namespace, pdb.Name)
		podKey := pdb.Namespace + "/" + utils.PodNameFromPDBName(pdb.Name)
		pod, exists := podsByName[podKey]
		if !exists || pod.DeletionTimestamp != nil {
			violations = append(violations, Violation{
				Rule:    RuleI2PerPodPDBFidelityAndPrecedence,
				Object:  pdbRef,
				Message: "orphaned controller-managed Per-Pod PDB has no active target Pod",
			})
			continue
		}

		expectedUID := string(pod.UID)
		selUID := ""
		if pdb.Spec.Selector != nil && pdb.Spec.Selector.MatchLabels != nil {
			selUID = pdb.Spec.Selector.MatchLabels[utils.PodUIDLabel]
		}
		if pdb.Labels[utils.PodUIDLabel] != expectedUID || selUID != expectedUID || pod.Labels[utils.PodUIDLabel] != expectedUID {
			violations = append(violations, Violation{
				Rule:   RuleI2PerPodPDBFidelityAndPrecedence,
				Object: pdbRef,
				Message: fmt.Sprintf(
					"UID mismatch between Pod (%s) and Per-Pod PDB label (%s) / selector (%s)",
					expectedUID, pdb.Labels[utils.PodUIDLabel], selUID,
				),
			})
		}

		if coveredByUserPDB(pod, snap.PDBs) {
			violations = append(violations, Violation{
				Rule:    RuleI2PerPodPDBFidelityAndPrecedence,
				Object:  pdbRef,
				Message: "controller-managed Per-Pod PDB coexists with a user-created PDB covering the same Pod",
			})
		}

		wc := resolveBestWorkloadClass(pod, nsByName[pod.Namespace], snap.WorkloadClasses)
		if wc == nil {
			violations = append(violations, Violation{
				Rule:    RuleI2PerPodPDBFidelityAndPrecedence,
				Object:  pdbRef,
				Message: "controller-managed Per-Pod PDB exists for a Pod not matched by any WorkloadClass",
			})
			continue
		}

		// When no bypass lease annotation is set, maxUnavailable must match readiness + validation state.
		if pdb.Annotations[utils.BypassOwner] == "" {
			shouldBeClosed := !isValidationFailed(wc) && wc.Status.MaintenanceReadiness == workloadsv1.ReadinessNotReady
			if shouldBeClosed && !isPDBClosed(pdb) {
				violations = append(violations, Violation{
					Rule:   RuleI2PerPodPDBFidelityAndPrecedence,
					Object: pdbRef,
					Message: fmt.Sprintf(
						"Per-Pod PDB is open without a bypass lease while WorkloadClass %q is Validated and NotReady",
						wc.Name,
					),
				})
			}
			if !shouldBeClosed && isPDBClosed(pdb) {
				violations = append(violations, Violation{
					Rule:   RuleI2PerPodPDBFidelityAndPrecedence,
					Object: pdbRef,
					Message: fmt.Sprintf(
						"Per-Pod PDB is closed (maxUnavailable=0) while WorkloadClass %q has MaintenanceReadiness=%q",
						wc.Name, wc.Status.MaintenanceReadiness,
					),
				})
			}
		}
	}
	return violations
}

// EvaluateI3BoundedAuthorizedLease verifies that any Per-Pod PDB carrying a bypass lease has a valid,
// non-expired expiration timestamp and belongs to an identity authorized in AllowedDisruptionsOutsideOfWindow.
func EvaluateI3BoundedAuthorizedLease(snap Snapshot) []Violation {
	var violations []Violation
	podsByName := indexPodsByKey(snap.Pods)
	nsByName := indexNamespacesByName(snap.Namespaces)

	for i := range snap.PDBs {
		pdb := &snap.PDBs[i]
		if !isControllerManagedPDB(pdb) {
			continue
		}
		owner := pdb.Annotations[utils.BypassOwner]
		expRaw := pdb.Annotations[utils.BypassExpiration]
		if owner == "" && expRaw == "" {
			continue
		}
		pdbRef := fmt.Sprintf("%s/%s", pdb.Namespace, pdb.Name)
		if owner == "" || expRaw == "" {
			violations = append(violations, Violation{
				Rule:    RuleI3BoundedAuthorizedLease,
				Object:  pdbRef,
				Message: "Per-Pod PDB has incomplete bypass lease annotations (both bypass-owner and bypass-expiration are required)",
			})
			continue
		}

		expTime, err := time.Parse(utils.ExpirationFormat, expRaw)
		if err != nil {
			violations = append(violations, Violation{
				Rule:    RuleI3BoundedAuthorizedLease,
				Object:  pdbRef,
				Message: fmt.Sprintf("unparseable bypass-expiration annotation %q: %v", expRaw, err),
			})
			continue
		}

		podKey := pdb.Namespace + "/" + utils.PodNameFromPDBName(pdb.Name)
		pod, exists := podsByName[podKey]
		if !exists {
			continue
		}
		wc := resolveBestWorkloadClass(pod, nsByName[pod.Namespace], snap.WorkloadClasses)
		if wc == nil {
			continue
		}

		if !isAuthorizedBypassOwner(owner, wc.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow) {
			violations = append(violations, Violation{
				Rule:   RuleI3BoundedAuthorizedLease,
				Object: pdbRef,
				Message: fmt.Sprintf(
					"bypass-owner %q is not authorized in WorkloadClass %q allowedDisruptionsOutsideOfWindow",
					owner, wc.Name,
				),
			})
		}

		if snap.Now.After(expTime) && wc.Status.MaintenanceReadiness == workloadsv1.ReadinessNotReady && !isPDBClosed(pdb) {
			violations = append(violations, Violation{
				Rule:   RuleI3BoundedAuthorizedLease,
				Object: pdbRef,
				Message: fmt.Sprintf(
					"expired bypass lease (expired at %s, now %s) still holds Per-Pod PDB open during NotReady",
					expTime.UTC().Format(time.RFC3339), snap.Now.UTC().Format(time.RFC3339),
				),
			})
		}
	}
	return violations
}

// EvaluateI4DeterministicClassPrecedence verifies that when a Namespace defines workloads.gke.io/default-class
// or multiple WorkloadClasses match a Pod, the Pod's Per-Pod PDB reflects the highest-precedence WorkloadClass.
func EvaluateI4DeterministicClassPrecedence(snap Snapshot) []Violation {
	var violations []Violation
	podsByName := indexPodsByKey(snap.Pods)
	nsByName := indexNamespacesByName(snap.Namespaces)

	for i := range snap.PDBs {
		pdb := &snap.PDBs[i]
		if !isControllerManagedPDB(pdb) || pdb.Annotations[utils.BypassOwner] != "" {
			continue
		}
		podKey := pdb.Namespace + "/" + utils.PodNameFromPDBName(pdb.Name)
		pod, exists := podsByName[podKey]
		if !exists {
			continue
		}
		bestWC := resolveBestWorkloadClass(pod, nsByName[pod.Namespace], snap.WorkloadClasses)
		if bestWC == nil || isValidationFailed(bestWC) {
			continue
		}
		expectClosed := bestWC.Status.MaintenanceReadiness == workloadsv1.ReadinessNotReady
		if expectClosed != isPDBClosed(pdb) {
			violations = append(violations, Violation{
				Rule:   RuleI4DeterministicClassPrecedence,
				Object: fmt.Sprintf("%s/%s", pdb.Namespace, pdb.Name),
				Message: fmt.Sprintf(
					"Per-Pod PDB budget (closed=%v) disagrees with winning WorkloadClass %q (MaintenanceReadiness=%s)",
					isPDBClosed(pdb), bestWC.Name, bestWC.Status.MaintenanceReadiness,
				),
			})
		}
	}
	return violations
}

// EvaluateI5SpotPlacementAndReversionSafety verifies Spot placement guardrail enforcement and Active reversion bounds.
func EvaluateI5SpotPlacementAndReversionSafety(snap Snapshot) []Violation {
	var violations []Violation

	spotForbidden := isSpotForbiddenByGuardrails(snap.Guardrails)
	if spotForbidden {
		for i := range snap.WorkloadClasses {
			wc := &snap.WorkloadClasses[i]
			if !isValidationPassed(wc) || wc.Spec.CapacityStrategy == nil {
				continue
			}
			spotType := wc.Spec.CapacityStrategy.Type
			if spotType == "" {
				spotType = workloadsv1.SpotPlacementTypeSpot
			}
			spotRatio := wc.Spec.CapacityStrategy.SpotRatio
			if spotRatio == "" {
				spotRatio = "100%"
			}
			if spotType == workloadsv1.SpotPlacementTypeSpot && spotRatio != "0%" {
				violations = append(violations, Violation{
					Rule:    RuleI5SpotPlacementAndReversionSafety,
					Object:  fmt.Sprintf("%s/%s", wc.Namespace, wc.Name),
					Message: "WorkloadClass is marked Validated=True with Spot placement enabled while a Guardrail sets enforcementMode=Forbidden",
				})
			}
		}
	}

	if rev := snap.ActiveReversionEviction; rev != nil {
		var policy *workloadsv1.GKESpotPlacementPolicy
		for i := range snap.SpotPlacementPolicies {
			if snap.SpotPlacementPolicies[i].Name == rev.PolicyName {
				policy = &snap.SpotPlacementPolicies[i]
				break
			}
		}
		if policy != nil {
			cond := meta.FindStatusCondition(policy.Status.Conditions, workloadsv1.ConditionTypeSpotCapacityAvailable)
			if cond == nil || cond.Status != metav1.ConditionTrue {
				violations = append(violations, Violation{
					Rule:   RuleI5SpotPlacementAndReversionSafety,
					Object: fmt.Sprintf("%s/%s", rev.PodNamespace, rev.PodName),
					Message: fmt.Sprintf(
						"Active Spot reversion evicted Pod while GKESpotPlacementPolicy %q SpotCapacityAvailable is not True",
						policy.Name,
					),
				})
			}
			minDur := 5 * time.Minute
			if policy.Spec.Reversion.MinDurationOnFallback != nil {
				minDur = policy.Spec.Reversion.MinDurationOnFallback.Duration
			}
			if rev.TimeOnFallback < minDur {
				violations = append(violations, Violation{
					Rule:   RuleI5SpotPlacementAndReversionSafety,
					Object: fmt.Sprintf("%s/%s", rev.PodNamespace, rev.PodName),
					Message: fmt.Sprintf(
						"Active Spot reversion evicted Pod after %s on fallback, before minDurationOnFallback (%s)",
						rev.TimeOnFallback, minDur,
					),
				})
			}
		}
	}

	return violations
}

func isControllerManagedPDB(pdb *policyv1.PodDisruptionBudget) bool {
	return strings.HasPrefix(pdb.Name, utils.PDBNamePrefix) && pdb.Labels[utils.PodUIDLabel] != ""
}

func isPDBClosed(pdb *policyv1.PodDisruptionBudget) bool {
	if pdb.Spec.MaxUnavailable == nil {
		return false
	}
	return pdb.Spec.MaxUnavailable.Type == 0 && pdb.Spec.MaxUnavailable.IntVal == 0
}

func isValidationFailed(wc *workloadsv1.WorkloadClass) bool {
	cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeValidated)
	return cond != nil && cond.Status == metav1.ConditionFalse
}

func isValidationPassed(wc *workloadsv1.WorkloadClass) bool {
	cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeValidated)
	return cond != nil && cond.Status == metav1.ConditionTrue
}

func coveredByUserPDB(pod *corev1.Pod, pdbs []policyv1.PodDisruptionBudget) bool {
	for i := range pdbs {
		pdb := &pdbs[i]
		if pdb.Namespace != pod.Namespace || isControllerManagedPDB(pdb) || pdb.Spec.Selector == nil {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(pod.Labels)) {
			return true
		}
	}
	return false
}

func resolveBestWorkloadClass(
	pod *corev1.Pod,
	ns *corev1.Namespace,
	classes []workloadsv1.WorkloadClass,
) *workloadsv1.WorkloadClass {
	if ns != nil {
		if defName := ns.Labels[workloadsv1.DefaultClassLabel]; defName != "" {
			for i := range classes {
				if classes[i].Namespace == pod.Namespace && classes[i].Name == defName {
					return &classes[i]
				}
			}
			return nil
		}
	}

	var best *workloadsv1.WorkloadClass
	bestSpec := -1
	for i := range classes {
		wc := &classes[i]
		if wc.Namespace != pod.Namespace || wc.Spec.PodSelector == nil {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil || !sel.Matches(labels.Set(pod.Labels)) {
			continue
		}
		spec := len(wc.Spec.PodSelector.MatchLabels) + len(wc.Spec.PodSelector.MatchExpressions)
		if best == nil || spec > bestSpec ||
			(spec == bestSpec && wc.CreationTimestamp.Before(&best.CreationTimestamp)) ||
			(spec == bestSpec && wc.CreationTimestamp.Equal(&best.CreationTimestamp) && wc.Name < best.Name) {
			best = wc
			bestSpec = spec
		}
	}
	return best
}

func isAuthorizedBypassOwner(owner string, allowed []workloadsv1.Subject) bool {
	for _, s := range allowed {
		if utils.BypassOwnerValue(s) == owner {
			return true
		}
	}
	return false
}

func isSpotForbiddenByGuardrails(guardrails []workloadsv1.WorkloadClassGuardrail) bool {
	for i := range guardrails {
		g := &guardrails[i]
		if g.Spec.PluginConstraints == nil {
			continue
		}
		for _, pc := range g.Spec.PluginConstraints.Placement {
			if pc.Name != workloadsv1.PluginNameGKESpotPlacement || pc.Parameters == nil || len(pc.Parameters.Raw) == 0 {
				continue
			}
			var params workloadsv1.GKESpotGuardrailParameters
			if err := json.Unmarshal(pc.Parameters.Raw, &params); err != nil {
				continue
			}
			if params.EnforcementMode == workloadsv1.ForbiddenEnforcementMode {
				return true
			}
		}
	}
	return false
}

func indexPodsByKey(pods []corev1.Pod) map[string]*corev1.Pod {
	m := make(map[string]*corev1.Pod, len(pods))
	for i := range pods {
		p := &pods[i]
		m[p.Namespace+"/"+p.Name] = p
	}
	return m
}

func indexNamespacesByName(namespaces []corev1.Namespace) map[string]*corev1.Namespace {
	m := make(map[string]*corev1.Namespace, len(namespaces))
	for i := range namespaces {
		ns := &namespaces[i]
		m[ns.Name] = ns
	}
	return m
}
