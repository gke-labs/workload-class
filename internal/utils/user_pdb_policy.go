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

package utils

import (
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// UserPDBPolicyDecision holds the strictest active enforcement mode per check across all
// cluster WorkloadClassGuardrails (Enforce > Warn > Allow).
type UserPDBPolicyDecision struct {
	ZeroDisruptionMode workloadsv1.UserPDBEnforcementMode
	OverlappingMode    workloadsv1.UserPDBEnforcementMode
}

// IsActive reports whether at least one user-PDB check is set to Warn or Enforce.
func (d UserPDBPolicyDecision) IsActive() bool {
	return d.ZeroDisruptionMode != workloadsv1.UserPDBModeAllow ||
		d.OverlappingMode != workloadsv1.UserPDBModeAllow
}

func stricterUserPDBMode(current, candidate workloadsv1.UserPDBEnforcementMode) workloadsv1.UserPDBEnforcementMode {
	switch {
	case current == workloadsv1.UserPDBModeEnforce || candidate == workloadsv1.UserPDBModeEnforce:
		return workloadsv1.UserPDBModeEnforce
	case current == workloadsv1.UserPDBModeWarn || candidate == workloadsv1.UserPDBModeWarn:
		return workloadsv1.UserPDBModeWarn
	default:
		return workloadsv1.UserPDBModeAllow
	}
}

// EffectiveUserPDBPolicy computes the strictest enforcement mode per check across all cluster
// WorkloadClassGuardrails using only the guardrails that enable each check. Boolean check flags
// default to true when omitted on a Warn or Enforce guardrail policy.
func EffectiveUserPDBPolicy(guardrails []workloadsv1.WorkloadClassGuardrail) UserPDBPolicyDecision {
	decision := UserPDBPolicyDecision{
		ZeroDisruptionMode: workloadsv1.UserPDBModeAllow,
		OverlappingMode:    workloadsv1.UserPDBModeAllow,
	}
	for i := range guardrails {
		p := guardrails[i].Spec.Constraints.Disruption.UserPDBPolicy
		if p == nil || p.Mode == "" || p.Mode == workloadsv1.UserPDBModeAllow {
			continue
		}
		if p.BlockZeroDisruption == nil || *p.BlockZeroDisruption {
			decision.ZeroDisruptionMode = stricterUserPDBMode(decision.ZeroDisruptionMode, p.Mode)
		}
		if p.BlockOverlapping == nil || *p.BlockOverlapping {
			decision.OverlappingMode = stricterUserPDBMode(decision.OverlappingMode, p.Mode)
		}
	}
	return decision
}

// IsZeroDisruptionPDB reports whether a PodDisruptionBudget statically allows zero voluntary evictions
// (maxUnavailable of 0 or "0%", or minAvailable of "100%").
func IsZeroDisruptionPDB(pdb *policyv1.PodDisruptionBudget) (bool, string) {
	if pdb == nil {
		return false, ""
	}
	if mu := pdb.Spec.MaxUnavailable; mu != nil {
		if (mu.Type == intstr.Int && mu.IntVal == 0) ||
			(mu.Type == intstr.String && (mu.StrVal == "0%" || mu.StrVal == "0")) {
			return true, fmt.Sprintf("maxUnavailable %s allows zero voluntary evictions and can block node drains", mu.String())
		}
	}
	if ma := pdb.Spec.MinAvailable; ma != nil {
		if ma.Type == intstr.String && ma.StrVal == "100%" {
			return true, "minAvailable 100% allows zero voluntary evictions and can block node drains"
		}
	}
	return false, ""
}

// OverlappingUserPDBNames returns the sorted names of existing user-authored PDBs in the same
// namespace that co-select at least one active Pod matched by pdb. Checking actual live Pod labels
// avoids false positives on disjoint-key selectors (e.g. {app: web} vs {team: payments}).
func OverlappingUserPDBNames(
	pdb *policyv1.PodDisruptionBudget,
	existingPDBs []policyv1.PodDisruptionBudget,
	namespacePods []corev1.Pod,
) []string {
	if pdb == nil || pdb.Spec.Selector == nil {
		return nil
	}
	sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return nil
	}

	var matchedPods []*corev1.Pod
	for i := range namespacePods {
		pod := &namespacePods[i]
		if pod.Namespace != pdb.Namespace || !IsPodActive(pod) {
			continue
		}
		if sel.Matches(labels.Set(pod.Labels)) {
			matchedPods = append(matchedPods, pod)
		}
	}
	if len(matchedPods) == 0 {
		return nil
	}

	var overlapping []string
	for i := range existingPDBs {
		other := &existingPDBs[i]
		if other.Namespace != pdb.Namespace || other.Name == pdb.Name || !other.DeletionTimestamp.IsZero() {
			continue
		}
		if IsWorkloadClassManagedPDB(other) || other.Spec.Selector == nil {
			continue
		}
		otherSel, err := metav1.LabelSelectorAsSelector(other.Spec.Selector)
		if err != nil {
			continue
		}
		for _, pod := range matchedPods {
			if otherSel.Matches(labels.Set(pod.Labels)) {
				overlapping = append(overlapping, other.Name)
				break
			}
		}
	}
	slices.Sort(overlapping)
	return overlapping
}

// ValidateUserPDB checks a user-authored PodDisruptionBudget against the effective per-check
// UserPDBPolicy across cluster WorkloadClassGuardrails, returning warning messages and blocking
// rejection messages separately. WorkloadClass-managed PDBs are always skipped.
func ValidateUserPDB(
	pdb *policyv1.PodDisruptionBudget,
	existingPDBs []policyv1.PodDisruptionBudget,
	namespacePods []corev1.Pod,
	guardrails []workloadsv1.WorkloadClassGuardrail,
) (warnings, rejections []string) {
	if pdb == nil || IsWorkloadClassManagedPDB(pdb) {
		return nil, nil
	}

	decision := EffectiveUserPDBPolicy(guardrails)
	if !decision.IsActive() {
		return nil, nil
	}

	record := func(mode workloadsv1.UserPDBEnforcementMode, msg string) {
		switch mode {
		case workloadsv1.UserPDBModeEnforce:
			rejections = append(rejections, msg)
		case workloadsv1.UserPDBModeWarn:
			warnings = append(warnings, msg)
		}
	}

	if decision.ZeroDisruptionMode != workloadsv1.UserPDBModeAllow {
		if zero, reason := IsZeroDisruptionPDB(pdb); zero {
			record(decision.ZeroDisruptionMode, reason)
		}
	}

	if decision.OverlappingMode != workloadsv1.UserPDBModeAllow {
		if overlapping := OverlappingUserPDBNames(pdb, existingPDBs, namespacePods); len(overlapping) > 0 {
			record(decision.OverlappingMode, fmt.Sprintf(
				"matches active Pod(s) already covered by user PodDisruptionBudget(s) %s; Pods matched by multiple PDBs cannot be evicted",
				QuotedList(overlapping),
			))
		}
	}

	return warnings, rejections
}
