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

	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// EffectiveUserPDBPolicy aggregates UserPDBPolicy settings across all cluster WorkloadClassGuardrails
// using strictest-wins precedence: Enforce > Warn > Allow. Boolean check flags default to true when
// omitted on an active (Warn or Enforce) guardrail policy.
func EffectiveUserPDBPolicy(guardrails []workloadsv1.WorkloadClassGuardrail) (mode workloadsv1.UserPDBEnforcementMode, blockZeroDisruption, blockOverlapping bool) {
	mode = workloadsv1.UserPDBModeAllow
	for i := range guardrails {
		p := guardrails[i].Spec.Constraints.Disruption.UserPDBPolicy
		if p == nil || p.Mode == "" || p.Mode == workloadsv1.UserPDBModeAllow {
			continue
		}
		if p.Mode == workloadsv1.UserPDBModeEnforce || mode == workloadsv1.UserPDBModeAllow {
			mode = p.Mode
		}
		if p.BlockZeroDisruption == nil || *p.BlockZeroDisruption {
			blockZeroDisruption = true
		}
		if p.BlockOverlapping == nil || *p.BlockOverlapping {
			blockOverlapping = true
		}
	}
	return mode, blockZeroDisruption, blockOverlapping
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

// ValidateUserPDB checks a user-authored PodDisruptionBudget against the effective UserPDBPolicy
// across cluster WorkloadClassGuardrails. WorkloadClass-managed PDBs are always skipped.
func ValidateUserPDB(
	pdb *policyv1.PodDisruptionBudget,
	existingPDBs []policyv1.PodDisruptionBudget,
	guardrails []workloadsv1.WorkloadClassGuardrail,
) (workloadsv1.UserPDBEnforcementMode, []string) {
	if pdb == nil || IsWorkloadClassManagedPDB(pdb) {
		return workloadsv1.UserPDBModeAllow, nil
	}

	mode, blockZero, blockOverlapping := EffectiveUserPDBPolicy(guardrails)
	if mode == workloadsv1.UserPDBModeAllow {
		return mode, nil
	}

	var violations []string
	if blockZero {
		if zero, reason := IsZeroDisruptionPDB(pdb); zero {
			violations = append(violations, reason)
		}
	}

	if blockOverlapping && pdb.Spec.Selector != nil {
		var overlapping []string
		for i := range existingPDBs {
			other := &existingPDBs[i]
			if other.Namespace != pdb.Namespace || other.Name == pdb.Name || !other.DeletionTimestamp.IsZero() {
				continue
			}
			if IsWorkloadClassManagedPDB(other) || other.Spec.Selector == nil {
				continue
			}
			overlap, err := SelectorsOverlap(pdb.Spec.Selector, other.Spec.Selector)
			if err != nil || !overlap {
				continue
			}
			overlapping = append(overlapping, other.Name)
		}
		if len(overlapping) > 0 {
			slices.Sort(overlapping)
			violations = append(violations, fmt.Sprintf(
				"selector overlaps with existing user PodDisruptionBudget(s) %s; Pods matched by multiple PDBs cannot be evicted",
				QuotedList(overlapping),
			))
		}
	}

	return mode, violations
}
