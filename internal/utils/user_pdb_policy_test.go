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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func boolPtr(b bool) *bool { return &b }

func guardrailWithPolicy(p *workloadsv1.UserPDBPolicy) workloadsv1.WorkloadClassGuardrail {
	return workloadsv1.WorkloadClassGuardrail{
		Spec: workloadsv1.WorkloadClassGuardrailSpec{
			Constraints: workloadsv1.Constraints{
				Disruption: workloadsv1.Disruption{UserPDBPolicy: p},
			},
		},
	}
}

func TestEffectiveUserPDBPolicy(t *testing.T) {
	tests := []struct {
		name       string
		guardrails []workloadsv1.WorkloadClassGuardrail
		want       UserPDBPolicyDecision
	}{
		{
			name: "no_guardrails_defaults_to_allow",
			want: UserPDBPolicyDecision{
				ZeroDisruptionMode: workloadsv1.UserPDBModeAllow,
				OverlappingMode:    workloadsv1.UserPDBModeAllow,
			},
		},
		{
			name: "warn_with_default_booleans",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				guardrailWithPolicy(&workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeWarn}),
			},
			want: UserPDBPolicyDecision{
				ZeroDisruptionMode: workloadsv1.UserPDBModeWarn,
				OverlappingMode:    workloadsv1.UserPDBModeWarn,
			},
		},
		{
			name: "per_check_strictest_mode_mixed_guardrails",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				guardrailWithPolicy(&workloadsv1.UserPDBPolicy{
					Mode:                workloadsv1.UserPDBModeWarn,
					BlockZeroDisruption: boolPtr(true),
					BlockOverlapping:    boolPtr(false),
				}),
				guardrailWithPolicy(&workloadsv1.UserPDBPolicy{
					Mode:                workloadsv1.UserPDBModeEnforce,
					BlockZeroDisruption: boolPtr(false),
					BlockOverlapping:    boolPtr(true),
				}),
			},
			want: UserPDBPolicyDecision{
				ZeroDisruptionMode: workloadsv1.UserPDBModeWarn,
				OverlappingMode:    workloadsv1.UserPDBModeEnforce,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EffectiveUserPDBPolicy(tt.guardrails); got != tt.want {
				t.Errorf("EffectiveUserPDBPolicy() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestIsZeroDisruptionPDB(t *testing.T) {
	zeroInt, oneInt := intstr.FromInt32(0), intstr.FromInt32(1)
	zeroPct, hundredPct, fiftyPct := intstr.FromString("0%"), intstr.FromString("100%"), intstr.FromString("50%")
	tests := []struct {
		name string
		pdb  *policyv1.PodDisruptionBudget
		want bool
	}{
		{"nil", nil, false},
		{"maxUnavailable_0_int", &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zeroInt}}, true},
		{"maxUnavailable_0_pct", &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zeroPct}}, true},
		{"minAvailable_100_pct", &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: &hundredPct}}, true},
		{"maxUnavailable_1", &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &oneInt}}, false},
		{"minAvailable_50_pct", &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: &fiftyPct}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := IsZeroDisruptionPDB(tt.pdb); got != tt.want {
				t.Errorf("IsZeroDisruptionPDB() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateUserPDB(t *testing.T) {
	zeroInt, oneInt := intstr.FromInt32(0), intstr.FromInt32(1)
	isController := true
	enforceAll := []workloadsv1.WorkloadClassGuardrail{
		guardrailWithPolicy(&workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeEnforce}),
	}
	wcOwnedPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name: "workload-pod-1", Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "workloads.gke.io/v1", Kind: "WorkloadClass", Name: "wc", Controller: &isController,
			}},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &zeroInt,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}
	if w, r := ValidateUserPDB(wcOwnedPDB, nil, nil, enforceAll); len(w) != 0 || len(r) != 0 {
		t.Fatalf("WorkloadClass-managed PDB should be skipped, got warn=%v reject=%v", w, r)
	}

	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "default", Labels: map[string]string{"app": "web", "tier": "fe"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pay-0", Namespace: "default", Labels: map[string]string{"team": "payments"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "done-0", Namespace: "default", Labels: map[string]string{"app": "web", "old": "yes"}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
	}
	candidate := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pdb", Namespace: "default"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &zeroInt,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}
	existing := []policyv1.PodDisruptionBudget{
		*candidate, *wcOwnedPDB,
		{ObjectMeta: metav1.ObjectMeta{Name: "z-pdb", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &oneInt, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web", "tier": "fe"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "a-pdb", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &oneInt, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "disjoint-keys-pdb", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &oneInt, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "payments"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "succeeded-only-pdb", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &oneInt, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"old": "yes"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "other-ns-pdb", Namespace: "other"}, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &oneInt, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}}},
	}

	warns, rejects := ValidateUserPDB(candidate, existing, pods, enforceAll)
	if len(warns) != 0 || len(rejects) != 2 {
		t.Fatalf("ValidateUserPDB(enforceAll) = (warn=%v, reject=%v), want 0 warns and 2 rejects", warns, rejects)
	}
	if !strings.Contains(rejects[0], "zero voluntary evictions") {
		t.Errorf("rejects[0] = %q, want zero-eviction message", rejects[0])
	}
	if !strings.Contains(rejects[1], "'a-pdb', 'z-pdb'") || strings.Contains(rejects[1], "disjoint-keys-pdb") || strings.Contains(rejects[1], "succeeded-only-pdb") {
		t.Errorf("rejects[1] = %q, want only sorted co-selected 'a-pdb', 'z-pdb'", rejects[1])
	}

	// Mixed guardrails: Warn zero-disruption, Enforce overlap (no cross-contamination to Enforce zero-disruption).
	mixed := []workloadsv1.WorkloadClassGuardrail{
		guardrailWithPolicy(&workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeWarn, BlockZeroDisruption: boolPtr(true), BlockOverlapping: boolPtr(false)}),
		guardrailWithPolicy(&workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeEnforce, BlockZeroDisruption: boolPtr(false), BlockOverlapping: boolPtr(true)}),
	}
	warns, rejects = ValidateUserPDB(candidate, existing, pods, mixed)
	if len(warns) != 1 || !strings.Contains(warns[0], "zero voluntary evictions") {
		t.Errorf("mixed warns = %v, want 1 zero-disruption warning", warns)
	}
	if len(rejects) != 1 || !strings.Contains(rejects[0], "'a-pdb', 'z-pdb'") {
		t.Errorf("mixed rejects = %v, want 1 overlap rejection", rejects)
	}

	// Disjoint-key selectors with no co-selected active Pod do not false-positive even in Enforce mode.
	noOverlap := []policyv1.PodDisruptionBudget{
		{ObjectMeta: metav1.ObjectMeta{Name: "payments-pdb", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &oneInt, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "payments"}}}},
	}
	validWeb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "web-pdb", Namespace: "default"},
		Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &oneInt, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}},
	}
	if w, r := ValidateUserPDB(validWeb, noOverlap, pods, enforceAll); len(w) != 0 || len(r) != 0 {
		t.Errorf("disjoint-key selectors with separate live Pods should be allowed, got warn=%v reject=%v", w, r)
	}
}
