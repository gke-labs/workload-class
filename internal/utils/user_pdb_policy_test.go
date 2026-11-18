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

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func boolPtr(b bool) *bool { return &b }

func TestEffectiveUserPDBPolicy(t *testing.T) {
	tests := []struct {
		name        string
		guardrails  []workloadsv1.WorkloadClassGuardrail
		wantMode    workloadsv1.UserPDBEnforcementMode
		wantZero    bool
		wantOverlap bool
	}{
		{
			name:        "no_guardrails_defaults_to_allow",
			wantMode:    workloadsv1.UserPDBModeAllow,
			wantZero:    false,
			wantOverlap: false,
		},
		{
			name: "warn_with_default_booleans",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				{
					Spec: workloadsv1.WorkloadClassGuardrailSpec{
						Constraints: workloadsv1.Constraints{
							Disruption: workloadsv1.Disruption{
								UserPDBPolicy: &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeWarn},
							},
						},
					},
				},
			},
			wantMode:    workloadsv1.UserPDBModeWarn,
			wantZero:    true,
			wantOverlap: true,
		},
		{
			name: "enforce_beats_warn_and_unions_checks",
			guardrails: []workloadsv1.WorkloadClassGuardrail{
				{
					Spec: workloadsv1.WorkloadClassGuardrailSpec{
						Constraints: workloadsv1.Constraints{
							Disruption: workloadsv1.Disruption{
								UserPDBPolicy: &workloadsv1.UserPDBPolicy{
									Mode:                workloadsv1.UserPDBModeWarn,
									BlockZeroDisruption: boolPtr(true),
									BlockOverlapping:    boolPtr(false),
								},
							},
						},
					},
				},
				{
					Spec: workloadsv1.WorkloadClassGuardrailSpec{
						Constraints: workloadsv1.Constraints{
							Disruption: workloadsv1.Disruption{
								UserPDBPolicy: &workloadsv1.UserPDBPolicy{
									Mode:                workloadsv1.UserPDBModeEnforce,
									BlockZeroDisruption: boolPtr(false),
									BlockOverlapping:    boolPtr(true),
								},
							},
						},
					},
				},
			},
			wantMode:    workloadsv1.UserPDBModeEnforce,
			wantZero:    true,
			wantOverlap: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode, zero, overlap := EffectiveUserPDBPolicy(tt.guardrails)
			if mode != tt.wantMode || zero != tt.wantZero || overlap != tt.wantOverlap {
				t.Errorf("EffectiveUserPDBPolicy() = (%v, %v, %v), want (%v, %v, %v)",
					mode, zero, overlap, tt.wantMode, tt.wantZero, tt.wantOverlap)
			}
		})
	}
}

func TestIsZeroDisruptionPDB(t *testing.T) {
	zeroInt := intstr.FromInt32(0)
	oneInt := intstr.FromInt32(1)
	zeroPct := intstr.FromString("0%")
	hundredPct := intstr.FromString("100%")
	fiftyPct := intstr.FromString("50%")

	tests := []struct {
		name string
		pdb  *policyv1.PodDisruptionBudget
		want bool
	}{
		{name: "nil", pdb: nil, want: false},
		{name: "maxUnavailable_0_int", pdb: &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zeroInt}}, want: true},
		{name: "maxUnavailable_0_pct", pdb: &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zeroPct}}, want: true},
		{name: "minAvailable_100_pct", pdb: &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: &hundredPct}}, want: true},
		{name: "maxUnavailable_1", pdb: &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &oneInt}}, want: false},
		{name: "minAvailable_50_pct", pdb: &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: &fiftyPct}}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := IsZeroDisruptionPDB(tt.pdb)
			if got != tt.want {
				t.Errorf("IsZeroDisruptionPDB() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateUserPDB(t *testing.T) {
	zeroInt := intstr.FromInt32(0)
	oneInt := intstr.FromInt32(1)
	isController := true
	enforceGuardrail := []workloadsv1.WorkloadClassGuardrail{
		{
			Spec: workloadsv1.WorkloadClassGuardrailSpec{
				Constraints: workloadsv1.Constraints{
					Disruption: workloadsv1.Disruption{
						UserPDBPolicy: &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeEnforce},
					},
				},
			},
		},
	}

	wcOwnedPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workload-pod-1",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "workloads.gke.io/v1", Kind: "WorkloadClass", Name: "wc", Controller: &isController},
			},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &zeroInt,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}
	if mode, violations := ValidateUserPDB(wcOwnedPDB, nil, enforceGuardrail); mode != workloadsv1.UserPDBModeAllow || len(violations) != 0 {
		t.Fatalf("WorkloadClass-managed PDB should be skipped, got (%v, %v)", mode, violations)
	}

	candidate := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pdb", Namespace: "default"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &zeroInt,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}
	existing := []policyv1.PodDisruptionBudget{
		*candidate, // self (update) should be ignored
		*wcOwnedPDB,
		{
			ObjectMeta: metav1.ObjectMeta{Name: "z-pdb", Namespace: "default"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &oneInt,
				Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web", "tier": "fe"}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "a-pdb", Namespace: "default"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &oneInt,
				Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "other-ns-pdb", Namespace: "other"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &oneInt,
				Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			},
		},
	}

	mode, violations := ValidateUserPDB(candidate, existing, enforceGuardrail)
	if mode != workloadsv1.UserPDBModeEnforce {
		t.Fatalf("ValidateUserPDB() mode = %v, want Enforce", mode)
	}
	if len(violations) != 2 {
		t.Fatalf("ValidateUserPDB() got %d violations (%v), want 2", len(violations), violations)
	}
	if !strings.Contains(violations[0], "zero voluntary evictions") {
		t.Errorf("violation[0] = %q, want zero-eviction message", violations[0])
	}
	if !strings.Contains(violations[1], "'a-pdb', 'z-pdb'") {
		t.Errorf("violation[1] = %q, want sorted overlap names 'a-pdb', 'z-pdb'", violations[1])
	}
}
