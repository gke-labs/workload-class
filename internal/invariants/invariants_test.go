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

package invariants

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

func validSnapshot(now time.Time) Snapshot {
	closed := intstr.FromInt32(0)
	return Snapshot{
		Now: now,
		Namespaces: []corev1.Namespace{
			{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		},
		WorkloadClasses: []workloadsv1.WorkloadClass{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "critical-batch",
					Namespace:         "default",
					CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Hour)),
				},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "batch"},
					},
					DisruptionPolicy: workloadsv1.DisruptionPolicy{
						AllowedDisruptionsOutsideOfWindow: []workloadsv1.Subject{
							{Kind: "ServiceAccount", Name: "vpa-updater", Namespace: "kube-system"},
						},
					},
				},
				Status: workloadsv1.WorkloadClassStatus{
					MaintenanceReadiness: workloadsv1.ReadinessNotReady,
					Conditions: []metav1.Condition{
						{
							Type:   workloadsv1.ConditionTypeValidated,
							Status: metav1.ConditionTrue,
							Reason: workloadsv1.ReasonValidationPassed,
						},
					},
				},
			},
		},
		Pods: []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "batch-0",
					Namespace: "default",
					UID:       types.UID("uid-batch-0"),
					Labels: map[string]string{
						"app":             "batch",
						utils.PodUIDLabel: "uid-batch-0",
					},
				},
			},
		},
		PDBs: []policyv1.PodDisruptionBudget{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload-batch-0",
					Namespace: "default",
					Labels: map[string]string{
						utils.PodUIDLabel: "uid-batch-0",
					},
				},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MaxUnavailable: &closed,
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							utils.PodUIDLabel: "uid-batch-0",
						},
					},
				},
			},
		},
	}
}

func TestEvaluateAll_GreenHealthySnapshot(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	snap := validSnapshot(now)
	if violations := EvaluateAll(snap); len(violations) != 0 {
		t.Fatalf("expected 0 violations on valid snapshot, got %v", violations)
	}
}

func TestRuleI1_RedInvalidWorkloadClassHoldingClosedPDB(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	snap := validSnapshot(now)
	snap.WorkloadClasses[0].Status.Conditions = []metav1.Condition{
		{
			Type:   workloadsv1.ConditionTypeValidated,
			Status: metav1.ConditionFalse,
			Reason: workloadsv1.ReasonValidationFailed,
		},
	}

	violations := EvaluateI1GuardrailValidationGating(snap)
	if len(violations) != 1 || violations[0].Rule != RuleI1GuardrailValidationGating {
		t.Fatalf("expected I1 violation when Validated=False holds closed PDB, got %v", violations)
	}
}

func TestRuleI2_RedOrphanedAndUserPDBConflict(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

	t.Run("orphaned controller PDB", func(t *testing.T) {
		snap := validSnapshot(now)
		snap.Pods = nil
		violations := EvaluateI2PerPodPDBFidelityAndPrecedence(snap)
		if len(violations) != 1 || violations[0].Rule != RuleI2PerPodPDBFidelityAndPrecedence {
			t.Fatalf("expected I2 violation for orphaned PDB, got %v", violations)
		}
	})

	t.Run("coexisting user PDB", func(t *testing.T) {
		snap := validSnapshot(now)
		one := intstr.FromInt32(1)
		snap.PDBs = append(snap.PDBs, policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "user-batch-pdb",
				Namespace: "default",
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &one,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "batch"},
				},
			},
		})
		violations := EvaluateI2PerPodPDBFidelityAndPrecedence(snap)
		if len(violations) != 1 || violations[0].Rule != RuleI2PerPodPDBFidelityAndPrecedence {
			t.Fatalf("expected I2 violation when user PDB covers the same Pod, got %v", violations)
		}
	})
}

func TestRuleI3_GreenAndRedAuthorizedLeaseBounds(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	open := intstr.FromString("100%")

	t.Run("green active lease for authorized subject", func(t *testing.T) {
		snap := validSnapshot(now)
		snap.PDBs[0].Spec.MaxUnavailable = &open
		snap.PDBs[0].Annotations = map[string]string{
			utils.BypassOwner:      "system:serviceaccount:kube-system:vpa-updater",
			utils.BypassExpiration: now.Add(30 * time.Second).Format(utils.ExpirationFormat),
		}
		if violations := EvaluateAll(snap); len(violations) != 0 {
			t.Fatalf("expected 0 violations for active authorized lease, got %v", violations)
		}
	})

	t.Run("red expired lease or unauthorized subject", func(t *testing.T) {
		snap := validSnapshot(now)
		snap.PDBs[0].Spec.MaxUnavailable = &open
		snap.PDBs[0].Annotations = map[string]string{
			utils.BypassOwner:      "user:attacker@example.com",
			utils.BypassExpiration: now.Add(-10 * time.Second).Format(utils.ExpirationFormat),
		}
		violations := EvaluateI3BoundedAuthorizedLease(snap)
		if len(violations) != 2 {
			t.Fatalf("expected 2 I3 violations (unauthorized owner + expired lease), got %v", violations)
		}
	})
}

func TestRuleI4_RedPrecedenceDisagreement(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	snap := validSnapshot(now)
	// Add a higher-specificity WorkloadClass that is Ready, while PDB remains closed (maxUnavailable=0).
	snap.Pods[0].Labels["tier"] = "backend"
	snap.WorkloadClasses = append(snap.WorkloadClasses, workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "specific-ready-class",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(now),
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "batch", "tier": "backend"},
			},
		},
		Status: workloadsv1.WorkloadClassStatus{
			MaintenanceReadiness: workloadsv1.ReadinessReady,
			Conditions: []metav1.Condition{
				{
					Type:   workloadsv1.ConditionTypeValidated,
					Status: metav1.ConditionTrue,
					Reason: workloadsv1.ReasonValidationPassed,
				},
			},
		},
	})

	violations := EvaluateI4DeterministicClassPrecedence(snap)
	if len(violations) != 1 || violations[0].Rule != RuleI4DeterministicClassPrecedence {
		t.Fatalf("expected I4 violation when higher-specificity class is Ready but PDB is closed, got %v", violations)
	}
}

func TestRuleI5_RedSpotGuardrailAndReversionSafety(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

	t.Run("forbidden Spot enforcement mode", func(t *testing.T) {
		snap := validSnapshot(now)
		snap.WorkloadClasses[0].Spec.CapacityStrategy = &workloadsv1.CapacityStrategy{
			Type:      workloadsv1.SpotPlacementTypeSpot,
			SpotRatio: "100%",
		}
		snap.Guardrails = []workloadsv1.WorkloadClassGuardrail{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster-guardrail"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					PluginConstraints: &workloadsv1.PluginConstraints{
						Placement: []workloadsv1.PluginConstraint{
							{
								Name: workloadsv1.PluginNameGKESpotPlacement,
								Parameters: &apiextensionsv1.JSON{
									Raw: []byte(`{"enforcementMode":"Forbidden"}`),
								},
							},
						},
					},
				},
			},
		}
		violations := EvaluateI5SpotPlacementAndReversionSafety(snap)
		if len(violations) != 1 || violations[0].Rule != RuleI5SpotPlacementAndReversionSafety {
			t.Fatalf("expected I5 violation when Spot is Forbidden by guardrail, got %v", violations)
		}
	})

	t.Run("active reversion during stockout and before minDurationOnFallback", func(t *testing.T) {
		snap := validSnapshot(now)
		minDur := metav1.Duration{Duration: 10 * time.Minute}
		snap.SpotPlacementPolicies = []workloadsv1.GKESpotPlacementPolicy{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "spot-policy"},
				Spec: workloadsv1.GKESpotPlacementPolicySpec{
					Reversion: workloadsv1.SpotReversionPolicy{
						Action:                workloadsv1.ActiveReversionAction,
						MinDurationOnFallback: &minDur,
					},
				},
				Status: workloadsv1.GKESpotPlacementPolicyStatus{
					Conditions: []metav1.Condition{
						{
							Type:   workloadsv1.ConditionTypeSpotCapacityAvailable,
							Status: metav1.ConditionFalse,
							Reason: workloadsv1.ReasonSpotStockout,
						},
					},
				},
			},
		}
		snap.ActiveReversionEviction = &ReversionDecision{
			PodName:           "batch-0",
			PodNamespace:      "default",
			WorkloadClassName: "critical-batch",
			PolicyName:        "spot-policy",
			TimeOnFallback:    2 * time.Minute,
		}
		violations := EvaluateI5SpotPlacementAndReversionSafety(snap)
		if len(violations) != 2 {
			t.Fatalf("expected 2 I5 violations (stockout + premature reversion), got %v", violations)
		}
	})
}
