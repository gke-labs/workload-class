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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func boolPtr(b bool) *bool {
	return &b
}

func TestValidatePlacementAgainstGuardrails(t *testing.T) {
	intZero := intstr.FromInt(0)
	strZeroPct := intstr.FromString("0%")
	dur1h := metav1.Duration{Duration: 1 * time.Hour}
	dur2h := metav1.Duration{Duration: 2 * time.Hour}

	tests := []struct {
		name           string
		wcSpot         workloadsv1.SpotPlacementPolicy
		guardrailSpots []workloadsv1.SpotPlacement
		wantViolations []string
	}{
		{
			name:   "default empty policy against default empty guardrail passes",
			wcSpot: workloadsv1.SpotPlacementPolicy{},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{},
			},
			wantViolations: nil,
		},
		{
			name: "Required enforcement passes with Spot and 100%",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "100%",
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{EnforcementMode: workloadsv1.EnforcementRequired},
			},
			wantViolations: nil,
		},
		{
			name: "Required enforcement fails with OnDemand",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Type: workloadsv1.SpotPlacementTypeOnDemand,
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{EnforcementMode: workloadsv1.EnforcementRequired},
			},
			wantViolations: []string{
				"spotPlacement type OnDemand is not allowed when guardrail enforcementMode is Required",
			},
		},
		{
			name: "Required enforcement fails with Spot 0%",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "0%",
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{EnforcementMode: workloadsv1.EnforcementRequired},
			},
			wantViolations: []string{
				"spotRatio cannot be 0% when guardrail enforcementMode is Required",
			},
		},
		{
			name: "Forbidden enforcement fails with Spot",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Type: workloadsv1.SpotPlacementTypeSpot,
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{EnforcementMode: workloadsv1.EnforcementForbidden},
			},
			wantViolations: []string{
				"spotPlacement type Spot is not allowed when guardrail enforcementMode is Forbidden",
			},
		},
		{
			name: "Forbidden enforcement passes with OnDemand",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Type: workloadsv1.SpotPlacementTypeOnDemand,
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{EnforcementMode: workloadsv1.EnforcementForbidden},
			},
			wantViolations: nil,
		},
		{
			name: "minSpotRatio fails when spotRatio is lower",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "60%",
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{MinSpotRatio: "80%"},
			},
			wantViolations: []string{
				"spotRatio 60% is less than guardrail minSpotRatio 80%",
			},
		},
		{
			name: "minSpotRatio fails when type is OnDemand (effective 0%)",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Type: workloadsv1.SpotPlacementTypeOnDemand,
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{MinSpotRatio: "50%"},
			},
			wantViolations: []string{
				"spotRatio 0% is less than guardrail minSpotRatio 50%",
			},
		},
		{
			name: "allowFallbackToOnDemand false fails with FallbackToOnDemand",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFallbackToOnDemand,
				},
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{
					Fallback: workloadsv1.Fallback{
						AllowFallbackToOnDemand: boolPtr(false),
					},
				},
			},
			wantViolations: []string{
				"fallback action FallbackToOnDemand is not allowed when guardrail allowFallbackToOnDemand is false",
			},
		},
		{
			name: "maxFallbackRatio 0 int fails with FallbackToOnDemand",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFallbackToOnDemand,
				},
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{
					Fallback: workloadsv1.Fallback{
						MaxFallbackRatio: &intZero,
					},
				},
			},
			wantViolations: []string{
				"fallback action FallbackToOnDemand is not allowed when guardrail maxFallbackRatio is 0",
			},
		},
		{
			name: "maxFallbackRatio 0% string fails with FallbackToOnDemand",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFallbackToOnDemand,
				},
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{
					Fallback: workloadsv1.Fallback{
						MaxFallbackRatio: &strZeroPct,
					},
				},
			},
			wantViolations: []string{
				"fallback action FallbackToOnDemand is not allowed when guardrail maxFallbackRatio is 0",
			},
		},
		{
			name: "requiredReversionAction passes when fallback is Fail and reversion is None",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFail,
				},
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{
					Reversion: &workloadsv1.Reversion{
						RequiredReversionAction: workloadsv1.ReversionActionActive,
					},
				},
			},
			wantViolations: nil,
		},
		{
			name: "requiredReversionAction fails when fallback is FallbackToOnDemand and reversion mismatches",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFallbackToOnDemand,
				},
				Reversion: workloadsv1.SpotReversionPolicy{
					Action: workloadsv1.ReversionActionLazy,
				},
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{
					Reversion: &workloadsv1.Reversion{
						RequiredReversionAction: workloadsv1.ReversionActionActive,
					},
				},
			},
			wantViolations: []string{
				"reversion action Lazy does not match guardrail requiredReversionAction Active",
			},
		},
		{
			name: "maxFallbackDuration fails when fallback is FallbackToOnDemand and reversion is None",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFallbackToOnDemand,
				},
				Reversion: workloadsv1.SpotReversionPolicy{
					Action: workloadsv1.ReversionActionNone,
				},
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{
					Reversion: &workloadsv1.Reversion{
						MaxFallbackDuration: &dur2h,
					},
				},
			},
			wantViolations: []string{
				"reversion action cannot be None when fallback action is FallbackToOnDemand and guardrail specifies maxFallbackDuration",
			},
		},
		{
			name: "maxFallbackDuration fails when minDurationOnFallback exceeds it",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFallbackToOnDemand,
				},
				Reversion: workloadsv1.SpotReversionPolicy{
					Action:                workloadsv1.ReversionActionActive,
					MinDurationOnFallback: &dur2h,
				},
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{
					Reversion: &workloadsv1.Reversion{
						MaxFallbackDuration: &dur1h,
					},
				},
			},
			wantViolations: []string{
				"minDurationOnFallback 2h0m0s exceeds guardrail maxFallbackDuration 1h0m0s",
			},
		},
		{
			name: "multiple guardrails deduplicate identical violations and combine distinct ones",
			wcSpot: workloadsv1.SpotPlacementPolicy{
				Type: workloadsv1.SpotPlacementTypeOnDemand,
			},
			guardrailSpots: []workloadsv1.SpotPlacement{
				{EnforcementMode: workloadsv1.EnforcementRequired},
				{EnforcementMode: workloadsv1.EnforcementRequired, MinSpotRatio: "80%"},
			},
			wantViolations: []string{
				"spotPlacement type OnDemand is not allowed when guardrail enforcementMode is Required",
				"spotRatio 0% is less than guardrail minSpotRatio 80%",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wc := &workloadsv1.WorkloadClass{
				Spec: workloadsv1.WorkloadClassSpec{
					PlacementPolicy: workloadsv1.PlacementPolicy{
						SpotPlacement: tc.wcSpot,
					},
				},
			}
			guardrails := make([]workloadsv1.WorkloadClassGuardrail, len(tc.guardrailSpots))
			for i, guardrailSpot := range tc.guardrailSpots {
				guardrails[i] = workloadsv1.WorkloadClassGuardrail{
					Spec: workloadsv1.WorkloadClassGuardrailSpec{
						Constraints: workloadsv1.Constraints{
							Placement: workloadsv1.Placement{
								SpotPlacement: guardrailSpot,
							},
						},
					},
				}
			}

			got := ValidatePlacementAgainstGuardrails(wc, guardrails)
			if len(got) != len(tc.wantViolations) {
				t.Fatalf("got %d violations (%v), want %d (%v)", len(got), got, len(tc.wantViolations), tc.wantViolations)
			}
			for i := range got {
				if got[i] != tc.wantViolations[i] {
					t.Errorf("violation[%d] = %q, want %q", i, got[i], tc.wantViolations[i])
				}
			}
		})
	}
}
