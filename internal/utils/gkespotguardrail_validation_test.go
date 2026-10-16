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

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestValidatePluginConstraints(t *testing.T) {
	testCases := []struct {
		name               string
		pc                 *workloadsv1.PluginConstraints
		wantViolationCount int
		wantSubstring      string
	}{
		{
			name:               "nil_plugin_constraints_passes",
			pc:                 nil,
			wantViolationCount: 0,
		},
		{
			name: "valid_gke_spot_placement_parameters_passes",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"Required","minSpotRatio":"80%","fallback":{"allowFallbackToOnDemand":true,"maxFallbackRatio":5},"reversion":{"requiredReversionAction":"Lazy","maxFallbackDuration":"30m"}}`),
						},
					},
				},
			},
			wantViolationCount: 0,
		},
		{
			name: "valid_non_gke_plugin_arbitrary_json_object_passes",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: "karpenter-spot-placement",
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"maxSpotPrice":"0.05","instanceFamilies":["m6i","c6i"]}`),
						},
					},
				},
			},
			wantViolationCount: 0,
		},
		{
			name: "empty_plugin_name_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: "   ",
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "name must not be empty",
		},
		{
			name: "unknown_field_in_gke_spot_parameters_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"unknownField":"value"}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "invalid gke-spot-placement parameters",
		},
		{
			name: "invalid_enforcement_mode_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"Optional"}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid enforcementMode "Optional"`,
		},
		{
			name: "invalid_min_spot_ratio_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"minSpotRatio":"80"}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid minSpotRatio "80"`,
		},
		{
			name: "invalid_max_fallback_ratio_percentage_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"fallback":{"maxFallbackRatio":"120%"}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid fallback.maxFallbackRatio "120%"`,
		},
		{
			name: "negative_max_fallback_ratio_int_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"fallback":{"maxFallbackRatio":-1}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "invalid fallback.maxFallbackRatio -1",
		},
		{
			name: "invalid_required_reversion_action_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"reversion":{"requiredReversionAction":"Immediate"}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid reversion.requiredReversionAction "Immediate"`,
		},
		{
			name: "negative_max_fallback_duration_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"reversion":{"maxFallbackDuration":"-10m"}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid reversion.maxFallbackDuration "-10m0s"`,
		},
		{
			name: "plugin_name_with_surrounding_whitespace_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: " " + workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"Required"}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "must not have leading or trailing whitespace",
		},
		{
			name: "nil_parameters_passes",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{Name: workloadsv1.PluginNameGKESpotPlacement, Parameters: nil},
				},
			},
			wantViolationCount: 0,
		},
		{
			name: "non_object_parameters_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name:       "karpenter-spot-placement",
						Parameters: &apiextensionsv1.JSON{Raw: []byte(`[1, 2, 3]`)},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "parameters must be a valid JSON object",
		},
		{
			name: "null_parameters_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name:       workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{Raw: []byte(`null`)},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "parameters must be a valid JSON object, got null",
		},
		{
			name: "required_reversion_none_with_max_fallback_duration_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"reversion":{"requiredReversionAction":"None","maxFallbackDuration":"2h"}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "reversion.maxFallbackDuration cannot be set when reversion.requiredReversionAction is None",
		},
		{
			name: "required_enforcement_with_zero_min_spot_ratio_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"Required","minSpotRatio":"0%"}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `minSpotRatio "0%" contradicts enforcementMode Required`,
		},
		{
			name: "forbidden_enforcement_with_positive_min_spot_ratio_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"Forbidden","minSpotRatio":"20%"}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `minSpotRatio "20%" contradicts enforcementMode Forbidden`,
		},
		{
			name: "forbidden_enforcement_with_zero_min_spot_ratio_passes",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"Forbidden","minSpotRatio":"0%"}`),
						},
					},
				},
			},
			wantViolationCount: 0,
		},
		{
			name: "fallback_disallowed_with_positive_max_fallback_ratio_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"fallback":{"allowFallbackToOnDemand":false,"maxFallbackRatio":"20%"}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "fallback.maxFallbackRatio 20% contradicts fallback.allowFallbackToOnDemand false",
		},
		{
			name: "fallback_disallowed_with_zero_max_fallback_ratio_passes",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"fallback":{"allowFallbackToOnDemand":false,"maxFallbackRatio":0}}`),
						},
					},
				},
			},
			wantViolationCount: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			violations := ValidatePluginConstraints(tc.pc)
			if len(violations) != tc.wantViolationCount {
				t.Fatalf("got %d violations (%v), want %d", len(violations), violations, tc.wantViolationCount)
			}
			if tc.wantSubstring != "" && !strings.Contains(strings.Join(violations, "; "), tc.wantSubstring) {
				t.Errorf("got violations %v, want substring %q", violations, tc.wantSubstring)
			}
		})
	}
}
