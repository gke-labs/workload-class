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
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestParseGKESpotGuardrailParameters(t *testing.T) {
	t.Run("all_fields_set", func(t *testing.T) {
		raw := `{"enforcementMode":"Required","minSpotRatio":"80%","fallback":{"allowFallbackToOnDemand":false,"maxFallbackRatio":"20%"},"reversion":{"requiredReversionAction":"Active","maxFallbackDuration":"2h"}}`
		got, err := ParseGKESpotGuardrailParameters([]byte(raw))
		if err != nil {
			t.Fatalf("ParseGKESpotGuardrailParameters() returned unexpected error: %v", err)
		}
		if got.EnforcementMode != workloadsv1.RequiredEnforcementMode {
			t.Errorf("EnforcementMode = %q, want %q", got.EnforcementMode, workloadsv1.RequiredEnforcementMode)
		}
		if got.MinSpotRatio == nil || *got.MinSpotRatio != "80%" {
			t.Errorf("MinSpotRatio = %v, want \"80%%\"", got.MinSpotRatio)
		}
		if got.Fallback == nil || got.Fallback.AllowFallbackToOnDemand == nil || *got.Fallback.AllowFallbackToOnDemand {
			t.Errorf("Fallback.AllowFallbackToOnDemand = %v, want false", got.Fallback)
		}
		if got.Fallback == nil || got.Fallback.MaxFallbackRatio == nil || *got.Fallback.MaxFallbackRatio != intstr.FromString("20%") {
			t.Errorf("Fallback.MaxFallbackRatio = %v, want \"20%%\"", got.Fallback)
		}
		if got.Reversion == nil || got.Reversion.RequiredReversionAction == nil || *got.Reversion.RequiredReversionAction != workloadsv1.ActiveReversionAction {
			t.Errorf("Reversion.RequiredReversionAction = %v, want %q", got.Reversion, workloadsv1.ActiveReversionAction)
		}
		if got.Reversion == nil || got.Reversion.MaxFallbackDuration == nil || got.Reversion.MaxFallbackDuration.Duration != 2*time.Hour {
			t.Errorf("Reversion.MaxFallbackDuration = %v, want 2h", got.Reversion)
		}
	})

	t.Run("omitted_fields_are_not_defaulted", func(t *testing.T) {
		got, err := ParseGKESpotGuardrailParameters([]byte(`{"fallback":{}}`))
		if err != nil {
			t.Fatalf("ParseGKESpotGuardrailParameters() returned unexpected error: %v", err)
		}
		if got.EnforcementMode != "" {
			t.Errorf("EnforcementMode = %q, want empty (omitted)", got.EnforcementMode)
		}
		if got.MinSpotRatio != nil {
			t.Errorf("MinSpotRatio = %q, want nil (omitted)", *got.MinSpotRatio)
		}
		if got.Fallback == nil {
			t.Fatalf("Fallback = nil, want non-nil")
		}
		if got.Fallback.AllowFallbackToOnDemand != nil {
			t.Errorf("Fallback.AllowFallbackToOnDemand = %v, want nil (omitted)", *got.Fallback.AllowFallbackToOnDemand)
		}
		if got.Reversion != nil {
			t.Errorf("Reversion = %+v, want nil (omitted)", got.Reversion)
		}
	})

	errorCases := []struct {
		name          string
		raw           string
		wantSubstring string
	}{
		{
			name:          "unknown_field",
			raw:           `{"enforcementMode":"Allowed","bogus":true}`,
			wantSubstring: `unknown field "bogus"`,
		},
		{
			name:          "unknown_nested_field",
			raw:           `{"fallback":{"bogus":true}}`,
			wantSubstring: `unknown field "bogus"`,
		},
		{
			name:          "invalid_json",
			raw:           `{"enforcementMode":`,
			wantSubstring: "unexpected EOF",
		},
		{
			name:          "wrong_type",
			raw:           `{"minSpotRatio":80}`,
			wantSubstring: "cannot unmarshal number",
		},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseGKESpotGuardrailParameters([]byte(tc.raw))
			if err == nil {
				t.Fatalf("ParseGKESpotGuardrailParameters(%s) = %+v, want error containing %q", tc.raw, got, tc.wantSubstring)
			}
			if !strings.Contains(err.Error(), tc.wantSubstring) {
				t.Errorf("ParseGKESpotGuardrailParameters(%s) error = %q, want substring %q", tc.raw, err, tc.wantSubstring)
			}
		})
	}
}
