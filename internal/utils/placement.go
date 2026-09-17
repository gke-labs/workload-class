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
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// ValidatePlacementAgainstGuardrail checks whether a WorkloadClass's placement policy
// violates the placement constraints of a single WorkloadClassGuardrail.
func ValidatePlacementAgainstGuardrail(wc *workloadsv1.WorkloadClass, g *workloadsv1.WorkloadClassGuardrail) []string {
	return ValidatePlacementAgainstGuardrails(wc, []workloadsv1.WorkloadClassGuardrail{*g})
}

// ValidatePlacementAgainstGuardrails checks whether a WorkloadClass's placement policy
// violates the placement constraints of any of the provided WorkloadClassGuardrails.
func ValidatePlacementAgainstGuardrails(wc *workloadsv1.WorkloadClass, guardrails []workloadsv1.WorkloadClassGuardrail) []string {
	var violations []string
	seen := make(map[string]bool)
	wcSpot := wc.Spec.PlacementPolicy.SpotPlacement
	wcType := wcSpot.Type

	// Default to Spot
	if wcType == "" {
		wcType = workloadsv1.SpotPlacementTypeSpot
	}

	wcSpotRatio := 100
	if wcType == workloadsv1.SpotPlacementTypeOnDemand {
		wcSpotRatio = 0
	} else if wcSpot.SpotRatio != "" {
		if parsed, err := strconv.Atoi(strings.TrimSuffix(wcSpot.SpotRatio, "%")); err == nil {
			wcSpotRatio = parsed
		}
	}

	wcFallbackAction := wcSpot.Fallback.Action
	if wcFallbackAction == "" {
		wcFallbackAction = workloadsv1.FallbackActionFail
	}

	wcReversionAction := wcSpot.Reversion.Action
	if wcReversionAction == "" {
		wcReversionAction = workloadsv1.ReversionActionNone
	}

	for _, g := range guardrails {
		guardrailSpot := g.Spec.Constraints.Placement.SpotPlacement
		validateEnforcement(&guardrailSpot, wcType, wcSpotRatio, seen, &violations)
		validateMinSpotRatio(&guardrailSpot, wcSpotRatio, seen, &violations)
		validateFallback(&guardrailSpot, wcFallbackAction, seen, &violations)
		validateReversion(&wcSpot, &guardrailSpot, wcReversionAction, wcFallbackAction, seen, &violations)
	}

	return violations
}

func validateEnforcement(guardrailSpot *workloadsv1.SpotPlacement, wcType workloadsv1.SpotPlacementType, wcSpotRatio int, seen map[string]bool, violations *[]string) {
	gMode := guardrailSpot.EnforcementMode
	if gMode == "" {
		gMode = workloadsv1.EnforcementAllowed
	}

	switch gMode {
	case workloadsv1.EnforcementRequired:
		if wcType != workloadsv1.SpotPlacementTypeSpot {
			addViolation(fmt.Sprintf("spotPlacement type %s is not allowed when guardrail enforcementMode is Required", wcType), seen, violations)
		} else if wcSpotRatio == 0 {
			addViolation("spotRatio cannot be 0% when guardrail enforcementMode is Required", seen, violations)
		}
	case workloadsv1.EnforcementForbidden:
		if wcType == workloadsv1.SpotPlacementTypeSpot {
			addViolation(fmt.Sprintf("spotPlacement type %s is not allowed when guardrail enforcementMode is Forbidden", wcType), seen, violations)
		}
	}
}

func validateMinSpotRatio(guardrailSpot *workloadsv1.SpotPlacement, wcSpotRatio int, seen map[string]bool, violations *[]string) {
	if guardrailSpot.MinSpotRatio == "" {
		return
	}

	if minRatio, err := strconv.Atoi(strings.TrimSuffix(guardrailSpot.MinSpotRatio, "%")); err == nil {
		if wcSpotRatio < minRatio {
			addViolation(fmt.Sprintf("spotRatio %d%% is less than guardrail minSpotRatio %d%%", wcSpotRatio, minRatio), seen, violations)
		}
	}
}

func validateFallback(guardrailSpot *workloadsv1.SpotPlacement, wcFallbackAction workloadsv1.FallbackAction, seen map[string]bool, violations *[]string) {
	allowFallback := guardrailSpot.Fallback.AllowFallbackToOnDemand == nil || *guardrailSpot.Fallback.AllowFallbackToOnDemand
	if !allowFallback && wcFallbackAction == workloadsv1.FallbackActionFallbackToOnDemand {
		addViolation("fallback action FallbackToOnDemand is not allowed when guardrail allowFallbackToOnDemand is false", seen, violations)
	}
	if guardrailSpot.Fallback.MaxFallbackRatio != nil && wcFallbackAction == workloadsv1.FallbackActionFallbackToOnDemand {
		isZero := (guardrailSpot.Fallback.MaxFallbackRatio.Type == intstr.Int && guardrailSpot.Fallback.MaxFallbackRatio.IntVal == 0) ||
			(guardrailSpot.Fallback.MaxFallbackRatio.Type == intstr.String && (guardrailSpot.Fallback.MaxFallbackRatio.StrVal == "0%" || guardrailSpot.Fallback.MaxFallbackRatio.StrVal == "0"))
		if isZero {
			addViolation("fallback action FallbackToOnDemand is not allowed when guardrail maxFallbackRatio is 0", seen, violations)
		}
	}
}

func validateReversion(wcSpot *workloadsv1.SpotPlacementPolicy, guardrailSpot *workloadsv1.SpotPlacement, wcReversionAction workloadsv1.ReversionAction, wcFallbackAction workloadsv1.FallbackAction, seen map[string]bool, violations *[]string) {
	if guardrailSpot.Reversion == nil {
		return
	}

	if guardrailSpot.Reversion.RequiredReversionAction != "" {
		if (wcFallbackAction == workloadsv1.FallbackActionFallbackToOnDemand ||
			(wcSpot.Reversion.Action != "" && wcSpot.Reversion.Action != workloadsv1.ReversionActionNone)) &&
			wcReversionAction != guardrailSpot.Reversion.RequiredReversionAction {
			addViolation(fmt.Sprintf("reversion action %s does not match guardrail requiredReversionAction %s", wcReversionAction, guardrailSpot.Reversion.RequiredReversionAction), seen, violations)
		}
	}

	if guardrailSpot.Reversion.MaxFallbackDuration != nil {
		if wcFallbackAction == workloadsv1.FallbackActionFallbackToOnDemand && wcReversionAction == workloadsv1.ReversionActionNone {
			addViolation("reversion action cannot be None when fallback action is FallbackToOnDemand and guardrail specifies maxFallbackDuration", seen, violations)
		}
		if wcSpot.Reversion.MinDurationOnFallback != nil &&
			wcSpot.Reversion.MinDurationOnFallback.Duration > guardrailSpot.Reversion.MaxFallbackDuration.Duration {
			addViolation(fmt.Sprintf("minDurationOnFallback %s exceeds guardrail maxFallbackDuration %s", wcSpot.Reversion.MinDurationOnFallback.Duration, guardrailSpot.Reversion.MaxFallbackDuration.Duration), seen, violations)
		}
	}
}

func addViolation(msg string, seen map[string]bool, violations *[]string) {
	if !seen[msg] {
		seen[msg] = true
		*violations = append(*violations, msg)
	}
}
