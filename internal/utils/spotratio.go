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
)

// ParsePercentage parses an integer percentage string between "0%" and "100%" (e.g. "80%").
func ParsePercentage(s string) (int, error) {
	if !strings.HasSuffix(s, "%") {
		return 0, fmt.Errorf("must end with %%")
	}
	v, err := strconv.Atoi(strings.TrimSuffix(s, "%"))
	if err != nil {
		return 0, err
	}
	if v < 0 || v > 100 {
		return 0, fmt.Errorf("must be between 0%% and 100%%")
	}
	return v, nil
}

// SpotRatioPercent parses a CapacityStrategy.SpotRatio value, treating an empty string as the default of 100%.
func SpotRatioPercent(spotRatio string) (int, error) {
	if spotRatio == "" {
		return 100, nil
	}
	v, err := ParsePercentage(spotRatio)
	if err != nil {
		return 0, fmt.Errorf("invalid spotRatio %q: %w", spotRatio, err)
	}
	return v, nil
}

// ShouldUseSpot is the single rule for placing the next Pod of a workload under a SpotRatio: the Pod goes to
// Spot while the existing Spot Pods are below spotRatio percent of the workload's Pods including the new one:
//
//	spotExisting × 100 < (totalExisting + 1) × spotRatio
//
// The rule fills Spot first and lets the Spot share land on or just above the target rather than below it.
// For example, at 80% the first Pod goes to Spot (0 < 80), the second goes to Spot (100 < 160), and the fifth
// goes to On-Demand (400 < 400 is false), giving 4 of 5 on Spot. At 0% no Pod ever goes to Spot; at 100% every
// Pod does.
//
// The Pod placement webhook uses it at admission. Active reversion uses it through SpotShareMet with the
// workload's current Pods, so both agree on when the ratio is satisfied.
func ShouldUseSpot(spotExisting, totalExisting, spotRatio int) bool {
	return spotExisting*100 < (totalExisting+1)*spotRatio
}

// SpotShareMet reports whether a workload with total active Pods, spot of them on Spot, already satisfies its
// SpotRatio, i.e. moving one more Pod from On-Demand to Spot would not be required by ShouldUseSpot.
//
// Reversion recreates a Pod rather than adding one, so the total stays the same: the question is whether the
// workload at its current size, with one Pod moved to Spot, would still be placed on Spot by the admission
// rule. That is ShouldUseSpot(spot, total-1, spotRatio) negated.
func SpotShareMet(spot, total, spotRatio int) bool {
	if total <= 0 {
		return true
	}
	return !ShouldUseSpot(spot, total-1, spotRatio)
}
