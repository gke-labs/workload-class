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
	"strings"
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// AllWeekdays returns every day of the week, as formatted by time.Weekday.String() (e.g. "Monday").
func AllWeekdays() []string {
	return []string{
		time.Sunday.String(),
		time.Monday.String(),
		time.Tuesday.String(),
		time.Wednesday.String(),
		time.Thursday.String(),
		time.Friday.String(),
		time.Saturday.String(),
	}
}

// GuardrailViolations returns a description of each constraint of the WorkloadClassGuardrail that the
// WorkloadClass violates. It returns an empty slice if the WorkloadClass complies with the guardrail.
//
// Unset (zero-valued) optional guardrail constraints are not enforced. On the WorkloadClass:
//   - Omitting allowedDisruptionWindows allows disruption on every day of the week.
//   - A DisruptionWindow with an empty daysOfWeek is active on every day of the week.
//   - A maxNonDisruptionDurationDays of 0 means no limit, which exceeds any guardrail limit.
func GuardrailViolations(wlc *workloadsv1.WorkloadClass, g *workloadsv1.WorkloadClassGuardrail) []string {
	var (
		daysOfWeek                      = g.Spec.Constraints.Disruption.DaysOfWeek
		maxAllowedWindows               = g.Spec.Constraints.Disruption.MaxAllowedWindows
		maxNonDisruptionDurationDays    = g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays
		wlcMaxNonDisruptionDurationDays = wlc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays
		windows                         = wlc.Spec.DisruptionPolicy.AllowedDisruptionWindows
		violations                      = []string{}
	)

	if v := daysOfWeekViolation(daysOfWeek, windows); v != "" {
		violations = append(violations, v)
	}

	if v := allowedWindowsViolation(int(maxAllowedWindows), len(windows)); v != "" {
		violations = append(violations, v)
	}

	if v := disruptionDurationDaysViolation(wlcMaxNonDisruptionDurationDays, maxNonDisruptionDurationDays); v != "" {
		violations = append(violations, v)
	}

	return violations
}

// daysOfWeekViolation returns a description of how the WorkloadClass disruption windows violate the guardrail
// daysOfWeek, or an empty string if they comply. If several windows violate the guardrail, their descriptions
// are joined with "; ".
//
// An empty guardrail daysOfWeek does not restrict the days. Omitting the windows allows disruption on every day
// of the week, and a window with an empty daysOfWeek is active on every day of the week.
func daysOfWeekViolation(daysOfWeek []string, windows []workloadsv1.DisruptionWindow) string {
	if len(daysOfWeek) == 0 {
		return ""
	}

	if len(windows) == 0 {
		if IsSubset(AllWeekdays(), daysOfWeek) {
			return ""
		}
		return fmt.Sprintf(
			"`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only %s (guardrail limit); "+
				"omitting it allows disruption on every day",
			QuotedList(daysOfWeek))
	}

	var violations []string
	for _, dw := range windows {
		if len(dw.DaysOfWeek) == 0 {
			if !IsSubset(AllWeekdays(), daysOfWeek) {
				violations = append(violations, fmt.Sprintf(
					"`daysOfWeek` of disruption window '%s' must be specified and contain only %s (guardrail limit); "+
						"an empty `daysOfWeek` allows disruption on every day",
					dw.Name, QuotedList(daysOfWeek)))
			}
			continue
		}
		if !IsSubset(dw.DaysOfWeek, daysOfWeek) {
			violations = append(violations, fmt.Sprintf(
				"`daysOfWeek` of disruption window '%s' must contain only %s (guardrail limit), got %s",
				dw.Name, QuotedList(daysOfWeek), QuotedList(dw.DaysOfWeek)))
		}
	}
	return strings.Join(violations, "; ")
}

// allowedWindowsViolation returns a description of how the number of WorkloadClass disruption windows
// violates the guardrail maxAllowedWindows, or an empty string if it complies.
//
// A maxAllowedWindows of 0 means the guardrail does not limit the number of windows.
func allowedWindowsViolation(maxAllowedWindows, wlcWindows int) string {
	if maxAllowedWindows > 0 && wlcWindows > maxAllowedWindows {
		return fmt.Sprintf("`allowedDisruptionWindows` must have at most %d items (guardrail limit), got %d", maxAllowedWindows, wlcWindows)
	}
	return ""
}

// disruptionDurationDaysViolation returns a description of how the WorkloadClass maxNonDisruptionDurationDays
// violates the guardrail maxNonDisruptionDurationDays, or an empty string if it complies.
//
// A guardrail value of 0 means the guardrail does not set a limit. A WorkloadClass value of 0 (or less) means
// the WorkloadClass has no limit, which exceeds any guardrail limit.
func disruptionDurationDaysViolation(wlcMaxDays, guardrailMaxDays int32) string {
	if guardrailMaxDays > 0 &&
		(wlcMaxDays <= 0 || wlcMaxDays > guardrailMaxDays) {
		return fmt.Sprintf(
			"`maxNonDisruptionDurationDays` must be between 1 and %d, inclusive (guardrail limit), got %d (0 means no limit)",
			guardrailMaxDays, wlcMaxDays)
	}
	return ""
}
