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
	"slices"
	"strings"
	"testing"
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestAllWeekdays(t *testing.T) {
	want := []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
	got := AllWeekdays()
	if !slices.Equal(got, want) {
		t.Fatalf("AllWeekdays() = %v, want %v", got, want)
	}

	for d := time.Sunday; d <= time.Saturday; d++ {
		if got[d] != d.String() {
			t.Errorf("AllWeekdays()[%d] = %q, want time.Weekday.String() %q", d, got[d], d.String())
		}
	}

	// Each call must return a new slice so callers cannot modify the result of later calls.
	got[0] = "Funday"
	if AllWeekdays()[0] != "Sunday" {
		t.Errorf("AllWeekdays() returned a shared slice: modifying one result changed the next")
	}
}

func TestAllowedWindowsViolation(t *testing.T) {
	tests := []struct {
		name                 string
		maxAllowedWindows    int
		workloadClassWindows int
		want                 string
	}{
		{
			name:                 "guardrail does not limit windows",
			maxAllowedWindows:    0,
			workloadClassWindows: 10,
		},
		{
			name:                 "no windows",
			maxAllowedWindows:    2,
			workloadClassWindows: 0,
		},
		{
			name:                 "below limit",
			maxAllowedWindows:    2,
			workloadClassWindows: 1,
		},
		{
			name:                 "at limit",
			maxAllowedWindows:    2,
			workloadClassWindows: 2,
		},
		{
			name:                 "exceeds limit",
			maxAllowedWindows:    2,
			workloadClassWindows: 3,
			want:                 "`allowedDisruptionWindows` must have at most 2 items (guardrail limit), got 3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := allowedWindowsViolation(tt.maxAllowedWindows, tt.workloadClassWindows); got != tt.want {
				t.Errorf("allowedWindowsViolation(%d, %d) = %q, want %q", tt.maxAllowedWindows, tt.workloadClassWindows, got, tt.want)
			}
		})
	}
}

func TestDisruptionDurationDaysViolation(t *testing.T) {
	tests := []struct {
		name          string
		workloadDays  int32
		guardrailDays int32
		want          string
	}{
		{
			name:          "guardrail does not set a limit",
			workloadDays:  100,
			guardrailDays: 0,
		},
		{
			name:          "neither sets a limit",
			workloadDays:  0,
			guardrailDays: 0,
		},
		{
			name:          "below limit",
			workloadDays:  7,
			guardrailDays: 30,
		},
		{
			name:          "at limit",
			workloadDays:  30,
			guardrailDays: 30,
		},
		{
			name:          "exceeds limit",
			workloadDays:  31,
			guardrailDays: 30,
			want:          "`maxNonDisruptionDurationDays` must be between 1 and 30, inclusive (guardrail limit), got 31 (0 means no limit)",
		},
		{
			name:          "workload class has no limit",
			workloadDays:  0,
			guardrailDays: 30,
			want:          "`maxNonDisruptionDurationDays` must be between 1 and 30, inclusive (guardrail limit), got 0 (0 means no limit)",
		},
		{
			name:          "negative workload class value is treated as no limit",
			workloadDays:  -1,
			guardrailDays: 30,
			want:          "`maxNonDisruptionDurationDays` must be between 1 and 30, inclusive (guardrail limit), got -1 (0 means no limit)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := disruptionDurationDaysViolation(tt.workloadDays, tt.guardrailDays); got != tt.want {
				t.Errorf("disruptionDurationDaysViolation(%d, %d) = %q, want %q", tt.workloadDays, tt.guardrailDays, got, tt.want)
			}
		})
	}
}

func TestDaysOfWeekViolation(t *testing.T) {
	window := func(name string, days ...string) workloadsv1.DisruptionWindow {
		return workloadsv1.DisruptionWindow{Name: name, DaysOfWeek: days}
	}
	weekend := []string{"Saturday", "Sunday"}

	tests := []struct {
		name       string
		daysOfWeek []string
		windows    []workloadsv1.DisruptionWindow
		want       string
	}{
		{
			name:    "guardrail does not restrict days",
			windows: []workloadsv1.DisruptionWindow{window("w", "Monday")},
		},
		{
			name:       "windows within allowed days",
			daysOfWeek: weekend,
			windows:    []workloadsv1.DisruptionWindow{window("sat", "Saturday"), window("sun", "Sunday")},
		},
		{
			name:       "omitted windows",
			daysOfWeek: weekend,
			want: "`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only 'Saturday', 'Sunday' " +
				"(guardrail limit); omitting it allows disruption on every day",
		},
		{
			name:       "omitted windows with every day allowed",
			daysOfWeek: AllWeekdays(),
		},
		{
			name:       "window with empty daysOfWeek",
			daysOfWeek: weekend,
			windows:    []workloadsv1.DisruptionWindow{window("w")},
			want: "`daysOfWeek` of disruption window 'w' must be specified and contain only 'Saturday', 'Sunday' " +
				"(guardrail limit); an empty `daysOfWeek` allows disruption on every day",
		},
		{
			name:       "window with empty daysOfWeek and every day allowed",
			daysOfWeek: AllWeekdays(),
			windows:    []workloadsv1.DisruptionWindow{window("w")},
		},
		{
			name:       "multiple violating windows are joined",
			daysOfWeek: weekend,
			windows: []workloadsv1.DisruptionWindow{
				window("mon", "Monday"), window("sat", "Saturday"), window("fri", "Friday"),
			},
			want: "`daysOfWeek` of disruption window 'mon' must contain only 'Saturday', 'Sunday' (guardrail limit), got 'Monday'; " +
				"`daysOfWeek` of disruption window 'fri' must contain only 'Saturday', 'Sunday' (guardrail limit), got 'Friday'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := daysOfWeekViolation(tt.daysOfWeek, tt.windows); got != tt.want {
				t.Errorf("daysOfWeekViolation() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGuardrailViolations(t *testing.T) {
	window := func(name string, days ...string) workloadsv1.DisruptionWindow {
		return workloadsv1.DisruptionWindow{Name: name, DaysOfWeek: days, StartTime: "22:00", EndTime: "04:00"}
	}
	guardrail := func(d workloadsv1.Disruption) *workloadsv1.WorkloadClassGuardrail {
		return &workloadsv1.WorkloadClassGuardrail{
			Spec: workloadsv1.WorkloadClassGuardrailSpec{
				Constraints: workloadsv1.Constraints{Disruption: d},
			},
		}
	}
	weekendOnly := workloadsv1.Disruption{
		DaysOfWeek:                   []string{"Saturday", "Sunday"},
		MaxAllowedWindows:            1,
		MaxNonDisruptionDurationDays: 30,
	}

	tests := []struct {
		name           string
		policy         workloadsv1.DisruptionPolicy
		guardrail      workloadsv1.Disruption
		wantViolations []string
	}{
		{
			name: "compliant",
			policy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows:     []workloadsv1.DisruptionWindow{window("w", "Saturday")},
				MaxNonDisruptionDurationDays: 7,
			},
			guardrail: weekendOnly,
		},
		{
			name: "window day not allowed",
			policy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows:     []workloadsv1.DisruptionWindow{window("w", "Saturday", "Monday")},
				MaxNonDisruptionDurationDays: 7,
			},
			guardrail:      weekendOnly,
			wantViolations: []string{"`daysOfWeek` of disruption window 'w' must contain only 'Saturday', 'Sunday' (guardrail limit), got 'Saturday', 'Monday'"},
		},
		{
			name: "window with empty daysOfWeek is active every day",
			policy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows:     []workloadsv1.DisruptionWindow{window("w")},
				MaxNonDisruptionDurationDays: 7,
			},
			guardrail:      weekendOnly,
			wantViolations: []string{"an empty `daysOfWeek` allows disruption on every day"},
		},
		{
			name:           "omitted windows allow disruption every day",
			policy:         workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 7},
			guardrail:      weekendOnly,
			wantViolations: []string{"`allowedDisruptionWindows` must be specified"},
		},
		{
			name:      "omitted windows allowed when guardrail allows every day",
			policy:    workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 7},
			guardrail: workloadsv1.Disruption{DaysOfWeek: AllWeekdays(), MaxNonDisruptionDurationDays: 30},
		},
		{
			name: "too many windows",
			policy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{
					window("sat", "Saturday"), window("sun", "Sunday"),
				},
				MaxNonDisruptionDurationDays: 7,
			},
			guardrail:      weekendOnly,
			wantViolations: []string{"`allowedDisruptionWindows` must have at most 1 items (guardrail limit), got 2"},
		},
		{
			name: "maxNonDisruptionDurationDays exceeds limit",
			policy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows:     []workloadsv1.DisruptionWindow{window("w", "Saturday")},
				MaxNonDisruptionDurationDays: 31,
			},
			guardrail:      weekendOnly,
			wantViolations: []string{"must be between 1 and 30, inclusive (guardrail limit), got 31"},
		},
		{
			name: "unset maxNonDisruptionDurationDays means no limit",
			policy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{window("w", "Saturday")},
			},
			guardrail:      weekendOnly,
			wantViolations: []string{"got 0 (0 means no limit)"},
		},
		{
			name: "unset optional guardrail constraints are not enforced",
			policy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{
					window("a"), window("b"), window("c"),
				},
				MaxNonDisruptionDurationDays: 7,
			},
			guardrail: workloadsv1.Disruption{MaxNonDisruptionDurationDays: 30},
		},
		{
			name:   "multiple violations",
			policy: workloadsv1.DisruptionPolicy{},
			guardrail: workloadsv1.Disruption{
				DaysOfWeek:                   []string{"Saturday"},
				MaxNonDisruptionDurationDays: 30,
			},
			wantViolations: []string{"`allowedDisruptionWindows` must be specified", "0 means no limit"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wc := &workloadsv1.WorkloadClass{Spec: workloadsv1.WorkloadClassSpec{DisruptionPolicy: tt.policy}}
			got := GuardrailViolations(wc, guardrail(tt.guardrail))
			if len(got) != len(tt.wantViolations) {
				t.Fatalf("GuardrailViolations() = %q, want %d violation(s) containing %q", got, len(tt.wantViolations), tt.wantViolations)
			}
			for i, want := range tt.wantViolations {
				if !strings.Contains(got[i], want) {
					t.Errorf("violation[%d] = %q, want it to contain %q", i, got[i], want)
				}
			}
		})
	}
}
