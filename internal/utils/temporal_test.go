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

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestIsTimeInWindows(t *testing.T) {
	tests := []struct {
		name    string
		now     time.Time
		windows []workloadsv1.DisruptionWindow
		wantIn  bool
	}{
		{
			name: "In window same day",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59"},
			},
			wantIn: true,
		},
		{
			name: "Outside window same day",
			now:  time.Date(2026, 4, 20, 21, 0, 0, 0, time.UTC), // Monday 21:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59"},
			},
			wantIn: false,
		},
		{
			name: "Wraps around midnight - currently in (after start)",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn: true,
		},
		{
			name: "Wraps around midnight - currently in (before end)",
			now:  time.Date(2026, 4, 20, 02, 0, 0, 0, time.UTC), // Monday 02:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn: true,
		},
		{
			name: "Wraps around midnight - currently outside",
			now:  time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC), // Monday 10:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIn, _ := IsTimeInWindows(tt.now, tt.windows)
			if gotIn != tt.wantIn {
				t.Errorf("IsTimeInWindows() gotIn = %v, want %v", gotIn, tt.wantIn)
			}
		})
	}
}

func TestCalculateWindowDuration(t *testing.T) {
	tests := []struct {
		name    string
		window  workloadsv1.DisruptionWindow
		wantDur time.Duration
	}{
		{
			name:    "Standard window",
			window:  workloadsv1.DisruptionWindow{StartTime: "10:00", EndTime: "12:00"},
			wantDur: 2 * time.Hour,
		},
		{
			name:    "Wraparound window",
			window:  workloadsv1.DisruptionWindow{StartTime: "22:00", EndTime: "04:00"},
			wantDur: 6 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateWindowDuration(tt.window)
			if got != tt.wantDur {
				t.Errorf("CalculateWindowDuration() = %v, want %v", got, tt.wantDur)
			}
		})
	}
}
