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
			name: "In window same day, start equals end",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "12:00", EndTime: "12:00"},
			},
			wantIn: true,
		},
		{
			name: "In window same day, different time zone",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "16:00", EndTime: "17:59", TimeZone: "America/Guatemala"},
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
			name: "Outside window same day, different time zone",
			now:  time.Date(2026, 4, 20, 21, 0, 0, 0, time.UTC), // Monday 21:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59", TimeZone: "Asia/Manila"},
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
			name: "Wraps around midnight - currently in (after start), different time zone",
			now:  time.Date(2026, 4, 20, 15, 0, 0, 0, time.UTC), // Monday 15:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00", TimeZone: "Asia/Manila"},
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
			name: "Wraps around midnight - currently in (before end), different time zone",
			now:  time.Date(2026, 4, 20, 8, 0, 0, 0, time.UTC), // Monday 8:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00", TimeZone: "America/Guatemala"},
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
		{
			name: "Wraps around midnight - currently outside, different time zone",
			now:  time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC), // Monday 10:00
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00", TimeZone: "America/Toronto"},
			},
			wantIn: false,
		},
	}

	ctx := t.Context()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIn, _ := IsTimeInWindows(ctx, tt.now, tt.windows)
			if gotIn != tt.wantIn {
				t.Errorf("IsTimeInWindows() gotIn = %v, want %v", gotIn, tt.wantIn)
			}
		})
	}
}
