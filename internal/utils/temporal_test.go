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

func TestWeekdaysValid(t *testing.T) {
	tests := []struct {
		name    string
		days    []string
		wantErr bool
	}{
		{
			name:    "empty days slice is valid",
			days:    nil,
			wantErr: false,
		},
		{
			name:    "valid weekdays",
			days:    []string{"Sunday", "Monday", "Saturday"},
			wantErr: false,
		},
		{
			name:    "invalid weekday name",
			days:    []string{"Monday", "Funday"},
			wantErr: true,
		},
		{
			name:    "lowercase weekday is rejected",
			days:    []string{"monday"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := WeekdaysValid(tt.days)
			if (err != nil) != tt.wantErr {
				t.Errorf("WeekdaysValid(%v) error = %v, wantErr %v", tt.days, err, tt.wantErr)
			}
		})
	}
}

func TestWeekdaysValidMessage(t *testing.T) {
	err := WeekdaysValid([]string{"Monday", "Funday", "monday"})
	if err == nil {
		t.Fatal("WeekdaysValid() error = nil, want an error")
	}
	want := "`daysOfWeek` must only contain 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', " +
		"got 'Funday', 'monday'"
	if err.Error() != want {
		t.Errorf("WeekdaysValid() error = %q, want %q", err.Error(), want)
	}
}

func TestTimeZoneValid(t *testing.T) {
	tests := []struct {
		name     string
		timeZone string
		want     bool
	}{
		{
			name:     "UTC is valid",
			timeZone: "UTC",
			want:     true,
		},
		{
			name:     "single-segment IANA zone EST is valid",
			timeZone: "EST",
			want:     true,
		},
		{
			name:     "single-segment IANA zone CET is valid",
			timeZone: "CET",
			want:     true,
		},
		{
			name:     "IANA region/city is valid",
			timeZone: "America/Los_Angeles",
			want:     true,
		},
		{
			name:     "three-segment IANA zone is valid",
			timeZone: "America/Argentina/Buenos_Aires",
			want:     true,
		},
		{
			name:     "invalid timezone returns false",
			timeZone: "Invalid/TimeZone",
			want:     false,
		},
		{
			name:     "Local timezone is rejected",
			timeZone: "Local",
			want:     false,
		},
		{
			name:     "lowercase local timezone is rejected",
			timeZone: "local",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TimeZoneValid(tt.timeZone); got != tt.want {
				t.Errorf("TimeZoneValid(%q) = %v, want %v", tt.timeZone, got, tt.want)
			}
		})
	}
}

func TestIsTimeInWindows(t *testing.T) {
	tests := []struct {
		name    string
		now     time.Time
		windows []workloadsv1.DisruptionWindow
		wantIn  bool
	}{
		{
			name:    "No windows configured defaults to in window",
			now:     time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC),
			windows: nil,
			wantIn:  true,
		},
		{
			name: "Empty daysOfWeek on a window defaults to every day of the week",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1.DisruptionWindow{
				{StartTime: "22:00", EndTime: "23:59"},
			},
			wantIn: true,
		},
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
			name: "In window same day, UTC and local dates differ (Tuesday UTC is Monday in America/Los_Angeles)",
			now:  time.Date(2026, 4, 21, 6, 0, 0, 0, time.UTC), // Tuesday 06:00 UTC == Monday 23:00 PDT
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59", TimeZone: "America/Los_Angeles"},
			},
			wantIn: true,
		},
		{
			name: "Outside window same day, UTC and local dates differ (Monday UTC is Tuesday in Asia/Manila)",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00 UTC == Tuesday 07:00 Manila
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "06:00", EndTime: "08:00", TimeZone: "Asia/Manila"},
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
			now:  time.Date(2026, 4, 20, 15, 0, 0, 0, time.UTC), // Monday 15:00 UTC == Monday 23:00 Manila
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00", TimeZone: "Asia/Manila"},
			},
			wantIn: true,
		},
		{
			name: "Wraps around midnight - currently in (before end on next day)",
			now:  time.Date(2026, 4, 21, 2, 0, 0, 0, time.UTC), // Tuesday 02:00 UTC (spillover from Monday night)
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn: true,
		},
		{
			name: "Wraps around midnight - currently outside (before end on start day itself)",
			now:  time.Date(2026, 4, 20, 2, 0, 0, 0, time.UTC), // Monday 02:00 UTC (Sunday night was not in DaysOfWeek)
			windows: []workloadsv1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn: false,
		},
		{
			name: "Wraps around midnight - currently in (before end on next day), different time zone",
			now:  time.Date(2026, 4, 21, 8, 0, 0, 0, time.UTC), // Tuesday 08:00 UTC == Tuesday 02:00 Guatemala (spillover from Monday night)
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
