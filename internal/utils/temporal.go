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
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// IsTimeInWindows checks if the given time is within any of the defined disruption windows.
// Returns true if in window, and the duration until the next state change (window opens or closes).
func IsTimeInWindows(now time.Time, windows []workloadsv1.DisruptionWindow) (bool, time.Duration) {
	if len(windows) == 0 {
		return true, 0
	}

	inWindow := false
	minWait := 24 * 7 * time.Hour

	for _, w := range windows {
		if slices.Contains(w.DaysOfWeek, now.Weekday().String()) {
			start, err := time.Parse("15:04", w.StartTime)
			if err != nil {
				continue
			}
			end, err := time.Parse("15:04", w.EndTime)
			if err != nil {
				continue
			}

			todayStart := time.Date(now.Year(), now.Month(), now.Day(), start.Hour(), start.Minute(), 0, 0, time.UTC)
			todayEnd := time.Date(now.Year(), now.Month(), now.Day(), end.Hour(), end.Minute(), 0, 0, time.UTC)

			// Handle windows that wrap around midnight (e.g. 22:00 to 04:00)
			if todayEnd.Before(todayStart) {
				// If now is after start, or now is before end, we're in the window
				if now.After(todayStart) {
					inWindow = true
					wait := todayEnd.Add(24 * time.Hour).Sub(now)
					if wait < minWait {
						minWait = wait
					}
				} else if now.Before(todayEnd) {
					inWindow = true
					wait := todayEnd.Sub(now)
					if wait < minWait {
						minWait = wait
					}
				} else {
					// Outside, wait for start
					wait := todayStart.Sub(now)
					if wait < minWait {
						minWait = wait
					}
				}
			} else {
				if now.After(todayStart) && now.Before(todayEnd) {
					inWindow = true
					wait := todayEnd.Sub(now)
					if wait < minWait {
						minWait = wait
					}
				} else if now.Before(todayStart) {
					wait := todayStart.Sub(now)
					if wait < minWait {
						minWait = wait
					}
				}
			}
		}
	}

	return inWindow, minWait
}

// CalculateWindowDuration calculates the duration of a window.
func CalculateWindowDuration(w workloadsv1.DisruptionWindow) time.Duration {
	start, err := time.Parse("15:04", w.StartTime)
	if err != nil {
		return 0
	}
	end, err := time.Parse("15:04", w.EndTime)
	if err != nil {
		return 0
	}
	if end.Before(start) {
		// Assume wraps around midnight
		return end.Add(24 * time.Hour).Sub(start)
	}
	return end.Sub(start)
}
