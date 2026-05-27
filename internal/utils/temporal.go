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
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// IsTimeInWindows checks if the given time is within any of the defined disruption windows.
// Returns true if in window, and the duration until the next state change (window opens or closes).
func IsTimeInWindows(ctx context.Context, nowUTC time.Time, windows []workloadsv1.DisruptionWindow) (bool, time.Duration) {
	log := logf.FromContext(ctx)
	if len(windows) == 0 {
		log.Info("WorkoadClass does not contain AllowedDisruptionWindows")
		return true, 0
	}

	inWindow := false
	minWait := 24 * 7 * time.Hour

	for _, w := range windows {
		if slices.Contains(w.DaysOfWeek, nowUTC.Weekday().String()) {
			start, end, err := windowInfo(nowUTC, w)
			if err != nil {
				log.Error(err, fmt.Sprintf("Error getting start and end times from DisruptionWindow %s", w.Name))
				continue
			}

			inCurrentWindow, waitForCurrentWindow := evaluateWindow(start, end, nowUTC)

			inWindow = inWindow || inCurrentWindow
			minWait = min(minWait, waitForCurrentWindow)
		}
	}

	return inWindow, minWait
}

// windowInfo calculates the DisruptionWindow's start and end times in UTC for the current day.
// It anchors the window's time to the date in the specified TimeZone before converting back to UTC.
// On error, it returns (nowUTC, nowUTC, err).
func windowInfo(nowUTC time.Time, w workloadsv1.DisruptionWindow) (start, end time.Time, err error) {
	location := "Etc/UTC"
	if w.TimeZone != "" {
		location = w.TimeZone
	}

	timeZone, timeZoneErr := time.LoadLocation(location)
	if timeZoneErr != nil {
		err = errors.Join(err, timeZoneErr)
	}

	start, startErr := time.Parse("15:04", w.StartTime)
	if startErr != nil {
		err = errors.Join(err, startErr)
	}

	end, endErr := time.Parse("15:04", w.EndTime)
	if endErr != nil {
		err = errors.Join(err, endErr)
	}

	if err != nil {
		return nowUTC, nowUTC, err
	}

	localTime := nowUTC.In(timeZone)
	localStart := time.Date(localTime.Year(), localTime.Month(), localTime.Day(), start.Hour(), start.Minute(), 0, 0, timeZone)
	localEnd := time.Date(localTime.Year(), localTime.Month(), localTime.Day(), end.Hour(), end.Minute(), 0, 0, timeZone)

	startUTC := localStart.UTC()
	endUTC := localEnd.UTC()

	return startUTC, endUTC, nil
}

func evaluateWindow(start, end, nowUTC time.Time) (bool, time.Duration) {
	// Handle windows that wrap around midnight (e.g. 22:00 to 04:00)
	if end.Before(start) {
		return evaluateCrossMidnightWindow(start, end, nowUTC)
	}

	return evaluateSameDayWindow(start, end, nowUTC)
}

func evaluateCrossMidnightWindow(start, end, now time.Time) (bool, time.Duration) {
	// If now is after start, or now is before end, we're in the window
	if now.After(start) {
		return true, end.Add(24 * time.Hour).Sub(now)
	}
	if now.Before(end) {
		return true, end.Sub(now)
	}

	// Outside, wait for start
	return false, start.Sub(now)
}

func evaluateSameDayWindow(start, end, now time.Time) (bool, time.Duration) {
	minWait := 24 * 7 * time.Hour

	if now.After(start) && now.Before(end) {
		return true, end.Sub(now)
	}
	if now.Before(start) {
		minWait = min(minWait, start.Sub(now))
	}

	return false, minWait
}
