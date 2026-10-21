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

import "testing"

func TestParsePercentage(t *testing.T) {
	testCases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{in: "0%", want: 0},
		{in: "80%", want: 80},
		{in: "100%", want: 100},
		{in: "80", wantErr: true},
		{in: "101%", wantErr: true},
		{in: "-1%", wantErr: true},
		{in: "abc%", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParsePercentage(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParsePercentage(%q) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("ParsePercentage(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestSpotRatioPercent(t *testing.T) {
	if got, err := SpotRatioPercent(""); err != nil || got != 100 {
		t.Errorf(`SpotRatioPercent("") = %d, %v; want 100, nil`, got, err)
	}
	if got, err := SpotRatioPercent("25%"); err != nil || got != 25 {
		t.Errorf(`SpotRatioPercent("25%%") = %d, %v; want 25, nil`, got, err)
	}
	if _, err := SpotRatioPercent("abc"); err == nil {
		t.Errorf(`SpotRatioPercent("abc") expected an error`)
	}
}

func TestShouldUseSpot(t *testing.T) {
	testCases := []struct {
		name                             string
		spotExisting, totalExisting, pct int
		want                             bool
	}{
		{name: "first Pod at 80% goes to Spot", spotExisting: 0, totalExisting: 0, pct: 80, want: true},
		{name: "second Pod at 80% goes to Spot", spotExisting: 1, totalExisting: 1, pct: 80, want: true},
		{name: "fifth Pod at 80% with 4 on Spot goes to On-Demand", spotExisting: 4, totalExisting: 4, pct: 80, want: false},
		{name: "sixth Pod at 80% with 4 of 5 on Spot goes to Spot", spotExisting: 4, totalExisting: 5, pct: 80, want: true},
		{name: "0% never uses Spot", spotExisting: 0, totalExisting: 0, pct: 0, want: false},
		{name: "100% always uses Spot", spotExisting: 9, totalExisting: 9, pct: 100, want: true},
		{name: "1 of 2 Spot at 50% -> next Pod to Spot", spotExisting: 1, totalExisting: 2, pct: 50, want: true},
		{name: "2 of 3 Spot at 50% -> next Pod to On-Demand", spotExisting: 2, totalExisting: 3, pct: 50, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldUseSpot(tc.spotExisting, tc.totalExisting, tc.pct); got != tc.want {
				t.Errorf("ShouldUseSpot(%d, %d, %d) = %v, want %v", tc.spotExisting, tc.totalExisting, tc.pct, got, tc.want)
			}
		})
	}
}

// TestShouldUseSpotSequence places Pods one at a time and checks the resulting Spot share lands on or just
// above the target, never below it.
func TestShouldUseSpotSequence(t *testing.T) {
	for _, pct := range []int{0, 25, 50, 80, 100} {
		spot := 0
		for total := range 20 {
			if ShouldUseSpot(spot, total, pct) {
				spot++
			}
			n := total + 1
			if spot*100 < n*pct {
				t.Errorf("pct=%d after %d Pods: %d on Spot is below target", pct, n, spot)
			}
			// At most one Pod above the ceiling of the target.
			if spot > (n*pct+99)/100 {
				t.Errorf("pct=%d after %d Pods: %d on Spot overshoots target", pct, n, spot)
			}
		}
	}
}

func TestSpotShareMet(t *testing.T) {
	testCases := []struct {
		name             string
		spot, total, pct int
		want             bool
	}{
		{name: "no Pods", spot: 0, total: 0, pct: 80, want: true},
		{name: "3 of 5 at 80% is not met", spot: 3, total: 5, pct: 80, want: false},
		{name: "4 of 5 at 80% is met", spot: 4, total: 5, pct: 80, want: true},
		{name: "1 of 2 at 50% is met", spot: 1, total: 2, pct: 50, want: true},
		{name: "0 of 2 at 50% is not met", spot: 0, total: 2, pct: 50, want: false},
		{name: "anything at 0% is met", spot: 0, total: 7, pct: 0, want: true},
		{name: "all but one at 100% is not met", spot: 6, total: 7, pct: 100, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SpotShareMet(tc.spot, tc.total, tc.pct); got != tc.want {
				t.Errorf("SpotShareMet(%d, %d, %d) = %v, want %v", tc.spot, tc.total, tc.pct, got, tc.want)
			}
		})
	}
}

// TestAdmissionAndReversionAgree checks that reversion stops exactly where admission would stop adding Spot
// Pods: for any workload size and ratio, SpotShareMet is true iff admission would not place one more Pod on
// Spot given the same total.
func TestAdmissionAndReversionAgree(t *testing.T) {
	for pct := 0; pct <= 100; pct += 10 {
		for total := 1; total <= 12; total++ {
			for spot := 0; spot <= total; spot++ {
				admissionWouldAddSpot := ShouldUseSpot(spot, total-1, pct)
				if SpotShareMet(spot, total, pct) == admissionWouldAddSpot {
					t.Errorf("pct=%d total=%d spot=%d: SpotShareMet=%v but admission would add Spot=%v",
						pct, total, spot, SpotShareMet(spot, total, pct), admissionWouldAddSpot)
				}
			}
		}
	}
}
