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

package main

import (
	"testing"

	"k8s.io/client-go/rest"
)

func TestApplyRestConfigRateLimits(t *testing.T) {
	testCases := []struct {
		name      string
		initial   *rest.Config
		qps       float64
		burst     int
		wantQPS   float32
		wantBurst int
	}{
		{
			name:      "defaults_preserve_controller_runtime_unlimited_config",
			initial:   &rest.Config{QPS: -1, Burst: -1},
			qps:       defaultKubeAPIQPS,
			burst:     defaultKubeAPIBurst,
			wantQPS:   -1,
			wantBurst: -1,
		},
		{
			name:      "positive_values_override_unlimited_config",
			initial:   &rest.Config{QPS: -1, Burst: -1},
			qps:       75.5,
			burst:     150,
			wantQPS:   75.5,
			wantBurst: 150,
		},
		{
			name:      "zero_preserves_existing_positive_config",
			initial:   &rest.Config{QPS: 20, Burst: 30},
			qps:       0,
			burst:     0,
			wantQPS:   20,
			wantBurst: 30,
		},
		{
			name:      "negative_values_passed_through_to_disable_limiter",
			initial:   &rest.Config{QPS: 20, Burst: 30},
			qps:       -1,
			burst:     -1,
			wantQPS:   -1,
			wantBurst: -1,
		},
	}

	// Nil config should be a no-op.
	applyRestConfigRateLimits(nil, 50, 100)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			applyRestConfigRateLimits(tc.initial, tc.qps, tc.burst)
			if tc.initial.QPS != tc.wantQPS {
				t.Errorf("QPS = %v, want %v", tc.initial.QPS, tc.wantQPS)
			}
			if tc.initial.Burst != tc.wantBurst {
				t.Errorf("Burst = %v, want %v", tc.initial.Burst, tc.wantBurst)
			}
		})
	}
}
