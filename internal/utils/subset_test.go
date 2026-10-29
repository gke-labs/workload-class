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

func TestIsSubset(t *testing.T) {
	tests := []struct {
		name     string
		subset   []string
		superset []string
		want     bool
	}{
		{
			name:     "empty subset is always a subset",
			subset:   nil,
			superset: []string{"Monday", "Tuesday"},
			want:     true,
		},
		{
			name:     "empty subset and empty superset",
			subset:   []string{},
			superset: []string{},
			want:     true,
		},
		{
			name:     "proper subset regardless of order",
			subset:   []string{"Wednesday", "Monday"},
			superset: []string{"Monday", "Tuesday", "Wednesday"},
			want:     true,
		},
		{
			name:     "subset with duplicate elements",
			subset:   []string{"Monday", "Monday"},
			superset: []string{"Monday", "Tuesday"},
			want:     true,
		},
		{
			name:     "element missing from superset",
			subset:   []string{"Monday", "Friday"},
			superset: []string{"Monday", "Tuesday"},
			want:     false,
		},
		{
			name:     "non-empty subset with empty superset",
			subset:   []string{"Monday"},
			superset: nil,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSubset(tt.subset, tt.superset); got != tt.want {
				t.Errorf("IsSubset(%v, %v) = %v, want %v", tt.subset, tt.superset, got, tt.want)
			}
		})
	}
}
