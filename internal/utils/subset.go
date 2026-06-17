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

// IsSubset checks if all elements in the `subset` slice are present in the `superset` slice.
// It treats the slices as mathematical sets, ignoring the order and duplicate values.
func IsSubset(subset, superset []string) bool {
	if len(subset) == 0 {
		return true
	}

	supersetMap := make(map[string]struct{}, len(superset))
	for _, d := range superset {
		supersetMap[d] = struct{}{}
	}

	for _, d := range subset {
		if _, found := supersetMap[d]; !found {
			return false
		}
	}

	return true
}
