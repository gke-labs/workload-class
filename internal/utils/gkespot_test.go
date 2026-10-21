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

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func gkeRef(group string) *workloadsv1.InfrastructureProfileReference {
	return &workloadsv1.InfrastructureProfileReference{Group: group, Kind: workloadsv1.GKESpotPlacementPolicyKind, Name: "p"}
}

func TestReferencesGKESpotPlacementPolicy(t *testing.T) {
	testCases := []struct {
		name string
		ref  *workloadsv1.InfrastructureProfileReference
		want bool
	}{
		{name: "nil", ref: nil, want: false},
		{name: "GKE policy with group", ref: gkeRef(workloadsv1.GroupVersion.Group), want: true},
		{name: "GKE policy without group", ref: gkeRef(""), want: true},
		{name: "GKE policy kind in another group", ref: gkeRef("other.example.io"), want: false},
		{name: "other kind", ref: &workloadsv1.InfrastructureProfileReference{Kind: "KarpenterSpotPolicy", Name: "p"}, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ReferencesGKESpotPlacementPolicy(tc.ref); got != tc.want {
				t.Errorf("ReferencesGKESpotPlacementPolicy() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTargetsSpotAndIsGKESpotManaged(t *testing.T) {
	if TargetsSpot(nil) {
		t.Errorf("TargetsSpot(nil) = true, want false")
	}
	if IsGKESpotManaged(nil) {
		t.Errorf("IsGKESpotManaged(nil) = true, want false")
	}

	testCases := []struct {
		name        string
		cs          *workloadsv1.CapacityStrategy
		ref         *workloadsv1.InfrastructureProfileReference
		wantSpot    bool
		wantManaged bool
	}{
		{name: "no CapacityStrategy", wantSpot: false, wantManaged: false},
		{name: "default type, no ref", cs: &workloadsv1.CapacityStrategy{}, wantSpot: true, wantManaged: true},
		{name: "Spot with GKE ref", cs: &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeSpot}, ref: gkeRef(""), wantSpot: true, wantManaged: true},
		{name: "Spot with 0% ratio", cs: &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeSpot, SpotRatio: "0%"}, ref: gkeRef(""), wantSpot: false, wantManaged: true},
		{name: "OnDemand with GKE ref", cs: &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand}, ref: gkeRef(""), wantSpot: false, wantManaged: true},
		{name: "Spot with other profile kind", cs: &workloadsv1.CapacityStrategy{}, ref: &workloadsv1.InfrastructureProfileReference{Kind: "KarpenterSpotPolicy", Name: "p"}, wantSpot: true, wantManaged: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := &workloadsv1.WorkloadClass{}
			wc.Spec.CapacityStrategy = tc.cs
			wc.Spec.InfrastructureProfileRef = tc.ref
			if got := TargetsSpot(wc); got != tc.wantSpot {
				t.Errorf("TargetsSpot() = %v, want %v", got, tc.wantSpot)
			}
			if got := IsGKESpotManaged(wc); got != tc.wantManaged {
				t.Errorf("IsGKESpotManaged() = %v, want %v", got, tc.wantManaged)
			}
		})
	}
}
