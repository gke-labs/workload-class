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

import workloadsv1 "github.com/gke-labs/workload-class/api/v1"

// ReferencesGKESpotPlacementPolicy reports whether the InfrastructureProfileReference points at a
// GKESpotPlacementPolicy. An empty Group is treated as the workloads.gke.io group.
func ReferencesGKESpotPlacementPolicy(ref *workloadsv1.InfrastructureProfileReference) bool {
	return ref != nil &&
		ref.Kind == workloadsv1.GKESpotPlacementPolicyKind &&
		(ref.Group == "" || ref.Group == workloadsv1.GroupVersion.Group)
}

// TargetsSpot reports whether the WorkloadClass places any Pods on Spot capacity: it has a CapacityStrategy
// whose Type is Spot (or unset, which defaults to Spot) and whose SpotRatio is above 0%.
func TargetsSpot(wc *workloadsv1.WorkloadClass) bool {
	if wc == nil || wc.Spec.CapacityStrategy == nil {
		return false
	}
	cs := wc.Spec.CapacityStrategy
	if cs.Type != "" && cs.Type != workloadsv1.SpotPlacementTypeSpot {
		return false
	}
	ratio, err := SpotRatioPercent(cs.SpotRatio)
	return err == nil && ratio > 0
}

// IsGKESpotManaged reports whether the gke-spot-placement plugin is responsible for the WorkloadClass's Spot
// placement. That is the case when the WorkloadClass has a CapacityStrategy and either references a
// GKESpotPlacementPolicy or has no InfrastructureProfileRef at all (in which case the CapacityStrategy is
// applied with GKE defaults). WorkloadClasses referencing another profile kind belong to that profile's plugin.
func IsGKESpotManaged(wc *workloadsv1.WorkloadClass) bool {
	if wc == nil || wc.Spec.CapacityStrategy == nil {
		return false
	}
	ref := wc.Spec.InfrastructureProfileRef
	return ref == nil || ReferencesGKESpotPlacementPolicy(ref)
}
