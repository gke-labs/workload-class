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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SpotPlacementConfigSpec defines the desired state of SpotPlacementConfig
type SpotPlacementConfigSpec struct {
	SpotRatioPercent int32 `json:"spotRatioPercent"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced

// SpotPlacementConfig is the Schema for the spotplacementconfigs API
type SpotPlacementConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec SpotPlacementConfigSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// SpotPlacementConfigList contains a list of SpotPlacementConfig
type SpotPlacementConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpotPlacementConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SpotPlacementConfig{}, &SpotPlacementConfigList{})
}
