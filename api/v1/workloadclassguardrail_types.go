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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Constraints defines the guardrails for WorkloadClasses.
type Constraints struct {
	// Disruption defines the constraints within which WorkloadClasses can set disruption policies.
	Disruption Disruption `json:"disruption"`
}

// Disruption defines the constraints within which WorkloadClasses can set disruption policies.
type Disruption struct {
	// AllowedDisruptionDays specifies days on which disruption can happen (Monday-Sunday).
	// +optional
	AllowedDisruptionDays []string `json:"allowedDisruptionDays,omitempty"`

	// MaxAllowedWindows sets the limit of how many windows workload owners can set.
	// This avoid complications where windows are too short or cases where there could be too many disruptions to workloads.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxAllowedWindows int32 `json:"maxAllowedWindows,omitempty"`

	// MaxNonDisruptionDurationDays is the limit for how long workload owners can go without having an open maintenance window.
	// +kubebuilder:validation:Minimum=1
	MaxNonDisruptionDurationDays int32 `json:"maxNonDisruptionDurationDays"`

	// EnforcedDisruptionTimeoutSeconds is the maximum time in seconds before a disruption is forced.
	// +optional
	// +kubebuilder:validation:Maximum=3600
	EnforcedDisruptionTimeoutSeconds int32 `json:"enforcedDisruptionTimeoutSeconds,omitempty"`

	// EmergencyOverride allows bypassing all constraints immediately.
	// +optional
	EmergencyOverride bool `json:"emergencyOverride,omitempty"`
}

// PluginConstraints defines guardrail constraints grouped by plugin optimization intent.
type PluginConstraints struct {
	// Placement holds the list of plugin constraints for workload placement.
	// +optional
	// +listType=map
	// +listMapKey=pluginName
	Placement []PluginConstraint `json:"placement,omitempty"`
}

// PluginConstraint defines constraints for a specific plugin controller.
type PluginConstraint struct {
	// PluginName identifies the target plugin controller (e.g., "gke-spot-placement").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	PluginName string `json:"pluginName"`

	// AllowedConfigTemplate optionally specifies a policy template name (e.g., "K8sAllowedSpotRatio").
	// +optional
	AllowedConfigTemplate string `json:"allowedConfigTemplate,omitempty"`

	// Parameters holds plugin-specific guardrail parameters as an arbitrary JSON/YAML object.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Parameters *apiextensionsv1.JSON `json:"parameters,omitempty"`
}

// WorkloadClassGuardrailSpec defines the desired state of WorkloadClassGuardrail.
type WorkloadClassGuardrailSpec struct {
	// Constraints defines the core cloud-agnostic guardrails for WorkloadClasses.
	// +required
	Constraints Constraints `json:"constraints"`

	// PluginConstraints defines guardrail constraints grouped by plugin optimization intent.
	// +optional
	PluginConstraints *PluginConstraints `json:"pluginConstraints,omitempty"`
}

// WorkloadClassGuardrailStatus defines the observed state of WorkloadClassGuardrail.
type WorkloadClassGuardrailStatus struct {
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// WorkloadClassGuardrail is the Schema for the workloadclassguardrails API
type WorkloadClassGuardrail struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of WorkloadClassGuardrail
	// +required
	Spec WorkloadClassGuardrailSpec `json:"spec"`

	// status defines the observed state of WorkloadClassGuardrail
	// +optional
	Status WorkloadClassGuardrailStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkloadClassGuardrailList contains a list of WorkloadClassGuardrail
type WorkloadClassGuardrailList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkloadClassGuardrail `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkloadClassGuardrail{}, &WorkloadClassGuardrailList{})
}
