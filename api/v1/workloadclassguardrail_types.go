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

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

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

// WorkloadClassGuardrailSpec defines the desired state of WorkloadClassGuardrail
type WorkloadClassGuardrailSpec struct {
	// Constraints defines the guardrails for WorkloadClasses.
	Constraints Constraints `json:"constraints"`
}

// WorkloadClassGuardrailStatus defines the observed state of WorkloadClassGuardrail.
type WorkloadClassGuardrailStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the WorkloadClassGuardrail resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
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
