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
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Enforcement specifies how strictly Spot capacity usage is enforced.
// +kubebuilder:validation:Enum=Allowed;Required;Forbidden
type Enforcement string

const (
	// EnforcementRequired specifies that workloads must run on Spot capacity.
	EnforcementRequired Enforcement = "Required"

	// EnforcementForbidden specifies that workloads must not run on Spot capacity.
	EnforcementForbidden Enforcement = "Forbidden"

	// EnforcementAllowed specifies that workloads may choose whether to run on Spot capacity (default).
	EnforcementAllowed Enforcement = "Allowed"
)

// ReversionAction specifies the strategy for moving fallback workloads back to Spot capacity.
// +kubebuilder:validation:Enum=Active
type ReversionAction string

const (
	// ReversionActionActive actively evicts/disrupts fallback pods to reschedule them onto Spot capacity.
	ReversionActionActive ReversionAction = "Active"
)

// Constraints defines the guardrails for WorkloadClasses.
type Constraints struct {
	// Disruption defines the constraints within which WorkloadClasses can set disruption policies.
	Disruption Disruption `json:"disruption"`

	// Placement specifies compute capacity and scheduling guardrails for workloads.
	// +optional
	// +kubebuilder:default={}
	Placement Placement `json:"placement"`
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

// Placement defines scheduling and compute placement constraints for workloads,
// governing where and on what capacity types pods can be scheduled.
type Placement struct {
	// SpotPlacement defines guardrails and lifecycle rules for Spot capacity usage.
	// +optional
	// +kubebuilder:default={}
	SpotPlacement SpotPlacement `json:"spotPlacement"`
}

// Fallback controls On-Demand fallback behavior when Spot capacity is unavailable.
// +kubebuilder:validation:XValidation:rule="self.allowFallbackToOnDemand || !has(self.maxFallbackRatio)",message="maxFallbackRatio cannot be set when allowFallbackToOnDemand is false"
type Fallback struct {
	// AllowFallbackToOnDemand specifies if workload owners are wllowed to fallback to On-Demand.
	// +optional
	// +kubebuilder:default=true
	AllowFallbackToOnDemand *bool `json:"allowFallbackToOnDemand,omitempty"`

	// MaxFallbackRatio is the budget control specifying the maximum percentage (e.g. "25%")
	// or absolute number (e.g. 5) of pods in the class allowed on On-Demand fallback concurrently.
	// +optional
	// +kubebuilder:validation:XIntOrString
	// +kubebuilder:validation:XValidation:rule="type(self) == int ? self >= 0 : self.matches('^(100|[1-9]?[0-9])%$')",message="must be a non-negative integer or a percentage between 0% and 100%"
	MaxFallbackRatio *intstr.IntOrString `json:"maxFallbackRatio,omitempty"`
}

// Reversion controls the transition of fallback workloads back to Spot capacity.
type Reversion struct {
	// RequiredReversionAction enforces a reversion strategy, e.g., must be `Active`.
	// +optional
	RequiredReversionAction ReversionAction `json:"requiredReversionAction,omitempty"`

	// MaxFallbackDuration sets an upper bound on how long a workload can run on
	// fallback before GKE forces reversion (disruption) (e.g., "30m", "2h").
	// +optional
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="maxFallbackDuration must be greater than 0"
	MaxFallbackDuration *metav1.Duration `json:"maxFallbackDuration,omitempty"`
}

// SpotPlacement defines the policy and lifecycle rules for running workloads on
// Spot capacity, including minimum Spot allocation requirements, On-Demand
// fallback limits, and reversion behavior.
// +kubebuilder:validation:XValidation:rule="self.enforcementMode != 'Forbidden' || !has(self.minSpotRatio)",message="minSpotRatio cannot be set when enforcementMode is Forbidden"
type SpotPlacement struct {
	// EnforcementMode enforces Spot usage. Required forces Spot. Forbidden blocks Spot.
	// Allowed lets workload choose.
	// +optional
	// +kubebuilder:validation:Enum=Allowed;Required;Forbidden
	// +kubebuilder:default="Allowed"
	EnforcementMode string `json:"enforcementMode,omitempty"`

	// MinSpotRatio is the minimum allowed Spot ratio (e.g., "50%").
	// Prevents workload owners from configuring too low Spot ratios.
	// +optional
	// +kubebuilder:validation:Pattern=`^(100|[1-9]?[0-9])%$`
	MinSpotRatio string `json:"minSpotRatio,omitempty"`

	// Fallback controls On-Demand fallback behavior when Spot capacity is unavailable.
	// +optional
	// +kubebuilder:default={allowFallbackToOnDemand: true}
	Fallback Fallback `json:"fallback"`

	// Reversion controls the transition of fallback workloads back to Spot capacity.
	// +optional
	Reversion *Reversion `json:"reversion"`
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
