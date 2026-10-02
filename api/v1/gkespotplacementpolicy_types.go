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

// ReversionAction specifies the strategy for moving fallback workloads back to Spot capacity.
// +kubebuilder:validation:Enum=Active;Lazy;None
type ReversionAction string

const (
	// ActiveReversionAction proactively evicts Pods on On-Demand to migrate them back to Spot (respecting PDBs).
	ActiveReversionAction ReversionAction = "Active"

	// LazyReversionAction migrates to Spot only when Pods are naturally recreated.
	LazyReversionAction ReversionAction = "Lazy"

	// NoneReversionAction stays on On-Demand permanently after fallback (default).
	NoneReversionAction ReversionAction = "None"
)

// EnforcementMode specifies whether Spot usage is Required, Allowed (default), or Forbidden
// +kubebuilder:validation:Enum=Allowed;Required;Forbidden
type EnforcementMode string

const (
	// AllowedEnforcementMode lets WorkloadClasses decide whether or not to use Spot placement.
	AllowedEnforcementMode EnforcementMode = "Allowed"

	// RequiredEnforcementMode forces WorkloadClasses to use Spot placement.
	RequiredEnforcementMode EnforcementMode = "Required"

	// ForbiddenEnforcementMode forbids WorkloadClasses from using Spot placement.
	ForbiddenEnforcementMode EnforcementMode = "Forbidden"
)

const (
	// PluginNameGKESpotPlacement is the plugin identifier used in WorkloadClassGuardrail.spec.pluginConstraints.
	PluginNameGKESpotPlacement = "gke-spot-placement"

	// ConditionTypeSpotCapacityAvailable indicates whether GKE Spot capacity is currently available in the cluster.
	ConditionTypeSpotCapacityAvailable = "SpotCapacityAvailable"

	// ReasonSpotCapacityHealthy indicates that GKE Spot node groups are healthy and able to scale up.
	ReasonSpotCapacityHealthy = "SpotCapacityHealthy"
	// ReasonSpotStockout indicates that GKE Spot node groups are in stockout or scale-up backoff.
	ReasonSpotStockout = "SpotStockout"
)

// GKESpotPlacementPolicySpec defines the desired state of GKESpotPlacementPolicy.
type GKESpotPlacementPolicySpec struct {
	// CustomComputeClass is the optional name of a GKE Custom Compute Class
	// (cloud.google.com/compute-class) used for the workload's primary placement.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	CustomComputeClass string `json:"customComputeClass,omitempty"`

	// FallbackAllowedMachineFamilies restricts which GKE machine families Pods may schedule onto
	// when falling back to On-Demand capacity during a Spot stockout (e.g., ["e2", "n2d"]).
	// If empty, all machine families are allowed.
	// +optional
	// +listType=set
	FallbackAllowedMachineFamilies []string `json:"fallbackAllowedMachineFamilies,omitempty"`

	// Reversion configures how fallback Pods running on On-Demand nodes return to GKE Spot VMs
	// once Spot capacity recovers (Active, Lazy, or None).
	// +optional
	// +kubebuilder:default={}
	Reversion SpotReversionPolicy `json:"reversion,omitempty"`
}

// SpotReversionPolicy configures the policy for returning workloads to Spot once capacity returns.
// +kubebuilder:validation:XValidation:rule="!has(self.minDurationOnFallback) || duration(self.minDurationOnFallback) == duration('0s') || self.action == 'Active'",message="minDurationOnFallback can only be greater than 0s when reversion action is Active"
type SpotReversionPolicy struct {
	// Action specifies the reversion strategy once Spot capacity returns: Active, Lazy, or None (default).
	// +optional
	// +kubebuilder:default="None"
	Action ReversionAction `json:"action,omitempty"`

	// MinDurationOnFallback sets the minimum duration a Pod must run on On-Demand fallback VMs
	// before becoming eligible for Active reversion (e.g., "4h"), dampening churn when Spot capacity is unstable.
	// +optional
	// +kubebuilder:default="0s"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="minDurationOnFallback must be non-negative"
	MinDurationOnFallback *metav1.Duration `json:"minDurationOnFallback,omitempty"`
}

// GKESpotPlacementPolicyStatus defines the observed state of GKESpotPlacementPolicy.
type GKESpotPlacementPolicyStatus struct {
	// Conditions represent the current state of the GKESpotPlacementPolicy resource (e.g., Validated, SpotCapacityAvailable).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=gkespotpolicy
// +kubebuilder:printcolumn:name="ComputeClass",type="string",JSONPath=".spec.customComputeClass"
// +kubebuilder:printcolumn:name="Reversion",type="string",JSONPath=".spec.reversion.action"
// +kubebuilder:printcolumn:name="Validated",type="string",JSONPath=".status.conditions[?(@.type=='Validated')].status"
// +kubebuilder:printcolumn:name="SpotAvailable",type="string",JSONPath=".status.conditions[?(@.type=='SpotCapacityAvailable')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// GKESpotPlacementPolicy defines GKE-specific Spot placement, machine family, and reversion settings.
type GKESpotPlacementPolicy struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of GKESpotPlacementPolicy.
	// +required
	Spec GKESpotPlacementPolicySpec `json:"spec"`

	// status defines the observed state of GKESpotPlacementPolicy.
	// +optional
	Status GKESpotPlacementPolicyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// GKESpotPlacementPolicyList contains a list of GKESpotPlacementPolicy.
type GKESpotPlacementPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []GKESpotPlacementPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GKESpotPlacementPolicy{}, &GKESpotPlacementPolicyList{})
}

// GKESpotGuardrailParameters defines the schema for "gke-spot-placement" guardrail parameters.
//
// These parameters are stored as raw JSON in PluginConstraint.Parameters and are not part of
// any CRD schema, so the API server does not apply defaults or validation to them. Instead,
// the controllers decode them in Go: omitted fields are treated as "no restriction"
// (e.g., an empty EnforcementMode behaves as Allowed, a nil AllowFallbackToOnDemand behaves as true),
// and field values are validated by the WorkloadClassGuardrail controller.
type GKESpotGuardrailParameters struct {
	// EnforcementMode specifies whether Spot usage is Required, Allowed, or Forbidden.
	// If omitted, it is treated as Allowed.
	// +optional
	EnforcementMode EnforcementMode `json:"enforcementMode,omitempty"`

	// MinSpotRatio sets the minimum allowed Spot ratio as an integer percentage
	// between "0%" and "100%" (e.g., "80%"). If omitted, no minimum is enforced.
	// +optional
	MinSpotRatio *string `json:"minSpotRatio,omitempty"`

	// Fallback restricts On-Demand fallback configuration and capacity budget.
	// +optional
	Fallback *GKESpotFallbackConstraints `json:"fallback,omitempty"`

	// Reversion mandates reversion behavior and maximum fallback duration.
	// +optional
	Reversion *GKESpotReversionConstraints `json:"reversion,omitempty"`
}

// GKESpotFallbackConstraints defines guardrail constraints on On-Demand fallback behavior when Spot capacity is unavailable.
type GKESpotFallbackConstraints struct {
	// AllowFallbackToOnDemand specifies whether workload owners are allowed to fall back to On-Demand capacity.
	// If omitted, fallback is allowed.
	// +optional
	AllowFallbackToOnDemand *bool `json:"allowFallbackToOnDemand,omitempty"`

	// MaxFallbackRatio is the budget control specifying the maximum percentage (e.g., "25%")
	// or absolute number (e.g., 5) of Pods in the WorkloadClass allowed on On-Demand fallback concurrently.
	// If omitted, no fallback budget is enforced.
	// +optional
	MaxFallbackRatio *intstr.IntOrString `json:"maxFallbackRatio,omitempty"`
}

// GKESpotReversionConstraints defines guardrail constraints on transitioning fallback workloads back to Spot capacity.
type GKESpotReversionConstraints struct {
	// RequiredReversionAction enforces a specific reversion strategy (e.g., must be "Active").
	// If omitted, any reversion action is allowed.
	// +optional
	RequiredReversionAction *ReversionAction `json:"requiredReversionAction,omitempty"`

	// MaxFallbackDuration sets an upper bound on how long a Pod can run on On-Demand fallback
	// before reversion to Spot is forced (e.g., "30m", "2h"). Must be non-negative.
	// If omitted, no upper bound is enforced.
	// +optional
	MaxFallbackDuration *metav1.Duration `json:"maxFallbackDuration,omitempty"`
}
