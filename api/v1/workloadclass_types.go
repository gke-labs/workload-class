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

// SpotPlacementType specifies the desired compute provisioning model for a workload.
// +kubebuilder:validation:Enum=Spot;OnDemand
type SpotPlacementType string

const (
	// SpotPlacementTypeSpot specifies Spot VMs as the desired provisioning model (default).
	SpotPlacementTypeSpot SpotPlacementType = "Spot"

	// SpotPlacementTypeOnDemand specifies On-Demand VMs as the desired provisioning model,
	// allowing workloads to opt out when the cluster/namespace default is Spot.
	SpotPlacementTypeOnDemand SpotPlacementType = "OnDemand"
)

// FallbackAction defines the action to take when Spot capacity is unavailable (stockout).
// +kubebuilder:validation:Enum=Fail;FallbackToOnDemand
type FallbackAction string

const (
	// FallbackActionFail keeps pods Pending when Spot capacity is unavailable (default).
	FallbackActionFail FallbackAction = "Fail"

	// FallbackActionFallbackToOnDemand schedules pods onto On-Demand VMs during Spot stockouts.
	FallbackActionFallbackToOnDemand FallbackAction = "FallbackToOnDemand"
)

// PlacementPolicy defines compute capacity and scheduling placement intents for a WorkloadClass.
type PlacementPolicy struct {
	// SpotPlacement specifies the desired Spot capacity allocation, stockout fallback,
	// and reversion behavior for pods in this WorkloadClass.
	// +optional
	// +kubebuilder:default={}
	SpotPlacement SpotPlacementPolicy `json:"spotPlacement,omitempty"`
}

// SpotPlacementPolicy defines the desired Spot provisioning state, target Spot ratio,
// stockout fallback strategy, and reversion policy for a WorkloadClass.
type SpotPlacementPolicy struct {
	// Type specifies the desired provisioning model: Spot (default) or OnDemand
	// (to opt out if the default is Spot).
	// +optional
	// +kubebuilder:default="Spot"
	Type string `json:"type"`

	// SpotRatio specifies the target percentage of Spot VMs for this workload
	// (e.g., "80%" Spot, 20% On-Demand), enforced via Pod Topology Spread Constraints.
	// +optional
	// +kubebuilder:default="100%"
	// +kubebuilder:validation:Pattern=`^(100|[1-9]?[0-9])%$`
	SpotRatio string `json:"spotRatio,omitempty"`

	// Fallback configures pod scheduling behavior when Spot capacity is unavailable (stockout).
	// +optional
	// +kubebuilder:default={}
	Fallback SpotFallbackPolicy `json:"fallback,omitempty"`

	// Reversion configures how and when workloads return to Spot once capacity is restored.
	// +optional
	// +kubebuilder:default={}
	Reversion SpotReversionPolicy `json:"reversion,omitempty"`
}

// SpotFallbackPolicy configures fallback behavior when Spot capacity is exhausted.
type SpotFallbackPolicy struct {
	// Action specifies the action to take on Spot stockout:
	// - Fail: Keeps pods Pending (default).
	// - FallbackToOnDemand: Schedules pods on On-Demand VMs.
	// +optional
	// +kubebuilder:default="Fail"
	Action FallbackAction `json:"action,omitempty"`
	// AllowedMachineFamilies is an optional allowlist of GCE machine families
	// (e.g., ["n2d", "t2d"]) permitted for On-Demand fallback VMs to control costs.
	// If empty, all machine families are allowed.
	// +optional
	AllowedMachineFamilies []string `json:"allowedMachineFamilies,omitempty"`
}

// SpotReversionPolicy configures the policy for returning workloads to Spot once capacity returns.
type SpotReversionPolicy struct {
	// Action specifies the reversion strategy once Spot capacity returns:
	// - Active: Proactively evicts pods on On-Demand to migrate them to Spot (respecting PDBs).
	// - Lazy: Migrates to Spot only when pods are naturally recreated.
	// - None: Stays on On-Demand permanently after fallback (default).
	// +optional
	// +kubebuilder:default="None"
	Action ReversionAction `json:"action,omitempty"`
	// MinDurationOnFallback sets the minimum duration a pod must run on On-Demand fallback VMs
	// before becoming eligible for Active reversion (e.g., "4h"), dampening churn when Spot capacity is unstable.
	// +optional
	// +kubebuilder:default="0s"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="minDurationOnFallback must be non-negative"
	MinDurationOnFallback *metav1.Duration `json:"minDurationOnFallback,omitempty"`
}

// DisruptionPolicy specifies the policy governing pod disruptions.
type DisruptionPolicy struct {
	// AllowedDisruptionWindows defines when disruptions are allowed.
	// +optional
	AllowedDisruptionWindows []DisruptionWindow `json:"allowedDisruptionWindows,omitempty"`

	// AllowedDisruptionsOutsideOfWindow specifies subjects that can disrupt even outside of windows. (e.g. VPA, ClusterAutoscaler)
	// +optional
	AllowedDisruptionsOutsideOfWindow []Subject `json:"allowedDisruptionsOutsideOfWindow,omitempty"`

	// MaxNonDisruptionDurationDays is the maximum duration a workload can remain undisrupted.
	// If exceeded, maintenance takes precedence over per-pod run-duration hints.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxNonDisruptionDurationDays int32 `json:"maxNonDisruptionDurationDays,omitempty"`

	// MinInitialRunDurationDays is the minimum duration a pod must run before it can be disrupted.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinInitialRunDurationDays int32 `json:"minInitialRunDurationDays,omitempty"`

	// GraceTerminationDuration is the maximum time in seconds for a pod to terminate gracefully.
	// +optional
	// +kubebuilder:validation:Minimum=0
	GraceTerminationDuration int32 `json:"graceTerminationDuration,omitempty"`

	// EmergencyOverride allows bypassing all constraints immediately.
	// +optional
	EmergencyOverride bool `json:"emergencyOverride,omitempty"`
}

// DisruptionWindow defines a temporal window when disruptions are allowed.
type DisruptionWindow struct {
	// Name is the name of the DisruptionWindow.
	Name string `json:"name,omitempty"`

	// DaysOfWeek specifies the days of the week (Monday-Sunday).
	DaysOfWeek []string `json:"daysOfWeek,omitempty"`

	// TimeZone is the IANA Time Zone Database name (e.g., "America/Los_Angeles", "Etc/UTC"). See https://www.iana.org/time-zones.
	// +kubebuilder:validation:Pattern=`^[A-Za-z_]+/[A-Za-z_]+$`
	TimeZone string `json:"timeZone,omitempty"`

	// StartTime is the start time of the window in HH:MM format (e.g. 22:00) in UTC.
	// +kubebuilder:validation:Pattern=`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$`
	StartTime string `json:"startTime,omitempty"`

	// EndTime is the end time of the window in HH:MM format (e.g. 04:00) in UTC.
	// +kubebuilder:validation:Pattern=`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$`
	EndTime string `json:"endTime,omitempty"`
}

// Subject defines the identity of a user, group, or service account.
// It is used to specify who or what is granted permissions or is the target
// of a policy. The structure aligns with standard Kubernetes RBAC subjects.
type Subject struct {
	// Kind can be User, Group, or ServiceAccount
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=User;Group;ServiceAccount
	Kind string `json:"kind"`

	// Name of the identity
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace of the ServiceAccount (only for ServiceAccount kind)
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"
	Namespace string `json:"namespace,omitempty"`
}

// WorkloadClassSpec defines the desired state of WorkloadClass
type WorkloadClassSpec struct {
	// PodSelector matches the pods that this class applies to.
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`

	// DisruptionPolicy specifies the policy governing pod disruptions.
	// +optional
	DisruptionPolicy DisruptionPolicy `json:"disruptionPolicy,omitempty"`

	// PlacementPolicy defines compute capacity and scheduling placement intents for a WorkloadClass.
	// +optional
	PlacementPolicy PlacementPolicy `json:"placementPolicy,omitempty"`
}

type MaintenanceReadiness string

const (
	ReadinessReady    MaintenanceReadiness = "Ready"
	ReadinessNotReady MaintenanceReadiness = "NotReady"
	ReadinessOverdue  MaintenanceReadiness = "Overdue"
)

const (
	// ConditionTypeValidated indicates if the WorkloadClass has been validated against Guardrails.
	ConditionTypeValidated = "Validated"

	// ReasonValidationPassed indicates that the WorkloadClass passed all Guardrail checks.
	ReasonValidationPassed = "ValidationPassed"
	// ReasonValidationFailed indicates that the WorkloadClass failed one or more Guardrail checks.
	ReasonValidationFailed = "ValidationFailed"
	// ReasonNoGuardrails indicates that no Guardrails were found to validate against.
	ReasonNoGuardrails = "NoGuardrails"
)

// WorkloadClassStatus defines the observed state of WorkloadClass.
type WorkloadClassStatus struct {
	// MaintenanceReadiness indicates if the workload is currently ready for maintenance.
	// +optional
	MaintenanceReadiness MaintenanceReadiness `json:"maintenanceReadiness,omitempty"`

	// LastDisruptionTime is the last time a disruption was observed for pods in this class.
	// +optional
	LastDisruptionTime *metav1.Time `json:"lastDisruptionTime,omitempty"`

	// Conditions represent the current state of the WorkloadClass resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced

// WorkloadClass is the Schema for the workloadclasses API
type WorkloadClass struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of WorkloadClass
	// +required
	Spec WorkloadClassSpec `json:"spec"`

	// status defines the observed state of WorkloadClass
	// +optional
	Status WorkloadClassStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkloadClassList contains a list of WorkloadClass
type WorkloadClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkloadClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkloadClass{}, &WorkloadClassList{})
}
