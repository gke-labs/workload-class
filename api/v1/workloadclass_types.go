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

	// CapacityStrategy defines portable, cloud-agnostic compute capacity and fallback intent
	// (e.g., targeting Spot vs. On-Demand capacity, target Spot percentage, and stockout fallback behavior).
	// +optional
	CapacityStrategy *CapacityStrategy `json:"capacityStrategy,omitempty"`

	// InfrastructureProfileRef references a cluster-scoped, provider-specific infrastructure profile CR
	// (such as GKESpotPlacementPolicy) managed by Platform/SRE.
	// +optional
	InfrastructureProfileRef *InfrastructureProfileReference `json:"infrastructureProfileRef,omitempty"`
}

// CapacityStrategy defines cloud-agnostic Spot/Preemptible capacity preferences.
type CapacityStrategy struct {
	// Type specifies whether the workload targets Spot or OnDemand capacity. Default: "Spot".
	// +optional
	// +kubebuilder:default="Spot"
	Type SpotPlacementType `json:"type,omitempty"`

	// SpotRatio is the target percentage of Pods to place on Spot capacity (e.g., "80%"). Default: "100%".
	// Must be an integer percentage between "0%" and "100%".
	// +optional
	// +kubebuilder:default="100%"
	// +kubebuilder:validation:Pattern=`^(100|[1-9]?[0-9])%$`
	SpotRatio string `json:"spotRatio,omitempty"`

	// FallbackAction determines whether Pods remain Pending ("Fail") or schedule onto On-Demand ("FallbackToOnDemand")
	// when Spot capacity is unavailable. Default: "Fail".
	// +optional
	// +kubebuilder:default="Fail"
	FallbackAction FallbackAction `json:"action,omitempty"`
}

// InfrastructureProfileReference identifies an external cluster-scoped, provider-specific profile CR.
type InfrastructureProfileReference struct {
	// Group is the API group of the infrastructure profile (e.g., "workloads.gke.io").
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Group string `json:"group,omitempty"`

	// Kind is the resource kind of the infrastructure profile (e.g., "GKESpotPlacementPolicy").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Kind string `json:"kind"`

	// Name is the metadata.name of the cluster-scoped infrastructure profile resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// MaintenanceReadiness indicates whether a workload is currently inside an allowed disruption window or overdue.
// +kubebuilder:validation:Enum=Ready;NotReady;Overdue
type MaintenanceReadiness string

const (
	// ReadinessReady indicates the workload is currently within an allowed disruption window.
	ReadinessReady MaintenanceReadiness = "Ready"
	// ReadinessNotReady indicates the workload is outside of its allowed disruption windows.
	ReadinessNotReady MaintenanceReadiness = "NotReady"
	// ReadinessOverdue indicates the workload has exceeded MaxNonDisruptionDurationDays and maintenance takes precedence.
	ReadinessOverdue MaintenanceReadiness = "Overdue"
)

const (
	// ConditionTypeValidated indicates if the WorkloadClass has been validated against Guardrails.
	ConditionTypeValidated = "Validated"

	// ReasonValidationPassed indicates that the WorkloadClass passed all Guardrail checks.
	ReasonValidationPassed = "ValidationPassed"
	// ReasonValidationFailed indicates that the WorkloadClass failed validation, either against Guardrails or due to the InfrastructureProfileRef.
	ReasonValidationFailed = "ValidationFailed"
	// ReasonNoGuardrails indicates that no Guardrails were found to validate against.
	ReasonNoGuardrails = "NoGuardrails"

	// ConditionTypePlacementPluginAttached indicates whether the referenced infrastructureProfileRef
	// has been resolved and attached by its corresponding placement plugin controller.
	ConditionTypePlacementPluginAttached = "PlacementPluginAttached"

	// ReasonPluginPending indicates that the WorkloadClass is waiting for the placement plugin controller to attach.
	ReasonPluginPending = "PluginPending"
	// ReasonProfileResolved indicates that the referenced infrastructure profile CR was found and attached.
	ReasonProfileResolved = "ProfileResolved"
	// ReasonPluginResolutionFailed indicates that the referenced infrastructure profile CR could not be found or resolved.
	ReasonPluginResolutionFailed = "PluginResolutionFailed"

	// ConditionTypeInFallback indicates whether Pods belonging to this WorkloadClass are currently running
	// on On-Demand fallback capacity due to a Spot stockout.
	ConditionTypeInFallback = "InFallback"

	// ReasonFallbackActive indicates that one or more Spot-targeted Pods are running on On-Demand fallback capacity,
	// or (with reversion None) that the workload fell back and remains latched on On-Demand.
	ReasonFallbackActive = "FallbackActive"
	// ReasonSpotTargetMet indicates that no Spot-targeted Pods are running on On-Demand fallback capacity.
	ReasonSpotTargetMet = "SpotTargetMet"
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
// +kubebuilder:resource:scope=Namespaced,shortName=wc
// +kubebuilder:printcolumn:name="Maintenance",type="string",JSONPath=".status.maintenanceReadiness"
// +kubebuilder:printcolumn:name="Capacity",type="string",JSONPath=".spec.capacityStrategy.type"
// +kubebuilder:printcolumn:name="SpotRatio",type="string",JSONPath=".spec.capacityStrategy.spotRatio"
// +kubebuilder:printcolumn:name="Profile",type="string",JSONPath=".spec.infrastructureProfileRef.name"
// +kubebuilder:printcolumn:name="Validated",type="string",JSONPath=".status.conditions[?(@.type=='Validated')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

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
