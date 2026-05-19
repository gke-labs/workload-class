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

// DisruptionPolicy specifies the policy governing pod disruptions.
type DisruptionPolicy struct {
	// AllowedDisruptionWindows defines when disruptions are allowed.
	// +optional
	AllowedDisruptionWindows []DisruptionWindow `json:"allowedDisruptionWindows,omitempty"`

	// AllowedDisruptionsOutsideOfWindow specifies identities or components that can disrupt even outside of windows. (e.g. "VPA", "ClusterAutoscaler")
	// +optional
	AllowedDisruptionsOutsideOfWindow []string `json:"allowedDisruptionsOutsideOfWindow,omitempty"`

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

// WorkloadClassSpec defines the desired state of WorkloadClass
type WorkloadClassSpec struct {
	// PodSelector matches the pods that this class applies to.
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`

	// DisruptionPolicy specifies the policy governing pod disruptions.
	// +optional
	DisruptionPolicy DisruptionPolicy `json:"disruptionPolicy,omitempty"`
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
