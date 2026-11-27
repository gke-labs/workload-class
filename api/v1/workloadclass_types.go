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
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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
// +kubebuilder:validation:XValidation:rule="!(has(self.maxUnavailable) && has(self.minAvailable))",message="maxUnavailable and minAvailable are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.maxUnavailable) || (type(self.maxUnavailable) == int ? self.maxUnavailable >= 0 : self.maxUnavailable.matches('^(100|[1-9]?[0-9])%$'))",message="maxUnavailable must be a non-negative integer or a percentage string between 0% and 100%"
// +kubebuilder:validation:XValidation:rule="!has(self.minAvailable) || (type(self.minAvailable) == int ? self.minAvailable >= 0 : self.minAvailable.matches('^(100|[1-9]?[0-9])%$'))",message="minAvailable must be a non-negative integer or a percentage string between 0% and 100%"
type DisruptionPolicy struct {
	// AllowedDisruptionWindows defines when disruptions are allowed.
	// When unset or empty, disruptions are allowed at all times on all days of the week.
	// +listType=map
	// +listMapKey=name
	// +optional
	AllowedDisruptionWindows []DisruptionWindow `json:"allowedDisruptionWindows,omitempty"`

	// AllowedDisruptionsOutsideOfWindow specifies subjects that can disrupt even outside of windows. (e.g. VPA, ClusterAutoscaler)
	// +listType=map
	// +listMapKey=kind
	// +listMapKey=name
	// +optional
	AllowedDisruptionsOutsideOfWindow []Subject `json:"allowedDisruptionsOutsideOfWindow,omitempty"`

	// MaxNonDisruptionDurationDays is the maximum duration a workload can remain undisrupted.
	// If exceeded, maintenance takes precedence over per-pod run-duration hints.
	// When unset (0), no upper bound is placed on how long the workload can remain undisrupted.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxNonDisruptionDurationDays int32 `json:"maxNonDisruptionDurationDays,omitempty"`

	// MinInitialRunDurationDays is the minimum duration a pod must run before it can be disrupted.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinInitialRunDurationDays int32 `json:"minInitialRunDurationDays,omitempty"`

	// GraceTerminationDurationSeconds is the duration in seconds after a pod's deletionTimestamp is set during which
	// the controller keeps the WorkloadClass MaintenanceReadiness as NotReady to allow in-flight pod terminations to drain
	// before further disruptions occur. It does not override Pod.spec.terminationGracePeriodSeconds or set eviction DeleteOptions.gracePeriodSeconds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	GraceTerminationDurationSeconds int32 `json:"graceTerminationDurationSeconds,omitempty"`

	// EmergencyOverride allows bypassing all constraints immediately.
	// +optional
	EmergencyOverride bool `json:"emergencyOverride,omitempty"`

	// UnhealthyPodEvictionPolicy specifies how unhealthy (non-Ready) pods are evicted during an open
	// disruption window or when MaintenanceReadiness is Overdue.
	// Supported values are "AlwaysAllow" (default when open) and "IfHealthyBudget".
	// When MaintenanceReadiness is NotReady (maxUnavailable: 0), Per-Pod PDBs always enforce
	// "IfHealthyBudget" so unhealthy pods cannot bypass closed disruption windows.
	// +optional
	// +kubebuilder:validation:Enum=IfHealthyBudget;AlwaysAllow
	UnhealthyPodEvictionPolicy *policyv1.UnhealthyPodEvictionPolicyType `json:"unhealthyPodEvictionPolicy,omitempty"`

	// ManageSafeToEvictAnnotation enables automatic lifecycle management of the
	// "cluster-autoscaler.kubernetes.io/safe-to-evict" annotation on governed Pods.
	// When true and the WorkloadClass protects Pods (valid against at least one
	// WorkloadClassGuardrail) and is Ready or Overdue, "safe-to-evict: false" is temporarily
	// removed and "workloads.gke.io/safe-to-evict-suspended: <workloadclass-name>" is recorded
	// so Cluster Autoscaler can drain nodes during the maintenance window. When the window closes,
	// validation fails, this option is disabled, Pod ownership/selector matching changes, or the
	// WorkloadClass is deleted, suspended Pods have "safe-to-evict: false" restored.
	// Note: in a cluster with no WorkloadClassGuardrail objects, the WorkloadClass does not
	// protect Pods and this option has no effect (annotations are never suspended).
	// +optional
	ManageSafeToEvictAnnotation bool `json:"manageSafeToEvictAnnotation,omitempty"`

	// MaxUnavailable limits how many governed Pods may be concurrently unavailable during disruptions
	// (an integer such as 1, or a percentage string such as "25%"). Mutually exclusive with MinAvailable.
	// When neither MaxUnavailable nor MinAvailable is set, in-window pacing is disabled.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// MinAvailable specifies the minimum number or percentage of governed Pods that must remain
	// Ready during disruptions (e.g., 2 or "75%"). Mutually exclusive with MaxUnavailable.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`

	// DisruptionStuckThresholdSeconds is the number of seconds an in-window eviction may remain
	// blocked by the pacing budget before break-glass allows it to proceed. Defaults to 120 when unset.
	// Set to 0 to disable break-glass (strict budget enforcement with no timeout override, e.g., for quorum systems).
	// Does not apply to automated actors bypassing windows via allowedDisruptionsOutsideOfWindow.
	// +optional
	// +kubebuilder:default=120
	// +kubebuilder:validation:Minimum=0
	DisruptionStuckThresholdSeconds *int32 `json:"disruptionStuckThresholdSeconds,omitempty"`
}

// DisruptionWindow defines a temporal window when disruptions are allowed.
type DisruptionWindow struct {
	// Name is the name of the DisruptionWindow.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// DaysOfWeek specifies the days of the week (Monday-Sunday) on which the window starts.
	// When unset or empty, the window applies to every day of the week.
	// +optional
	DaysOfWeek []string `json:"daysOfWeek,omitempty"`

	// TimeZone is the IANA Time Zone Database name (e.g., "America/Los_Angeles", "America/Argentina/Buenos_Aires", "Etc/UTC", "UTC"). See https://www.iana.org/time-zones.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^([A-Za-z0-9_+-]+(/[A-Za-z0-9_+-]+){0,2}|UTC|GMT)$`
	// +kubebuilder:validation:XValidation:rule="self != 'Local' && self != 'local'",message="must not be 'Local' or 'local'"
	TimeZone string `json:"timeZone,omitempty"`

	// StartTime is the start time of the window in HH:MM format (e.g. 22:00) in the specified timeZone (or UTC if timeZone is omitted).
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$`
	StartTime string `json:"startTime,omitempty"`

	// EndTime is the end time of the window in HH:MM format (e.g. 04:00) in the specified timeZone (or UTC if timeZone is omitted).
	// +optional
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
	// PodSelector matches the pods in the same namespace that this class applies to.
	// If a Namespace has the "workloads.gke.io/enforced-class" label (EnforcedClassLabel) set,
	// the referenced WorkloadClass is enforced for all pods in that namespace and overrides PodSelector
	// matching. Otherwise, PodSelector selects the target pods in the namespace; overlapping PodSelectors
	// across WorkloadClasses in the same namespace are surfaced as a status Condition.
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

	// ResourceManagement configures autoscaling and rightsizing for the Deployments and StatefulSets that the
	// WorkloadClass governs. For each governed workload, the controller creates and owns one HorizontalPodAutoscaler
	// (from autoscaling) and one VerticalPodAutoscaler (from rightsizing).
	// +optional
	ResourceManagement *ResourceManagement `json:"resourceManagement,omitempty"`
}

// AutoscalingProfile selects how quickly autoscaled workloads scale up and down.
// +kubebuilder:validation:Enum=Responsive;Balanced;Conservative
type AutoscalingProfile string

const (
	// AutoscalingProfileResponsive scales down sooner than Balanced, for workloads whose load changes quickly.
	AutoscalingProfileResponsive AutoscalingProfile = "Responsive"

	// AutoscalingProfileBalanced uses the upstream HorizontalPodAutoscaler default behavior (default).
	AutoscalingProfileBalanced AutoscalingProfile = "Balanced"

	// AutoscalingProfileConservative scales up and down more slowly than Balanced, for workloads that are
	// expensive to start or stop.
	AutoscalingProfileConservative AutoscalingProfile = "Conservative"
)

// RightsizingMode selects whether and when rightsizing applies new container sizes.
// +kubebuilder:validation:Enum=Off;Recommend;OnCreate;Auto
type RightsizingMode string

const (
	// RightsizingModeOff disables rightsizing: no VerticalPodAutoscaler is created.
	RightsizingModeOff RightsizingMode = "Off"

	// RightsizingModeRecommend only computes size recommendations, without applying them (default).
	RightsizingModeRecommend RightsizingMode = "Recommend"

	// RightsizingModeOnCreate applies sizes only when Pods are created.
	RightsizingModeOnCreate RightsizingMode = "OnCreate"

	// RightsizingModeAuto applies sizes when Pods are created and to running Pods, as set by the update strategy.
	RightsizingModeAuto RightsizingMode = "Auto"
)

// RightsizingUpdateStrategy selects how the Auto rightsizing mode applies a new size to a running Pod.
// +kubebuilder:validation:Enum=InPlacePreferred;Recreate
type RightsizingUpdateStrategy string

const (
	// RightsizingUpdateStrategyInPlacePreferred resizes the Pod in place when it can, and recreates it when it
	// can't (default).
	RightsizingUpdateStrategyInPlacePreferred RightsizingUpdateStrategy = "InPlacePreferred"

	// RightsizingUpdateStrategyRecreate always recreates the Pod.
	RightsizingUpdateStrategyRecreate RightsizingUpdateStrategy = "Recreate"
)

// RightsizedResource is a container resource that rightsizing can size.
// +kubebuilder:validation:Enum=cpu;memory
type RightsizedResource string

const (
	// RightsizedResourceCPU is the container CPU request.
	RightsizedResourceCPU RightsizedResource = "cpu"

	// RightsizedResourceMemory is the container memory request.
	RightsizedResourceMemory RightsizedResource = "memory"
)

// ResourceManagement configures how the workloads that a WorkloadClass governs scale horizontally (autoscaling)
// and how their containers are sized (rightsizing). Either part can be set on its own.
// +kubebuilder:validation:XValidation:rule="has(self.autoscaling) || has(self.rightsizing)",message="set autoscaling, rightsizing, or both"
type ResourceManagement struct {
	// Autoscaling configures horizontal scaling: one HorizontalPodAutoscaler per governed workload.
	// +optional
	Autoscaling *Autoscaling `json:"autoscaling,omitempty"`

	// Rightsizing configures vertical sizing: one VerticalPodAutoscaler per governed workload.
	// Rightsizing needs the VerticalPodAutoscaler to be installed on the cluster.
	// +optional
	Rightsizing *Rightsizing `json:"rightsizing,omitempty"`
}

// Autoscaling configures horizontal scaling for the workloads that a WorkloadClass governs.
// +kubebuilder:validation:XValidation:rule="!has(self.replicas.min) || self.replicas.min <= self.replicas.max",message="replicas.min must not exceed replicas.max"
// +kubebuilder:validation:XValidation:rule="self.metrics.all(m, m.type != 'Object')",message="Object metrics are not allowed"
type Autoscaling struct {
	// Replicas bounds the number of replicas of every governed workload.
	// +required
	Replicas ReplicaBounds `json:"replicas"`

	// Metrics lists what to scale on. It is the upstream autoscaling/v2 metric list, and it is copied into each
	// HorizontalPodAutoscaler as written. Object metrics are not allowed, because they name one specific object.
	// +required
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	Metrics []autoscalingv2.MetricSpec `json:"metrics"`

	// Profile selects how quickly governed workloads scale up and down: Responsive, Balanced (default),
	// or Conservative.
	// +optional
	// +kubebuilder:default=Balanced
	Profile AutoscalingProfile `json:"profile,omitempty"`
}

// ReplicaBounds bounds the number of replicas of a workload.
type ReplicaBounds struct {
	// Min is the minimum number of replicas for each governed workload. Defaults to 1.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Min *int32 `json:"min,omitempty"`

	// Max is the maximum number of replicas for each governed workload.
	// +required
	// +kubebuilder:validation:Minimum=1
	Max int32 `json:"max"`
}

// Rightsizing configures vertical sizing for the workloads that a WorkloadClass governs.
type Rightsizing struct {
	// Mode selects whether and when rightsizing applies new container sizes: Off (no rightsizing), Recommend
	// (default, recommendations only), OnCreate (apply sizes only when Pods are created), or Auto (also apply
	// sizes to running Pods). Quote "Off" in YAML, where an unquoted Off is read as a boolean.
	// +optional
	// +kubebuilder:default=Recommend
	Mode RightsizingMode `json:"mode,omitempty"`

	// Resources lists the container resources to rightsize: cpu, memory, or both (default).
	// When autoscaling targets the utilization of a resource, that resource is left out of rightsizing.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:default={cpu,memory}
	Resources []RightsizedResource `json:"resources,omitempty"`

	// UpdateStrategy selects how the Auto mode applies a new size to a running Pod. InPlacePreferred (default)
	// resizes the Pod in place when it can, and recreates it when it can't. Recreate always recreates the Pod.
	// +optional
	// +kubebuilder:default=InPlacePreferred
	UpdateStrategy RightsizingUpdateStrategy `json:"updateStrategy,omitempty"`

	// ExcludeContainers lists the containers that rightsizing leaves alone, such as sidecars.
	// +optional
	// +listType=set
	ExcludeContainers []string `json:"excludeContainers,omitempty"`

	// Bounds sets the per-container floor and ceiling for the sizes that rightsizing sets. Where unset, the
	// guardrail containerBounds apply.
	// +optional
	Bounds *ContainerBounds `json:"bounds,omitempty"`
}

// ContainerBounds sets a per-container floor (min) and ceiling (max) for the cpu and memory sizes that
// rightsizing sets.
// +kubebuilder:validation:XValidation:rule="!has(self.min) || !has(self.max) || self.min.all(r, !(r in self.max) || quantity(string(self.min[r])).compareTo(quantity(string(self.max[r]))) <= 0)",message="min must not exceed max for any resource"
type ContainerBounds struct {
	// Min is the per-container floor, by resource.
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:MaxProperties=2
	// +kubebuilder:validation:XValidation:rule="self.all(r, r == 'cpu' || r == 'memory')",message="only cpu and memory are supported"
	Min corev1.ResourceList `json:"min,omitempty"`

	// Max is the per-container ceiling, by resource.
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:MaxProperties=2
	// +kubebuilder:validation:XValidation:rule="self.all(r, r == 'cpu' || r == 'memory')",message="only cpu and memory are supported"
	Max corev1.ResourceList `json:"max,omitempty"`
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
	// EnforcedClassLabel is the label key applied to a Namespace to enforce a single WorkloadClass across all
	// pods in that namespace. When set on a Namespace, the referenced WorkloadClass applies to all pods in the
	// namespace and overrides PodSelector matching. It is not a fallback default.
	EnforcedClassLabel = "workloads.gke.io/enforced-class"

	// PodUIDLabel is the label set on governed Pods and their Per-Pod PodDisruptionBudgets
	// to associate each PDB with a single Pod instance by UID.
	PodUIDLabel = "workloads.gke.io/pod-uid"

	// ManagedByLabel is the label set on the HorizontalPodAutoscalers and VerticalPodAutoscalers that the
	// controller creates for the workloads a WorkloadClass governs, with the value ManagedByLabelValue.
	// Autoscalers without it are user-created, and take precedence over the WorkloadClass.
	ManagedByLabel = "workloads.gke.io/managed-by"

	// ManagedByLabelValue is the value of ManagedByLabel on the autoscalers that the controller manages.
	ManagedByLabelValue = "workload-class"

	// WorkloadClassLabel is the label set on the autoscalers that the controller manages, with the name of the
	// WorkloadClass they were created from.
	WorkloadClassLabel = "workloads.gke.io/workload-class"

	// ConditionTypeValidated indicates if the WorkloadClass has been validated against Guardrails across all pillars.
	ConditionTypeValidated = "Validated"

	// ConditionTypeDisruptionValidated indicates whether the WorkloadClass disruption policy passed spec and
	// guardrail validation independently of placement validation.
	ConditionTypeDisruptionValidated = "DisruptionValidated"

	// ConditionTypePlacementValidated indicates whether the WorkloadClass placement configuration
	// (capacityStrategy and infrastructureProfileRef) passed guardrail validation and plugin attachment
	// independently of disruption validation.
	ConditionTypePlacementValidated = "PlacementValidated"

	// ConditionTypeResourceManagementValidated indicates whether the WorkloadClass resourceManagement passed spec
	// and guardrail validation, independently of the disruption and placement pillars. It is set only when
	// resourceManagement is configured.
	ConditionTypeResourceManagementValidated = "ResourceManagementValidated"

	// ReasonValidationPassed indicates that the WorkloadClass passed all Guardrail checks.
	ReasonValidationPassed = "ValidationPassed"
	// ReasonValidationFailed indicates that the WorkloadClass failed validation, either against Guardrails or due to the InfrastructureProfileRef.
	ReasonValidationFailed = "ValidationFailed"
	// ReasonNoGuardrails indicates that no Guardrails were found to validate against.
	ReasonNoGuardrails = "NoGuardrails"
	// ReasonSelectorOverlap indicates that the WorkloadClass PodSelector overlaps with another WorkloadClass in the
	// same namespace. It is only used when the WorkloadClass has no other validation failures. Each Pod is still
	// governed by its single best-match WorkloadClass, so overlapping WorkloadClasses keep protecting their Pods.
	ReasonSelectorOverlap = "SelectorOverlap"

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

	// ReasonFallbackActive indicates that one or more Pods are running on On-Demand fallback capacity below the target SpotRatio.
	ReasonFallbackActive = "FallbackActive"
	// ReasonSpotTargetMet indicates that the workload meets its target SpotRatio with no active fallback deficit.
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
