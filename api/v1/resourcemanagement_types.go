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
)

// LimitPolicy specifies how initial resources set limits on containers that do not set them.
// +kubebuilder:validation:Enum=MemoryEqualsRequest;EqualsRequest;None
type LimitPolicy string

const (
	// LimitPolicyMemoryEqualsRequest sets the memory limit to the memory request and leaves CPU unlimited (default).
	LimitPolicyMemoryEqualsRequest LimitPolicy = "MemoryEqualsRequest"

	// LimitPolicyEqualsRequest sets the CPU and memory limits to the matching requests.
	LimitPolicyEqualsRequest LimitPolicy = "EqualsRequest"

	// LimitPolicyNone leaves limits unset.
	LimitPolicyNone LimitPolicy = "None"
)

// AutoscalingProfile specifies how quickly a workload scales up and down.
// +kubebuilder:validation:Enum=Responsive;Balanced;Conservative
type AutoscalingProfile string

const (
	// AutoscalingProfileResponsive scales up immediately and scales down after a short stabilization window.
	AutoscalingProfileResponsive AutoscalingProfile = "Responsive"

	// AutoscalingProfileBalanced uses the upstream HorizontalPodAutoscaler default behavior (default).
	AutoscalingProfileBalanced AutoscalingProfile = "Balanced"

	// AutoscalingProfileConservative scales up and down slowly, after longer stabilization windows.
	AutoscalingProfileConservative AutoscalingProfile = "Conservative"
)

// RightsizingMode specifies whether and when rightsizing applies new container sizes.
// +kubebuilder:validation:Enum=Off;Recommend;OnCreate;Auto
type RightsizingMode string

const (
	// RightsizingModeOff disables rightsizing.
	RightsizingModeOff RightsizingMode = "Off"

	// RightsizingModeRecommend only publishes recommendations (default).
	RightsizingModeRecommend RightsizingMode = "Recommend"

	// RightsizingModeOnCreate applies sizes only when Pods are created.
	RightsizingModeOnCreate RightsizingMode = "OnCreate"

	// RightsizingModeAuto applies sizes when Pods are created and to running Pods.
	RightsizingModeAuto RightsizingMode = "Auto"
)

// RightsizingUpdateStrategy specifies how the Auto rightsizing mode applies a new size to a running Pod.
// +kubebuilder:validation:Enum=InPlacePreferred;Recreate
type RightsizingUpdateStrategy string

const (
	// RightsizingUpdateStrategyInPlacePreferred resizes the Pod in place when possible, and recreates it otherwise (default).
	RightsizingUpdateStrategyInPlacePreferred RightsizingUpdateStrategy = "InPlacePreferred"

	// RightsizingUpdateStrategyRecreate always recreates the Pod.
	RightsizingUpdateStrategyRecreate RightsizingUpdateStrategy = "Recreate"
)

// RightsizedResource is a resource that rightsizing can manage.
// +kubebuilder:validation:Enum=cpu;memory
type RightsizedResource string

const (
	// RightsizedResourceCPU rightsizes CPU.
	RightsizedResourceCPU RightsizedResource = "cpu"

	// RightsizedResourceMemory rightsizes memory.
	RightsizedResourceMemory RightsizedResource = "memory"
)

// ResourceManagement defines how the workloads governed by a WorkloadClass start, scale, and are rightsized.
// +kubebuilder:validation:XValidation:rule="has(self.initialResources) || has(self.autoscaling) || has(self.rightsizing)",message="set at least one of initialResources, autoscaling or rightsizing"
type ResourceManagement struct {
	// InitialResources fills in requests and limits for containers that do not set them, when a governed Pod is created.
	// +optional
	InitialResources *InitialResources `json:"initialResources,omitempty"`

	// Autoscaling defines horizontal scaling. One HorizontalPodAutoscaler is created for each governed
	// Deployment or StatefulSet.
	// +optional
	Autoscaling *Autoscaling `json:"autoscaling,omitempty"`

	// Rightsizing defines vertical sizing. One VerticalPodAutoscaler is created for each governed
	// Deployment or StatefulSet. Rightsizing requires the VerticalPodAutoscaler CRD and components
	// to be installed on the cluster.
	// +optional
	Rightsizing *Rightsizing `json:"rightsizing,omitempty"`
}

// InitialResources fills in missing requests and limits when a governed Pod is created. Values that a
// container already sets are never changed.
// +kubebuilder:validation:XValidation:rule="'cpu' in self.requests && 'memory' in self.requests",message="requests must set both cpu and memory"
type InitialResources struct {
	// Requests are the requests to set on containers that do not set them. Each resource is filled in
	// separately: a container that sets only a memory request gets the cpu request. Both cpu and memory
	// must be set, and no other resource may be set.
	// +kubebuilder:validation:Required
	Requests corev1.ResourceList `json:"requests"`

	// LimitPolicy specifies how to set limits on containers that do not set them. Default: "MemoryEqualsRequest".
	// Limits that a container already sets are never changed.
	// +optional
	// +kubebuilder:default="MemoryEqualsRequest"
	LimitPolicy LimitPolicy `json:"limitPolicy,omitempty"`

	// ExcludeContainers lists the names of containers that initial resources leave alone.
	// +optional
	// +listType=set
	ExcludeContainers []string `json:"excludeContainers,omitempty"`
}

// Autoscaling defines the horizontal scaling of the workloads governed by a WorkloadClass.
// +kubebuilder:validation:XValidation:rule="!has(self.replicas.min) || self.replicas.min <= self.replicas.max",message="replicas.min must not exceed replicas.max"
// +kubebuilder:validation:XValidation:rule="self.metrics.all(m, m.type != 'Object')",message="Object metrics are not allowed because they name one specific object"
type Autoscaling struct {
	// Replicas are the replica bounds for every governed workload.
	// +kubebuilder:validation:Required
	Replicas ReplicaBounds `json:"replicas"`

	// Metrics specify what to scale on. This is the upstream autoscaling/v2 metric list, copied into each
	// HorizontalPodAutoscaler as written. Object metrics are not allowed.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Metrics []autoscalingv2.MetricSpec `json:"metrics"`

	// Profile specifies how quickly to scale up and down. Default: "Balanced".
	// +optional
	// +kubebuilder:default="Balanced"
	Profile AutoscalingProfile `json:"profile,omitempty"`
}

// ReplicaBounds defines the minimum and maximum number of replicas of a governed workload.
type ReplicaBounds struct {
	// Min is the minimum number of replicas. Default: 1.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Min *int32 `json:"min,omitempty"`

	// Max is the maximum number of replicas.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Max int32 `json:"max"`
}

// Rightsizing defines the vertical sizing of the workloads governed by a WorkloadClass.
type Rightsizing struct {
	// Mode specifies whether and when new sizes are applied. Default: "Recommend".
	// +optional
	// +kubebuilder:default="Recommend"
	Mode RightsizingMode `json:"mode,omitempty"`

	// Resources lists the resources to rightsize. When unset or empty, both cpu and memory are rightsized.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=2
	Resources []RightsizedResource `json:"resources,omitempty"`

	// UpdateStrategy specifies how the Auto mode applies a new size to a running Pod. Default: "InPlacePreferred".
	// +optional
	// +kubebuilder:default="InPlacePreferred"
	UpdateStrategy RightsizingUpdateStrategy `json:"updateStrategy,omitempty"`

	// ExcludeContainers lists the names of containers that rightsizing leaves alone, such as sidecars.
	// +optional
	// +listType=set
	ExcludeContainers []string `json:"excludeContainers,omitempty"`

	// Bounds is the per-container floor and ceiling for the sizes that rightsizing sets. When unset, the
	// guardrail containerBounds apply.
	// +optional
	Bounds *ContainerBounds `json:"bounds,omitempty"`
}

// ContainerBounds defines a per-container floor and ceiling for cpu and memory.
//
// Only cpu and memory may be set, and each min must not exceed the matching max. These rules are checked by
// the controllers and reported on the status of the WorkloadClass or WorkloadClassGuardrail, rather than by
// the CRD schema: a CEL rule over a resource map of unbounded size exceeds the API server's rule cost budget.
type ContainerBounds struct {
	// Min is the per-container floor.
	// +optional
	Min corev1.ResourceList `json:"min,omitempty"`

	// Max is the per-container ceiling.
	// +optional
	Max corev1.ResourceList `json:"max,omitempty"`
}

// ResourceManagementConstraints defines the constraints within which WorkloadClasses can set resource management.
// Unset constraints are not enforced. When several guardrails apply, the strictest value wins.
type ResourceManagementConstraints struct {
	// RequireInitialResources requires every WorkloadClass to set resourceManagement.initialResources, so
	// that no governed Pod starts without requests.
	// +optional
	RequireInitialResources bool `json:"requireInitialResources,omitempty"`

	// AllowedLimitPolicies lists the initial resources limit policies that WorkloadClasses may use.
	// When unset or empty, all limit policies are allowed.
	// +optional
	// +listType=set
	AllowedLimitPolicies []LimitPolicy `json:"allowedLimitPolicies,omitempty"`

	// AllowedRightsizingModes lists the rightsizing modes that WorkloadClasses may use.
	// When unset or empty, all modes are allowed.
	// +optional
	// +listType=set
	AllowedRightsizingModes []RightsizingMode `json:"allowedRightsizingModes,omitempty"`

	// AllowedUpdateStrategies lists the rightsizing update strategies that WorkloadClasses may use with the
	// Auto mode. When unset or empty, all update strategies are allowed.
	// +optional
	// +listType=set
	AllowedUpdateStrategies []RightsizingUpdateStrategy `json:"allowedUpdateStrategies,omitempty"`

	// MaxReplicas is the upper limit on the autoscaling replicas.max of any WorkloadClass.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxReplicas *int32 `json:"maxReplicas,omitempty"`

	// AllowedAutoscalingProfiles lists the autoscaling profiles that WorkloadClasses may use.
	// When unset or empty, all profiles are allowed.
	// +optional
	// +listType=set
	AllowedAutoscalingProfiles []AutoscalingProfile `json:"allowedAutoscalingProfiles,omitempty"`

	// AllowedMetricTypes lists the autoscaling metric types that WorkloadClasses may use.
	// When unset or empty, all metric types other than Object are allowed.
	// +optional
	// +listType=set
	// +kubebuilder:validation:items:Enum=Resource;ContainerResource;Pods;External
	AllowedMetricTypes []autoscalingv2.MetricSourceType `json:"allowedMetricTypes,omitempty"`

	// ContainerBounds is the widest range that initial resources and rightsizing may use. The
	// initialResources.requests and rightsizing.bounds of a WorkloadClass must sit inside it. It applies
	// to rightsizing when a WorkloadClass sets no bounds.
	// +optional
	ContainerBounds *ContainerBounds `json:"containerBounds,omitempty"`
}
