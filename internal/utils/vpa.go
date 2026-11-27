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

package utils

import (
	"fmt"
	"slices"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// AutoscalerCapabilities describes what the cluster's autoscalers support. The controller detects it at startup.
type AutoscalerCapabilities struct {
	// VPAInstalled is true when the VerticalPodAutoscaler API (autoscaling.k8s.io/v1) is served.
	VPAInstalled bool

	// InPlaceOrRecreate is true when the installed VerticalPodAutoscaler accepts the InPlaceOrRecreate update mode
	// (VPA 1.4 or later). Otherwise the InPlacePreferred update strategy falls back to Recreate.
	InPlaceOrRecreate bool

	// CoordinatedResources lists the resources for which the platform coordinates the HorizontalPodAutoscaler and
	// the VerticalPodAutoscaler, so that a workload can be both autoscaled on their utilization and rightsized. On
	// GKE, HPA Rightsizing coordinates CPU.
	CoordinatedResources []corev1.ResourceName
}

// VPAOutcome explains how BuildVPA translated the rightsizing settings of a WorkloadClass.
type VPAOutcome struct {
	// ExcludedResources are the resources left out of rightsizing because autoscaling targets their utilization and
	// the platform does not coordinate them.
	ExcludedResources []corev1.ResourceName

	// InPlaceFallback is true when the InPlacePreferred update strategy was mapped to the Recreate update mode,
	// because the VerticalPodAutoscaler does not support InPlaceOrRecreate.
	InPlaceFallback bool
}

// BuildVPA returns the VerticalPodAutoscaler that the WorkloadClass rightsizing settings describe for a workload,
// and how it was translated. It returns a nil VerticalPodAutoscaler when none should exist: the WorkloadClass has no
// rightsizing, its mode is Off, or no resource is left to rightsize. The caller sets the workload as the controller
// owner.
//
//   - spec.targetRef references the workload (apps/v1).
//   - spec.updatePolicy.updateMode follows the mode and update strategy (see VPAUpdateMode).
//   - The "*" container policy controls the rightsized resources, minus those whose utilization autoscaling
//     targets, unless the platform coordinates them. Its minAllowed and maxAllowed come from bounds, per
//     resource, and else from the effective guardrail containerBounds.
//   - Each excluded container gets a container policy with mode Off.
//
// controlledValues keeps the VerticalPodAutoscaler default (RequestsAndLimits).
func BuildVPA(wc *workloadsv1.WorkloadClass, workload WorkloadRef, effective EffectiveResourceManagement, caps AutoscalerCapabilities) (*vpav1.VerticalPodAutoscaler, VPAOutcome) {
	var outcome VPAOutcome
	if wc == nil || wc.Spec.ResourceManagement == nil || wc.Spec.ResourceManagement.Rightsizing == nil {
		return nil, outcome
	}
	r := wc.Spec.ResourceManagement.Rightsizing
	updateMode, inPlaceFallback, ok := VPAUpdateMode(r, caps)
	if !ok {
		return nil, outcome
	}

	controlled, excluded := RightsizedResources(wc.Spec.ResourceManagement, caps)
	outcome.ExcludedResources = excluded
	if len(controlled) == 0 {
		return nil, outcome
	}
	outcome.InPlaceFallback = inPlaceFallback

	defaultPolicy := vpav1.ContainerResourcePolicy{
		ContainerName:       vpav1.DefaultContainerResourcePolicy,
		ControlledResources: &controlled,
	}
	var classMin, classMax corev1.ResourceList
	if r.Bounds != nil {
		classMin, classMax = r.Bounds.Min, r.Bounds.Max
	}
	defaultPolicy.MinAllowed = effectiveBounds(controlled, classMin, effective.ContainerBounds.Min)
	defaultPolicy.MaxAllowed = effectiveBounds(controlled, classMax, effective.ContainerBounds.Max)

	policies := []vpav1.ContainerResourcePolicy{defaultPolicy}
	off := vpav1.ContainerScalingModeOff
	for _, name := range r.ExcludeContainers {
		policies = append(policies, vpav1.ContainerResourcePolicy{ContainerName: name, Mode: &off})
	}

	return &vpav1.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ManagedAutoscalerName(workload.Kind, workload.Name),
			Namespace: workload.Namespace,
			Labels:    ManagedAutoscalerLabels(wc.Name),
		},
		Spec: vpav1.VerticalPodAutoscalerSpec{
			TargetRef: &autoscalingv1.CrossVersionObjectReference{
				APIVersion: WorkloadAPIVersion,
				Kind:       workload.Kind,
				Name:       workload.Name,
			},
			UpdatePolicy:   &vpav1.PodUpdatePolicy{UpdateMode: &updateMode},
			ResourcePolicy: &vpav1.PodResourcePolicy{ContainerPolicies: policies},
		},
	}, outcome
}

// VPAUpdateMode returns the VerticalPodAutoscaler update mode of the rightsizing settings, whether the
// InPlacePreferred update strategy fell back to Recreate, and false when no VerticalPodAutoscaler should exist
// (mode Off):
//
//	Mode                                 Update mode
//	Recommend (default)                  Off: the VerticalPodAutoscaler only writes recommendations
//	OnCreate                             Initial
//	Auto, updateStrategy InPlacePreferred  InPlaceOrRecreate, or Recreate if the VerticalPodAutoscaler lacks it
//	Auto, updateStrategy Recreate        Recreate
func VPAUpdateMode(r *workloadsv1.Rightsizing, caps AutoscalerCapabilities) (vpav1.UpdateMode, bool, bool) {
	switch RightsizingModeOrDefault(r.Mode) {
	case workloadsv1.RightsizingModeOff:
		return "", false, false
	case workloadsv1.RightsizingModeOnCreate:
		return vpav1.UpdateModeInitial, false, true
	case workloadsv1.RightsizingModeAuto:
		if RightsizingUpdateStrategyOrDefault(r.UpdateStrategy) == workloadsv1.RightsizingUpdateStrategyRecreate {
			return vpav1.UpdateModeRecreate, false, true
		}
		if caps.InPlaceOrRecreate {
			return vpav1.UpdateModeInPlaceOrRecreate, false, true
		}
		return vpav1.UpdateModeRecreate, true, true
	default:
		return vpav1.UpdateModeOff, false, true
	}
}

// RightsizedResources returns the resources that rightsizing controls, and those it leaves out because autoscaling
// targets their utilization and the platform does not coordinate them. Rightsizing changes a container's request,
// and autoscaling on utilization measures usage against that request, so the two would fight. Both lists are in a
// stable order (cpu, memory).
func RightsizedResources(rm *workloadsv1.ResourceManagement, caps AutoscalerCapabilities) (controlled, excluded []corev1.ResourceName) {
	if rm == nil || rm.Rightsizing == nil {
		return nil, nil
	}
	var conflicting []corev1.ResourceName
	if rm.Autoscaling != nil {
		conflicting = UtilizationTargetedResources(rm.Autoscaling.Metrics)
	}
	for _, name := range rightsizedResources {
		if !rightsizes(rm.Rightsizing, name) {
			continue
		}
		if slices.Contains(conflicting, name) && !slices.Contains(caps.CoordinatedResources, name) {
			excluded = append(excluded, name)
			continue
		}
		controlled = append(controlled, name)
	}
	return controlled, excluded
}

// rightsizes reports whether the rightsizing settings include the resource. Unset resources mean both cpu and
// memory, the API default.
func rightsizes(r *workloadsv1.Rightsizing, name corev1.ResourceName) bool {
	if len(r.Resources) == 0 {
		return true
	}
	return slices.Contains(r.Resources, workloadsv1.RightsizedResource(name))
}

// effectiveBounds returns, for each controlled resource, the WorkloadClass bound if it sets one, and else the
// guardrail bound, or nil if neither sets any.
func effectiveBounds(controlled []corev1.ResourceName, class, guardrail corev1.ResourceList) corev1.ResourceList {
	var bounds corev1.ResourceList
	for _, name := range controlled {
		q, ok := class[name]
		if !ok {
			q, ok = guardrail[name]
		}
		if !ok {
			continue
		}
		if bounds == nil {
			bounds = corev1.ResourceList{}
		}
		bounds[name] = q.DeepCopy()
	}
	return bounds
}

// ParseCoordinatedResources parses the value of the --coordinated-hpa-vpa-resources flag: a comma-separated list of
// the resources (cpu, memory) that the platform coordinates between the HorizontalPodAutoscaler and the
// VerticalPodAutoscaler.
func ParseCoordinatedResources(values []string) ([]corev1.ResourceName, error) {
	var names []corev1.ResourceName
	for _, v := range values {
		name := corev1.ResourceName(v)
		if !slices.Contains(rightsizedResources, name) {
			return nil, fmt.Errorf("unsupported resource %q: must be one of %s", v, quotedValues(rightsizedResources))
		}
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	return names, nil
}
