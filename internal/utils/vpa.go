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
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// VPAGroupVersionKind is the kind of the upstream VerticalPodAutoscaler API.
//
// VerticalPodAutoscaler is an add-on with its own release cycle rather than a core Kubernetes API, so its
// objects are handled as unstructured data. This keeps the manager free of a compile-time dependency on a
// particular VPA version.
var VPAGroupVersionKind = schema.GroupVersionKind{Group: "autoscaling.k8s.io", Version: "v1", Kind: "VerticalPodAutoscaler"}

const (
	// VPAUpdateModeOff makes the VPA publish recommendations without applying them.
	VPAUpdateModeOff = "Off"
	// VPAUpdateModeInitial makes the VPA apply recommendations only when Pods are created.
	VPAUpdateModeInitial = "Initial"
	// VPAUpdateModeRecreate makes the VPA apply recommendations by evicting Pods.
	VPAUpdateModeRecreate = "Recreate"
	// VPAUpdateModeInPlaceOrRecreate makes the VPA resize Pods in place when possible, and evict them
	// otherwise. It needs a VPA version and configuration that support it.
	VPAUpdateModeInPlaceOrRecreate = "InPlaceOrRecreate"

	vpaContainerModeOff  = "Off"
	vpaAllContainersName = "*"
)

// VPACapabilities describes what the VerticalPodAutoscaler installation and the platform support.
type VPACapabilities struct {
	// InPlaceUnsupported is true if the VPA installation rejects the InPlaceOrRecreate update mode. The
	// InPlacePreferred update strategy then falls back to Recreate.
	InPlaceUnsupported bool

	// CoordinatedResources are the resources for which the platform coordinates horizontal scaling on
	// utilization with rightsizing, so that both can act on the same resource.
	CoordinatedResources []corev1.ResourceName
}

// RightsizedResources returns the resources that rightsizing controls on the workloads of the WorkloadClass,
// and the resources it is asked to control but leaves alone.
//
// Rightsizing changes the request of a container, and scaling on utilization measures usage against that
// request, so the two would fight over a resource. A resource whose utilization the autoscaling metrics
// target is therefore left out of rightsizing, unless the platform coordinates the two for that resource.
//
// Both lists are in a stable order (cpu, then memory). When rightsizing lists no resources, it controls
// both cpu and memory.
func RightsizedResources(rm *workloadsv1.ResourceManagement, coordinated []corev1.ResourceName) (controlled, skipped []corev1.ResourceName) {
	if rm == nil || rm.Rightsizing == nil {
		return nil, nil
	}
	var contended []corev1.ResourceName
	if rm.Autoscaling != nil {
		contended = UtilizationTargetedResources(rm.Autoscaling.Metrics)
	}
	for _, name := range managedResources {
		if len(rm.Rightsizing.Resources) > 0 && !slices.Contains(rm.Rightsizing.Resources, workloadsv1.RightsizedResource(name)) {
			continue
		}
		if slices.Contains(contended, name) && !slices.Contains(coordinated, name) {
			skipped = append(skipped, name)
			continue
		}
		controlled = append(controlled, name)
	}
	return controlled, skipped
}

// VPAUpdateMode returns the VPA update mode for the rightsizing mode and update strategy, or an empty string
// if no VPA should exist (rightsizing is not configured, or its mode is Off).
//
//	Mode                             Update mode
//	Recommend                        Off
//	OnCreate                         Initial
//	Auto with InPlacePreferred       InPlaceOrRecreate (Recreate if in-place is unsupported)
//	Auto with Recreate               Recreate
func VPAUpdateMode(r *workloadsv1.Rightsizing, inPlaceUnsupported bool) string {
	switch EffectiveRightsizingMode(r) {
	case workloadsv1.RightsizingModeRecommend:
		return VPAUpdateModeOff
	case workloadsv1.RightsizingModeOnCreate:
		return VPAUpdateModeInitial
	case workloadsv1.RightsizingModeAuto:
		if EffectiveUpdateStrategy(r) == workloadsv1.RightsizingUpdateStrategyInPlacePreferred && !inPlaceUnsupported {
			return VPAUpdateModeInPlaceOrRecreate
		}
		return VPAUpdateModeRecreate
	default:
		return ""
	}
}

// BuildVPA returns the VerticalPodAutoscaler that the rightsizing of the WorkloadClass asks for on the
// workload, or nil if no VPA should exist: the WorkloadClass does not set rightsizing, its mode is Off, or
// every resource it would control is left to horizontal scaling (see RightsizedResources).
//
// The result holds the name, namespace, labels and spec of the managed VPA. Ownership is set by the caller.
//
//   - targetRef is the workload.
//   - updatePolicy.updateMode comes from the mode and update strategy (see VPAUpdateMode).
//   - The "*" container policy controls the rightsized resources within the bounds. A bound that the
//     WorkloadClass does not set falls back to the effective guardrail containerBounds.
//   - Each excluded container gets a container policy with mode Off.
//
// controlledValues is left to the VPA default (RequestsAndLimits), which keeps limits proportional to
// requests when the VPA resizes a container.
func BuildVPA(wc *workloadsv1.WorkloadClass, w WorkloadRef, effective EffectiveResourceManagement, capabilities VPACapabilities) *unstructured.Unstructured {
	if wc == nil || wc.Spec.ResourceManagement == nil {
		return nil
	}
	rs := wc.Spec.ResourceManagement.Rightsizing
	updateMode := VPAUpdateMode(rs, capabilities.InPlaceUnsupported)
	if updateMode == "" {
		return nil
	}
	controlled, _ := RightsizedResources(wc.Spec.ResourceManagement, capabilities.CoordinatedResources)
	if len(controlled) == 0 {
		return nil
	}

	controlledResources := make([]any, 0, len(controlled))
	for _, name := range controlled {
		controlledResources = append(controlledResources, string(name))
	}
	allContainers := map[string]any{
		"containerName":       vpaAllContainersName,
		"controlledResources": controlledResources,
	}
	if minAllowed := vpaAllowed(controlled, boundsMin(rs.Bounds), effective.ContainerBounds.Min); len(minAllowed) > 0 {
		allContainers["minAllowed"] = minAllowed
	}
	if maxAllowed := vpaAllowed(controlled, boundsMax(rs.Bounds), effective.ContainerBounds.Max); len(maxAllowed) > 0 {
		allContainers["maxAllowed"] = maxAllowed
	}

	containerPolicies := []any{allContainers}
	excluded := slices.Clone(rs.ExcludeContainers)
	slices.Sort(excluded)
	for _, name := range slices.Compact(excluded) {
		containerPolicies = append(containerPolicies, map[string]any{
			"containerName": name,
			"mode":          vpaContainerModeOff,
		})
	}

	vpa := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"targetRef": map[string]any{
				"apiVersion": WorkloadAPIVersion,
				"kind":       w.Kind,
				"name":       w.Name,
			},
			"updatePolicy":   map[string]any{"updateMode": updateMode},
			"resourcePolicy": map[string]any{"containerPolicies": containerPolicies},
		},
	}}
	vpa.SetGroupVersionKind(VPAGroupVersionKind)
	vpa.SetName(ManagedAutoscalerName(w.Kind, w.Name))
	vpa.SetNamespace(w.Namespace)
	vpa.SetLabels(ManagedAutoscalerLabels(wc))
	return vpa
}

func boundsMin(b *workloadsv1.ContainerBounds) corev1.ResourceList {
	if b == nil {
		return nil
	}
	return b.Min
}

func boundsMax(b *workloadsv1.ContainerBounds) corev1.ResourceList {
	if b == nil {
		return nil
	}
	return b.Max
}

// vpaAllowed returns the minAllowed or maxAllowed of a VPA container policy for the controlled resources:
// the bound of the WorkloadClass, or the guardrail bound for a resource that the WorkloadClass does not bound.
func vpaAllowed(controlled []corev1.ResourceName, classBound, guardrailBound corev1.ResourceList) map[string]any {
	allowed := map[string]any{}
	for _, name := range controlled {
		if q, ok := classBound[name]; ok {
			allowed[string(name)] = q.String()
		} else if q, ok := guardrailBound[name]; ok {
			allowed[string(name)] = q.String()
		}
	}
	return allowed
}

// VPAUpdateModeOf returns the update mode of a VerticalPodAutoscaler, or an empty string if it sets none.
func VPAUpdateModeOf(vpa *unstructured.Unstructured) string {
	if vpa == nil {
		return ""
	}
	mode, _, _ := unstructured.NestedString(vpa.Object, "spec", "updatePolicy", "updateMode")
	return mode
}

// VPATargetsWorkload reports whether the targetRef of a VerticalPodAutoscaler points at the workload.
func VPATargetsWorkload(vpa *unstructured.Unstructured, w WorkloadRef) bool {
	if vpa == nil {
		return false
	}
	apiVersion, _, _ := unstructured.NestedString(vpa.Object, "spec", "targetRef", "apiVersion")
	kind, _, _ := unstructured.NestedString(vpa.Object, "spec", "targetRef", "kind")
	name, _, _ := unstructured.NestedString(vpa.Object, "spec", "targetRef", "name")
	return ScaleTargetsWorkload(apiVersion, kind, name, w)
}

// ApplyVPASpec copies the fields that BuildVPA manages (targetRef, updatePolicy.updateMode and
// resourcePolicy.containerPolicies) from the desired VPA into an existing one, leaving any other field of
// its spec untouched.
func ApplyVPASpec(existing, desired *unstructured.Unstructured) error {
	for _, path := range [][]string{
		{"spec", "targetRef"},
		{"spec", "updatePolicy", "updateMode"},
		{"spec", "resourcePolicy", "containerPolicies"},
	} {
		value, found, err := unstructured.NestedFieldCopy(desired.Object, path...)
		if err != nil {
			return err
		}
		if !found {
			unstructured.RemoveNestedField(existing.Object, path...)
			continue
		}
		if err := unstructured.SetNestedField(existing.Object, value, path...); err != nil {
			return err
		}
	}
	return nil
}

// ParseCoordinatedResources parses a comma-separated list of the resources (cpu, memory) for which the
// platform coordinates horizontal scaling on utilization with rightsizing. It returns them without
// duplicates in a stable order (cpu, then memory). An empty string means that no resource is coordinated.
func ParseCoordinatedResources(value string) ([]corev1.ResourceName, error) {
	var requested []corev1.ResourceName
	for part := range strings.SplitSeq(value, ",") {
		name := corev1.ResourceName(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		if !slices.Contains(managedResources, name) {
			return nil, fmt.Errorf("unsupported resource %q: must be cpu or memory", name)
		}
		requested = append(requested, name)
	}
	var resources []corev1.ResourceName
	for _, name := range managedResources {
		if slices.Contains(requested, name) {
			resources = append(resources, name)
		}
	}
	return resources, nil
}
