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

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// rightsizedResources lists the container resources that rightsizing can size, in a stable order.
var rightsizedResources = []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory}

// allowedClassMetricTypes lists the HorizontalPodAutoscaler metric types that a WorkloadClass may use. Object
// metrics are excluded, because they name one specific object.
var allowedClassMetricTypes = []autoscalingv2.MetricSourceType{
	autoscalingv2.ResourceMetricSourceType,
	autoscalingv2.PodsMetricSourceType,
	autoscalingv2.ContainerResourceMetricSourceType,
	autoscalingv2.ExternalMetricSourceType,
}

// EffectiveResourceManagement is the combination of the resourceManagement constraints of all
// WorkloadClassGuardrails, with the strictest value winning for each constraint.
type EffectiveResourceManagement struct {
	// AllowedRightsizingModes, AllowedUpdateStrategies, AllowedAutoscalingProfiles, and AllowedMetricTypes are the
	// intersection of the lists that guardrails set, in the order of the first guardrail that sets them. A nil
	// list allows every value. An empty, non-nil list allows none, because guardrails set disjoint lists.
	AllowedRightsizingModes    []workloadsv1.RightsizingMode
	AllowedUpdateStrategies    []workloadsv1.RightsizingUpdateStrategy
	AllowedAutoscalingProfiles []workloadsv1.AutoscalingProfile
	AllowedMetricTypes         []autoscalingv2.MetricSourceType

	// MaxReplicas is the lowest maxReplicas that guardrails set, or nil if none sets it.
	MaxReplicas *int32

	// ContainerBounds holds, per resource, the highest minimum and the lowest maximum that guardrails set.
	ContainerBounds workloadsv1.ContainerBounds
}

// EffectiveResourceManagementConstraints combines the resourceManagement constraints of the guardrails into a
// single set, with the strictest value winning, as the disruption constraints do:
//   - The allowed lists are intersected. Guardrails that don't set a list don't restrict it.
//   - MaxReplicas is the minimum of the values that are set.
//   - ContainerBounds.Min is, per resource, the maximum of the minimums, and ContainerBounds.Max the minimum of
//     the maximums.
//
// Disjoint lists, and a maximum below the matching minimum, are left in the result and reported as guardrail
// conflicts by ResourceManagementViolations.
func EffectiveResourceManagementConstraints(guardrails []workloadsv1.WorkloadClassGuardrail) EffectiveResourceManagement {
	var e EffectiveResourceManagement
	for i := range guardrails {
		c := guardrails[i].Spec.Constraints.ResourceManagement
		if c == nil {
			continue
		}
		e.AllowedRightsizingModes = intersectAllowed(e.AllowedRightsizingModes, c.AllowedRightsizingModes)
		e.AllowedUpdateStrategies = intersectAllowed(e.AllowedUpdateStrategies, c.AllowedUpdateStrategies)
		e.AllowedAutoscalingProfiles = intersectAllowed(e.AllowedAutoscalingProfiles, c.AllowedAutoscalingProfiles)
		e.AllowedMetricTypes = intersectAllowed(e.AllowedMetricTypes, c.AllowedMetricTypes)
		if c.MaxReplicas != nil && (e.MaxReplicas == nil || *c.MaxReplicas < *e.MaxReplicas) {
			e.MaxReplicas = new(int32)
			*e.MaxReplicas = *c.MaxReplicas
		}
		if c.ContainerBounds != nil {
			e.ContainerBounds.Min = mergeQuantities(e.ContainerBounds.Min, c.ContainerBounds.Min, 1)
			e.ContainerBounds.Max = mergeQuantities(e.ContainerBounds.Max, c.ContainerBounds.Max, -1)
		}
	}
	return e
}

// intersectAllowed intersects the allowed values seen so far with the values allowed by another guardrail.
// A nil list allows every value; an empty guardrail list doesn't restrict values either.
func intersectAllowed[T comparable](current, allowed []T) []T {
	if len(allowed) == 0 {
		return current
	}
	if current == nil {
		return slices.Clone(allowed)
	}
	return slices.DeleteFunc(current, func(v T) bool { return !slices.Contains(allowed, v) })
}

// mergeQuantities merges the per-resource quantities of a guardrail into the current ones, keeping the greater
// quantity of each resource when sign is 1 and the smaller when sign is -1.
func mergeQuantities(current, other corev1.ResourceList, sign int) corev1.ResourceList {
	for name, q := range other {
		if current == nil {
			current = corev1.ResourceList{}
		}
		if existing, ok := current[name]; !ok || q.Cmp(existing) == sign {
			current[name] = q.DeepCopy()
		}
	}
	return current
}

// ResourceManagementViolations returns a description of each effective guardrail constraint that the
// WorkloadClass resourceManagement violates. It returns nil if the WorkloadClass has no resourceManagement or
// complies with the constraints.
//
// Constraints apply only to the parts of resourceManagement that a WorkloadClass uses: the autoscaling
// constraints when autoscaling is set, the rightsizing mode when rightsizing is set, the update strategy when the
// mode is Auto (the only mode it affects), and the container bounds when the mode is not Off. Guardrail
// conflicts are reported the same way, so a conflict in a constraint that a WorkloadClass doesn't use doesn't
// fail it.
func ResourceManagementViolations(wc *workloadsv1.WorkloadClass, e EffectiveResourceManagement) []string {
	rm := wc.Spec.ResourceManagement
	if rm == nil {
		return nil
	}
	var violations []string
	if a := rm.Autoscaling; a != nil {
		violations = append(violations, autoscalingViolations(a, e)...)
	}
	if r := rm.Rightsizing; r != nil {
		violations = append(violations, rightsizingViolations(r, e)...)
	}
	return violations
}

func autoscalingViolations(a *workloadsv1.Autoscaling, e EffectiveResourceManagement) []string {
	var violations []string
	if e.MaxReplicas != nil && a.Replicas.Max > *e.MaxReplicas {
		violations = append(violations, fmt.Sprintf(
			"`resourceManagement.autoscaling.replicas.max` must be at most %d (guardrail limit), got %d",
			*e.MaxReplicas, a.Replicas.Max))
	}

	profile := AutoscalingProfileOrDefault(a.Profile)
	if v := allowedValueViolation("resourceManagement.autoscaling.profile", "allowedAutoscalingProfiles", "autoscaling profile",
		e.AllowedAutoscalingProfiles, profile); v != "" {
		violations = append(violations, v)
	}

	if e.AllowedMetricTypes != nil {
		if len(e.AllowedMetricTypes) == 0 {
			violations = append(violations, guardrailConflictMessage("allowedMetricTypes", "metric type"))
		} else {
			var disallowed []string
			for _, m := range a.Metrics {
				if !slices.Contains(e.AllowedMetricTypes, m.Type) && !slices.Contains(disallowed, string(m.Type)) {
					disallowed = append(disallowed, string(m.Type))
				}
			}
			if len(disallowed) > 0 {
				violations = append(violations, fmt.Sprintf(
					"`resourceManagement.autoscaling.metrics` must only use metric types %s (guardrail limit), got %s",
					quotedValues(e.AllowedMetricTypes), QuotedList(disallowed)))
			}
		}
	}
	return violations
}

func rightsizingViolations(r *workloadsv1.Rightsizing, e EffectiveResourceManagement) []string {
	var violations []string
	mode := RightsizingModeOrDefault(r.Mode)
	if v := allowedValueViolation("resourceManagement.rightsizing.mode", "allowedRightsizingModes", "rightsizing mode",
		e.AllowedRightsizingModes, mode); v != "" {
		violations = append(violations, v)
	}

	if mode == workloadsv1.RightsizingModeAuto {
		strategy := RightsizingUpdateStrategyOrDefault(r.UpdateStrategy)
		if v := allowedValueViolation("resourceManagement.rightsizing.updateStrategy", "allowedUpdateStrategies", "update strategy",
			e.AllowedUpdateStrategies, strategy); v != "" {
			violations = append(violations, v)
		}
	}

	if mode != workloadsv1.RightsizingModeOff {
		violations = append(violations, containerBoundsViolations(r.Bounds, e.ContainerBounds)...)
	}
	return violations
}

// allowedValueViolation returns a description of how value violates the effective allowed list, or an empty
// string if it complies. An empty, non-nil allowed list is a guardrail conflict.
func allowedValueViolation[T ~string](field, constraint, noun string, allowed []T, value T) string {
	switch {
	case allowed == nil, slices.Contains(allowed, value):
		return ""
	case len(allowed) == 0:
		return guardrailConflictMessage(constraint, noun)
	default:
		return fmt.Sprintf("`%s` must be one of %s (guardrail limit), got '%s'", field, quotedValues(allowed), value)
	}
}

// containerBoundsViolations returns a description of each WorkloadClass rightsizing bound that falls outside the
// effective guardrail container bounds, and of each guardrail conflict where the effective maximum of a resource
// is below its minimum.
func containerBoundsViolations(bounds *workloadsv1.ContainerBounds, limits workloadsv1.ContainerBounds) []string {
	var violations []string
	for _, name := range rightsizedResources {
		limitMin, hasLimitMin := limits.Min[name]
		limitMax, hasLimitMax := limits.Max[name]
		if hasLimitMin && hasLimitMax && limitMax.Cmp(limitMin) < 0 {
			violations = append(violations, fmt.Sprintf(
				"cluster guardrails set `containerBounds.max` below `containerBounds.min` for '%s', so no rightsizing "+
					"bounds can comply; contact your cluster administrator", name))
			continue
		}
		if bounds == nil {
			continue
		}
		for _, side := range []struct {
			field  string
			values corev1.ResourceList
		}{{"min", bounds.Min}, {"max", bounds.Max}} {
			q, ok := side.values[name]
			if !ok {
				continue
			}
			field := fmt.Sprintf("resourceManagement.rightsizing.bounds.%s.%s", side.field, name)
			if hasLimitMin && q.Cmp(limitMin) < 0 {
				violations = append(violations, quantityLimitViolation(field, "at least", limitMin, q))
			}
			if hasLimitMax && q.Cmp(limitMax) > 0 {
				violations = append(violations, quantityLimitViolation(field, "at most", limitMax, q))
			}
		}
	}
	return violations
}

func quantityLimitViolation(field, relation string, limit, got resource.Quantity) string {
	return fmt.Sprintf("`%s` must be %s '%s' (guardrail limit), got '%s'", field, relation, limit.String(), got.String())
}

func guardrailConflictMessage(constraint, noun string) string {
	return fmt.Sprintf("cluster guardrails restrict `%s` to disjoint sets, so no %s can comply; contact your cluster administrator",
		constraint, noun)
}

func quotedValues[T ~string](values []T) string {
	strs := make([]string, 0, len(values))
	for _, v := range values {
		strs = append(strs, string(v))
	}
	return QuotedList(strs)
}

// ResourceManagementSpecViolations returns a description of each problem in the WorkloadClass resourceManagement
// that the CRD schema does not catch. These are mainly HorizontalPodAutoscaler metric rules, checked here so that
// a metric the API server would reject in the HorizontalPodAutoscaler is reported on the WorkloadClass status.
func ResourceManagementSpecViolations(rm *workloadsv1.ResourceManagement) []string {
	if rm == nil {
		return nil
	}
	var violations []string
	if rm.Autoscaling == nil && rm.Rightsizing == nil {
		violations = append(violations, "`resourceManagement` must set `autoscaling`, `rightsizing`, or both")
	}
	if a := rm.Autoscaling; a != nil {
		if a.Replicas.Min != nil && *a.Replicas.Min > a.Replicas.Max {
			violations = append(violations, fmt.Sprintf(
				"`resourceManagement.autoscaling.replicas.min` must not exceed `replicas.max` (%d), got %d",
				a.Replicas.Max, *a.Replicas.Min))
		}
		if len(a.Metrics) == 0 {
			violations = append(violations, "`resourceManagement.autoscaling.metrics` must have at least one metric")
		}
		for i := range a.Metrics {
			violations = append(violations, metricViolations(i, &a.Metrics[i])...)
		}
	}
	if r := rm.Rightsizing; r != nil && r.Bounds != nil {
		for _, name := range rightsizedResources {
			lower, hasLower := r.Bounds.Min[name]
			upper, hasUpper := r.Bounds.Max[name]
			if hasLower && hasUpper && lower.Cmp(upper) > 0 {
				violations = append(violations, fmt.Sprintf(
					"`resourceManagement.rightsizing.bounds.min.%s` must not exceed `bounds.max.%s` ('%s'), got '%s'",
					name, name, upper.String(), lower.String()))
			}
		}
	}
	return violations
}

// metricViolations checks that a metric sets the source of its type, and only that source, and that its target
// type and value fit the metric type, following the HorizontalPodAutoscaler validation rules.
func metricViolations(i int, m *autoscalingv2.MetricSpec) []string {
	path := fmt.Sprintf("resourceManagement.autoscaling.metrics[%d]", i)
	sources := map[autoscalingv2.MetricSourceType]bool{
		autoscalingv2.ObjectMetricSourceType:            m.Object != nil,
		autoscalingv2.PodsMetricSourceType:              m.Pods != nil,
		autoscalingv2.ResourceMetricSourceType:          m.Resource != nil,
		autoscalingv2.ContainerResourceMetricSourceType: m.ContainerResource != nil,
		autoscalingv2.ExternalMetricSourceType:          m.External != nil,
	}
	if !slices.Contains(allowedClassMetricTypes, m.Type) {
		return []string{fmt.Sprintf("`%s.type` must be one of %s, got '%s'", path, quotedValues(allowedClassMetricTypes), m.Type)}
	}

	field := metricSourceField(m.Type)
	if !sources[m.Type] {
		return []string{fmt.Sprintf("`%s.%s` must be set for metric type '%s'", path, field, m.Type)}
	}
	var violations []string
	for t, set := range sources {
		if set && t != m.Type {
			violations = append(violations, fmt.Sprintf("`%s` must only set `%s` for metric type '%s'", path, field, m.Type))
			break
		}
	}

	var target autoscalingv2.MetricTarget
	var allowedTargets []autoscalingv2.MetricTargetType
	switch m.Type {
	case autoscalingv2.ResourceMetricSourceType:
		target = m.Resource.Target
		allowedTargets = []autoscalingv2.MetricTargetType{autoscalingv2.UtilizationMetricType, autoscalingv2.AverageValueMetricType}
	case autoscalingv2.ContainerResourceMetricSourceType:
		target = m.ContainerResource.Target
		allowedTargets = []autoscalingv2.MetricTargetType{autoscalingv2.UtilizationMetricType, autoscalingv2.AverageValueMetricType}
	case autoscalingv2.PodsMetricSourceType:
		target = m.Pods.Target
		allowedTargets = []autoscalingv2.MetricTargetType{autoscalingv2.AverageValueMetricType}
	case autoscalingv2.ExternalMetricSourceType:
		target = m.External.Target
		allowedTargets = []autoscalingv2.MetricTargetType{autoscalingv2.ValueMetricType, autoscalingv2.AverageValueMetricType}
	}
	targetPath := fmt.Sprintf("%s.%s.target", path, field)
	if !slices.Contains(allowedTargets, target.Type) {
		return append(violations, fmt.Sprintf("`%s.type` must be one of %s for metric type '%s', got '%s'",
			targetPath, quotedValues(allowedTargets), m.Type, target.Type))
	}
	switch target.Type {
	case autoscalingv2.UtilizationMetricType:
		if target.AverageUtilization == nil || *target.AverageUtilization <= 0 {
			violations = append(violations, fmt.Sprintf("`%s.averageUtilization` must be greater than 0 when `type` is '%s'", targetPath, target.Type))
		}
	case autoscalingv2.AverageValueMetricType:
		if target.AverageValue == nil || target.AverageValue.Sign() <= 0 {
			violations = append(violations, fmt.Sprintf("`%s.averageValue` must be greater than 0 when `type` is '%s'", targetPath, target.Type))
		}
	case autoscalingv2.ValueMetricType:
		if target.Value == nil || target.Value.Sign() <= 0 {
			violations = append(violations, fmt.Sprintf("`%s.value` must be greater than 0 when `type` is '%s'", targetPath, target.Type))
		}
	}
	return violations
}

// metricSourceField returns the name of the MetricSpec field that holds the source of a metric type.
func metricSourceField(t autoscalingv2.MetricSourceType) string {
	switch t {
	case autoscalingv2.ContainerResourceMetricSourceType:
		return "containerResource"
	case autoscalingv2.PodsMetricSourceType:
		return "pods"
	case autoscalingv2.ExternalMetricSourceType:
		return "external"
	case autoscalingv2.ObjectMetricSourceType:
		return "object"
	default:
		return "resource"
	}
}

// ResourceManagementConstraintViolations returns a description of each problem in the resourceManagement
// constraints of a single WorkloadClassGuardrail: values outside the allowed enums, and a containerBounds.min
// above the matching containerBounds.max. The CRD schema rejects these too; the checks also cover guardrails
// stored before the schema was in place.
func ResourceManagementConstraintViolations(c *workloadsv1.ResourceManagementConstraints) []string {
	if c == nil {
		return nil
	}
	var violations []string
	violations = append(violations, enumViolations("allowedRightsizingModes", c.AllowedRightsizingModes, []workloadsv1.RightsizingMode{
		workloadsv1.RightsizingModeOff, workloadsv1.RightsizingModeRecommend, workloadsv1.RightsizingModeOnCreate, workloadsv1.RightsizingModeAuto,
	})...)
	violations = append(violations, enumViolations("allowedUpdateStrategies", c.AllowedUpdateStrategies, []workloadsv1.RightsizingUpdateStrategy{
		workloadsv1.RightsizingUpdateStrategyInPlacePreferred, workloadsv1.RightsizingUpdateStrategyRecreate,
	})...)
	violations = append(violations, enumViolations("allowedAutoscalingProfiles", c.AllowedAutoscalingProfiles, []workloadsv1.AutoscalingProfile{
		workloadsv1.AutoscalingProfileResponsive, workloadsv1.AutoscalingProfileBalanced, workloadsv1.AutoscalingProfileConservative,
	})...)
	violations = append(violations, enumViolations("allowedMetricTypes", c.AllowedMetricTypes,
		append([]autoscalingv2.MetricSourceType{autoscalingv2.ObjectMetricSourceType}, allowedClassMetricTypes...))...)
	if c.MaxReplicas != nil && *c.MaxReplicas < 1 {
		violations = append(violations, fmt.Sprintf("`resourceManagement.maxReplicas` must be at least 1, got %d", *c.MaxReplicas))
	}
	if b := c.ContainerBounds; b != nil {
		for _, side := range []struct {
			field  string
			values corev1.ResourceList
		}{{"min", b.Min}, {"max", b.Max}} {
			for name := range side.values {
				if !slices.Contains(rightsizedResources, name) {
					violations = append(violations, fmt.Sprintf(
						"`resourceManagement.containerBounds.%s` must only set 'cpu' and 'memory', got '%s'", side.field, name))
				}
			}
		}
		for _, name := range rightsizedResources {
			lower, hasLower := b.Min[name]
			upper, hasUpper := b.Max[name]
			if hasLower && hasUpper && lower.Cmp(upper) > 0 {
				violations = append(violations, fmt.Sprintf(
					"`resourceManagement.containerBounds.min.%s` must not exceed `containerBounds.max.%s` ('%s'), got '%s'",
					name, name, upper.String(), lower.String()))
			}
		}
	}
	return violations
}

func enumViolations[T ~string](field string, values, allowed []T) []string {
	var violations []string
	for _, v := range values {
		if !slices.Contains(allowed, v) {
			violations = append(violations, fmt.Sprintf("`resourceManagement.%s` must only contain %s, got '%s'", field, quotedValues(allowed), v))
		}
	}
	return violations
}

// AutoscalingProfileOrDefault returns the profile, or Balanced (the API default) if it is empty.
func AutoscalingProfileOrDefault(p workloadsv1.AutoscalingProfile) workloadsv1.AutoscalingProfile {
	if p == "" {
		return workloadsv1.AutoscalingProfileBalanced
	}
	return p
}

// RightsizingModeOrDefault returns the mode, or Recommend (the API default) if it is empty.
func RightsizingModeOrDefault(m workloadsv1.RightsizingMode) workloadsv1.RightsizingMode {
	if m == "" {
		return workloadsv1.RightsizingModeRecommend
	}
	return m
}

// RightsizingUpdateStrategyOrDefault returns the update strategy, or InPlacePreferred (the API default) if it is
// empty.
func RightsizingUpdateStrategyOrDefault(s workloadsv1.RightsizingUpdateStrategy) workloadsv1.RightsizingUpdateStrategy {
	if s == "" {
		return workloadsv1.RightsizingUpdateStrategyInPlacePreferred
	}
	return s
}
