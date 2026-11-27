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
	"k8s.io/apimachinery/pkg/util/sets"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	initialResourcesField = "resourceManagement.initialResources"
	autoscalingField      = "resourceManagement.autoscaling"
	rightsizingField      = "resourceManagement.rightsizing"

	guardrailLimit = "guardrail limit"
)

// managedResources are the resources that initial resources and rightsizing can manage, in the order
// they are reported in validation messages.
var managedResources = []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory}

// AllowList is the set of values that guardrails allow for a WorkloadClass field. The zero value allows
// every value.
type AllowList[T ~string] struct {
	restricted bool
	values     sets.Set[T]
}

// restrict narrows the AllowList to the given values. An empty list does not restrict it.
func (a *AllowList[T]) restrict(values []T) {
	if len(values) == 0 {
		return
	}
	allowed := sets.New(values...)
	if !a.restricted {
		a.restricted = true
		a.values = allowed
		return
	}
	a.values = a.values.Intersection(allowed)
}

// Allows reports whether the value is allowed.
func (a AllowList[T]) Allows(value T) bool {
	return !a.restricted || a.values.Has(value)
}

// Conflicting reports whether guardrails restrict the field to disjoint sets, so that no value is allowed.
func (a AllowList[T]) Conflicting() bool {
	return a.restricted && a.values.Len() == 0
}

// Values returns the allowed values in sorted order, or nil if every value is allowed.
func (a AllowList[T]) Values() []string {
	if !a.restricted {
		return nil
	}
	values := make([]string, 0, a.values.Len())
	for v := range a.values {
		values = append(values, string(v))
	}
	slices.Sort(values)
	return values
}

// EffectiveResourceManagement is the combination of the resourceManagement constraints of every
// WorkloadClassGuardrail, with the strictest value of each constraint winning.
type EffectiveResourceManagement struct {
	// RequireInitialResources is true if any guardrail requires initial resources.
	RequireInitialResources bool

	LimitPolicies       AllowList[workloadsv1.LimitPolicy]
	RightsizingModes    AllowList[workloadsv1.RightsizingMode]
	UpdateStrategies    AllowList[workloadsv1.RightsizingUpdateStrategy]
	AutoscalingProfiles AllowList[workloadsv1.AutoscalingProfile]
	MetricTypes         AllowList[autoscalingv2.MetricSourceType]

	// MaxReplicas is the lowest maxReplicas of the guardrails that set it, or nil if none does.
	MaxReplicas *int32

	// ContainerBounds holds, per resource, the highest minimum and the lowest maximum of the guardrails
	// that set them.
	ContainerBounds workloadsv1.ContainerBounds
}

// EffectiveResourceManagementConstraints combines the resourceManagement constraints of the guardrails into
// the effective (strictest) constraints:
//   - requireInitialResources is true if any guardrail sets it.
//   - Each allowed list is the intersection of the lists that are set. An unset list allows every value.
//   - maxReplicas is the minimum of the values that are set.
//   - containerBounds.min is, per resource, the maximum of the minimums, and containerBounds.max is the
//     minimum of the maximums.
//
// Guardrails that cannot be satisfied together are reported by Conflicts.
func EffectiveResourceManagementConstraints(guardrails []workloadsv1.WorkloadClassGuardrail) EffectiveResourceManagement {
	var e EffectiveResourceManagement
	for i := range guardrails {
		c := guardrails[i].Spec.Constraints.ResourceManagement
		if c == nil {
			continue
		}
		e.RequireInitialResources = e.RequireInitialResources || c.RequireInitialResources
		e.LimitPolicies.restrict(c.AllowedLimitPolicies)
		e.RightsizingModes.restrict(c.AllowedRightsizingModes)
		e.UpdateStrategies.restrict(c.AllowedUpdateStrategies)
		e.AutoscalingProfiles.restrict(c.AllowedAutoscalingProfiles)
		e.MetricTypes.restrict(c.AllowedMetricTypes)
		if c.MaxReplicas != nil && (e.MaxReplicas == nil || *c.MaxReplicas < *e.MaxReplicas) {
			v := *c.MaxReplicas
			e.MaxReplicas = &v
		}
		if c.ContainerBounds == nil {
			continue
		}
		for _, r := range managedResources {
			if q, ok := c.ContainerBounds.Min[r]; ok {
				if cur, set := e.ContainerBounds.Min[r]; !set || q.Cmp(cur) > 0 {
					setQuantity(&e.ContainerBounds.Min, r, q)
				}
			}
			if q, ok := c.ContainerBounds.Max[r]; ok {
				if cur, set := e.ContainerBounds.Max[r]; !set || q.Cmp(cur) < 0 {
					setQuantity(&e.ContainerBounds.Max, r, q)
				}
			}
		}
	}
	return e
}

func setQuantity(list *corev1.ResourceList, name corev1.ResourceName, q resource.Quantity) {
	if *list == nil {
		*list = corev1.ResourceList{}
	}
	(*list)[name] = q.DeepCopy()
}

// Conflicts returns a description of each constraint that the guardrails restrict in a way that no
// WorkloadClass can satisfy: allowed lists with an empty intersection, and containerBounds whose minimum is
// above the maximum.
func (e EffectiveResourceManagement) Conflicts() []string {
	var conflicts []string
	for _, c := range []struct {
		conflicting bool
		field, what string
	}{
		{e.LimitPolicies.Conflicting(), "allowedLimitPolicies", "limit policy"},
		{e.RightsizingModes.Conflicting(), "allowedRightsizingModes", "rightsizing mode"},
		{e.UpdateStrategies.Conflicting(), "allowedUpdateStrategies", "update strategy"},
		{e.AutoscalingProfiles.Conflicting(), "allowedAutoscalingProfiles", "autoscaling profile"},
		{e.MetricTypes.Conflicting(), "allowedMetricTypes", "metric type"},
	} {
		if c.conflicting {
			conflicts = append(conflicts, fmt.Sprintf(
				"cluster guardrails restrict `%s` to disjoint sets, so no %s can comply; contact your cluster administrator",
				c.field, c.what))
		}
	}
	for _, r := range managedResources {
		lo, hasMin := e.ContainerBounds.Min[r]
		hi, hasMax := e.ContainerBounds.Max[r]
		if hasMin && hasMax && lo.Cmp(hi) > 0 {
			conflicts = append(conflicts, fmt.Sprintf(
				"cluster guardrails set a `containerBounds` minimum (%s) above the maximum (%s) for %s, so no value can comply; "+
					"contact your cluster administrator", lo.String(), hi.String(), r))
		}
	}
	return conflicts
}

// EffectiveLimitPolicy returns the limit policy of the initial resources, applying the default.
func EffectiveLimitPolicy(ir *workloadsv1.InitialResources) workloadsv1.LimitPolicy {
	if ir == nil || ir.LimitPolicy == "" {
		return workloadsv1.LimitPolicyMemoryEqualsRequest
	}
	return ir.LimitPolicy
}

// EffectiveAutoscalingProfile returns the autoscaling profile, applying the default.
func EffectiveAutoscalingProfile(a *workloadsv1.Autoscaling) workloadsv1.AutoscalingProfile {
	if a == nil || a.Profile == "" {
		return workloadsv1.AutoscalingProfileBalanced
	}
	return a.Profile
}

// EffectiveMinReplicas returns the minimum number of replicas, applying the default.
func EffectiveMinReplicas(a *workloadsv1.Autoscaling) int32 {
	if a == nil || a.Replicas.Min == nil {
		return 1
	}
	return *a.Replicas.Min
}

// EffectiveRightsizingMode returns the rightsizing mode, applying the default. It returns Off when
// rightsizing is not configured.
func EffectiveRightsizingMode(r *workloadsv1.Rightsizing) workloadsv1.RightsizingMode {
	if r == nil {
		return workloadsv1.RightsizingModeOff
	}
	if r.Mode == "" {
		return workloadsv1.RightsizingModeRecommend
	}
	return r.Mode
}

// EffectiveUpdateStrategy returns the rightsizing update strategy, applying the default.
func EffectiveUpdateStrategy(r *workloadsv1.Rightsizing) workloadsv1.RightsizingUpdateStrategy {
	if r == nil || r.UpdateStrategy == "" {
		return workloadsv1.RightsizingUpdateStrategyInPlacePreferred
	}
	return r.UpdateStrategy
}

// ResourceManagementSpecViolations returns a description of each rule that the resourceManagement of a
// WorkloadClass breaks on its own, independent of guardrails.
//
// It covers the rules that the CRD schema cannot express within the API server's CEL cost budget (resource
// lists that only set cpu and memory, positive quantities, minimums that do not exceed maximums, and initial
// requests that sit inside the rightsizing bounds), and acts as defense-in-depth for the rules that the
// schema also checks at admission time.
func ResourceManagementSpecViolations(rm *workloadsv1.ResourceManagement) []string {
	if rm == nil {
		return nil
	}
	var violations []string

	if ir := rm.InitialResources; ir != nil {
		field := initialResourcesField + ".requests"
		violations = append(violations, resourceListViolations(field, ir.Requests, true)...)
		for _, r := range managedResources {
			if _, ok := ir.Requests[r]; !ok {
				violations = append(violations, fmt.Sprintf("`%s` must set %s", field, r))
			}
		}
	}

	if a := rm.Autoscaling; a != nil {
		if minReplicas := EffectiveMinReplicas(a); minReplicas > a.Replicas.Max {
			violations = append(violations, fmt.Sprintf("`%s.replicas.min` must not exceed `replicas.max` (%d), got %d",
				autoscalingField, a.Replicas.Max, minReplicas))
		}
		if len(a.Metrics) == 0 {
			violations = append(violations, fmt.Sprintf("`%s.metrics` must have at least 1 item", autoscalingField))
		}
		for _, m := range a.Metrics {
			if m.Type == autoscalingv2.ObjectMetricSourceType {
				violations = append(violations, fmt.Sprintf(
					"`%s.metrics` must not use 'Object' metrics, because they name one specific object", autoscalingField))
				break
			}
		}
	}

	if rs := rm.Rightsizing; rs != nil {
		violations = append(violations, ContainerBoundsViolations(rightsizingField+".bounds", rs.Bounds)...)
		if rm.InitialResources != nil && rs.Bounds != nil {
			violations = append(violations, outsideBoundsViolations(
				initialResourcesField+".requests", rm.InitialResources.Requests, *rs.Bounds,
				"`"+rightsizingField+".bounds`")...)
		}
	}

	return violations
}

// ContainerBoundsViolations returns a description of each rule that the container bounds break: resources
// other than cpu and memory, negative quantities, and minimums above the matching maximum. The field is the
// path of the bounds used in the descriptions.
func ContainerBoundsViolations(field string, bounds *workloadsv1.ContainerBounds) []string {
	if bounds == nil {
		return nil
	}
	violations := resourceListViolations(field+".min", bounds.Min, false)
	violations = append(violations, resourceListViolations(field+".max", bounds.Max, true)...)
	for _, r := range managedResources {
		lo, hasMin := bounds.Min[r]
		hi, hasMax := bounds.Max[r]
		if hasMin && hasMax && lo.Cmp(hi) > 0 {
			violations = append(violations, fmt.Sprintf("`%s.min` %s must not exceed `%s.max` (%s), got %s",
				field, r, field, hi.String(), lo.String()))
		}
	}
	return violations
}

// resourceListViolations checks that the list only sets cpu and memory, and that its quantities are positive
// (or not negative, if positive is false).
func resourceListViolations(field string, list corev1.ResourceList, positive bool) []string {
	var violations []string

	var unsupported []string
	for name := range list {
		if !slices.Contains(managedResources, name) {
			unsupported = append(unsupported, string(name))
		}
	}
	if len(unsupported) > 0 {
		slices.Sort(unsupported)
		violations = append(violations, fmt.Sprintf("`%s` may only set cpu and memory, got %s", field, QuotedList(unsupported)))
	}

	for _, r := range managedResources {
		q, ok := list[r]
		if !ok {
			continue
		}
		switch sign := q.Sign(); {
		case positive && sign <= 0:
			violations = append(violations, fmt.Sprintf("`%s` %s must be greater than 0, got %s", field, r, q.String()))
		case sign < 0:
			violations = append(violations, fmt.Sprintf("`%s` %s must not be negative, got %s", field, r, q.String()))
		}
	}
	return violations
}

// outsideBoundsViolations returns a description of each value that sits outside the bounds. The limit is the
// name of the bounds used in the descriptions.
func outsideBoundsViolations(field string, values corev1.ResourceList, bounds workloadsv1.ContainerBounds, limit string) []string {
	var violations []string
	for _, r := range managedResources {
		q, ok := values[r]
		if !ok {
			continue
		}
		if lo, ok := bounds.Min[r]; ok && q.Cmp(lo) < 0 {
			violations = append(violations, fmt.Sprintf("`%s` %s must be at least %s (%s), got %s", field, r, lo.String(), limit, q.String()))
		}
		if hi, ok := bounds.Max[r]; ok && q.Cmp(hi) > 0 {
			violations = append(violations, fmt.Sprintf("`%s` %s must be at most %s (%s), got %s", field, r, hi.String(), limit, q.String()))
		}
	}
	return violations
}

// ResourceManagementViolations returns a description of each effective guardrail constraint that the
// resourceManagement of the WorkloadClass violates, including guardrails that cannot be satisfied together.
// It returns an empty slice if the WorkloadClass complies.
//
// Guardrail names are deliberately omitted, as in GuardrailViolations. Defaults are applied to unset
// WorkloadClass fields before they are checked. The update strategy is only checked when the rightsizing
// mode is Auto, because it has no effect in the other modes.
func ResourceManagementViolations(wlc *workloadsv1.WorkloadClass, e EffectiveResourceManagement) []string {
	rm := wlc.Spec.ResourceManagement
	violations := []string{}

	if e.RequireInitialResources && (rm == nil || rm.InitialResources == nil) {
		violations = append(violations, fmt.Sprintf("`%s` must be specified (%s)", initialResourcesField, guardrailLimit))
	}
	if rm == nil {
		return violations
	}
	// Report unsatisfiable guardrails once per WorkloadClass that sets resourceManagement.
	violations = append(violations, e.Conflicts()...)

	if ir := rm.InitialResources; ir != nil {
		violations = appendNotAllowed(violations, initialResourcesField+".limitPolicy", e.LimitPolicies, EffectiveLimitPolicy(ir))
		violations = append(violations, outsideBoundsViolations(
			initialResourcesField+".requests", ir.Requests, e.ContainerBounds, guardrailLimit)...)
	}

	if a := rm.Autoscaling; a != nil {
		if e.MaxReplicas != nil && a.Replicas.Max > *e.MaxReplicas {
			violations = append(violations, fmt.Sprintf("`%s.replicas.max` must be at most %d (%s), got %d",
				autoscalingField, *e.MaxReplicas, guardrailLimit, a.Replicas.Max))
		}
		violations = appendNotAllowed(violations, autoscalingField+".profile", e.AutoscalingProfiles, EffectiveAutoscalingProfile(a))
		if !e.MetricTypes.Conflicting() {
			var notAllowed []string
			for _, m := range a.Metrics {
				if !e.MetricTypes.Allows(m.Type) && !slices.Contains(notAllowed, string(m.Type)) {
					notAllowed = append(notAllowed, string(m.Type))
				}
			}
			if len(notAllowed) > 0 {
				slices.Sort(notAllowed)
				violations = append(violations, fmt.Sprintf("`%s.metrics` must only use the metric types %s (%s), got %s",
					autoscalingField, QuotedList(e.MetricTypes.Values()), guardrailLimit, QuotedList(notAllowed)))
			}
		}
	}

	if rs := rm.Rightsizing; rs != nil {
		mode := EffectiveRightsizingMode(rs)
		violations = appendNotAllowed(violations, rightsizingField+".mode", e.RightsizingModes, mode)
		if mode == workloadsv1.RightsizingModeAuto {
			violations = appendNotAllowed(violations, rightsizingField+".updateStrategy", e.UpdateStrategies, EffectiveUpdateStrategy(rs))
		}
		if rs.Bounds != nil {
			violations = append(violations, outsideBoundsViolations(rightsizingField+".bounds.min", rs.Bounds.Min, e.ContainerBounds, guardrailLimit)...)
			violations = append(violations, outsideBoundsViolations(rightsizingField+".bounds.max", rs.Bounds.Max, e.ContainerBounds, guardrailLimit)...)
		}
	}

	return violations
}

// appendNotAllowed appends a violation if the value is not in the AllowList. Conflicting AllowLists are
// reported separately by Conflicts.
func appendNotAllowed[T ~string](violations []string, field string, allowed AllowList[T], value T) []string {
	if allowed.Conflicting() || allowed.Allows(value) {
		return violations
	}
	return append(violations, fmt.Sprintf("`%s` must be one of %s (%s), got '%s'",
		field, QuotedList(allowed.Values()), guardrailLimit, value))
}

// ResourceManagementConstraintsViolations returns a description of each rule that the resourceManagement
// constraints of a WorkloadClassGuardrail break on their own: a containerBounds that sets resources other
// than cpu and memory, negative quantities, or a minimum above the matching maximum.
func ResourceManagementConstraintsViolations(c *workloadsv1.ResourceManagementConstraints) []string {
	if c == nil {
		return nil
	}
	return ContainerBoundsViolations("constraints.resourceManagement.containerBounds", c.ContainerBounds)
}
