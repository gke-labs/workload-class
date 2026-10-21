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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/intstr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// fallbackBudgetExhausted reports whether admitting the incoming Pod with FallbackToOnDemand would exceed a
// guardrail maxFallbackRatio. When it does, the caller downgrades the Pod to strict Spot (Fail) so it stays
// Pending during a stockout instead of adding to the On-Demand fallback footprint.
//
// The budget is only enforced once the workload is in fallback (InFallback=True, or a Spot-targeted Pod is
// already running on a non-Spot node). Outside a fallback there is nothing to budget, and enforcing it would
// pin small workloads to strict Spot for no reason (e.g. a 1-Pod workload under a 25% budget).
//
// Every WorkloadClassGuardrail that sets maxFallbackRatio for gke-spot-placement applies; the Pod is admitted
// with fallback only if it fits within all of them. Guardrails with unparsable parameters are skipped; the
// WorkloadClassGuardrail controller reports those.
func (d *PodPlacementDefaulter) fallbackBudgetExhausted(ctx context.Context, pod *corev1.Pod, wc *workloadsv1.WorkloadClass, getCounts func() (existingPodCounts, error)) bool {
	log := podlog.WithValues("pod", podName(pod), "namespace", pod.Namespace, "workloadClass", wc.Name)
	ctx = logf.IntoContext(ctx, log)

	budgets, err := d.fallbackBudgets(ctx)
	if err != nil {
		log.Error(err, "Failed to read guardrail fallback budgets; not enforcing maxFallbackRatio")
		return false
	}
	if len(budgets) == 0 {
		return false
	}

	counts, err := getCounts()
	if err != nil {
		log.Error(err, "Failed to count fallback Pods; not enforcing maxFallbackRatio")
		return false
	}
	fallbackPods, totalPods := counts.fallback, counts.total
	inFallback := fallbackPods > 0 || meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeInFallback)
	if !inFallback {
		return false
	}

	// If this Pod falls back too, the workload would have fallbackPods+1 fallback Pods out of totalPods+1.
	for _, b := range budgets {
		within, err := fallbackWithinBudget(fallbackPods+1, totalPods+1, b.ratio)
		if err != nil {
			log.Error(err, "Skipping unparsable guardrail maxFallbackRatio", "guardrail", b.guardrail)
			continue
		}
		if !within {
			log.Info("On-Demand fallback budget exhausted; placing Pod on strict Spot",
				"guardrail", b.guardrail, "maxFallbackRatio", b.ratio.String(), "fallbackPods", fallbackPods, "totalPods", totalPods)
			if d.Recorder != nil {
				d.Recorder.Eventf(wc, nil, corev1.EventTypeWarning, "FallbackBudgetExhausted", "PodPlacement",
					"Pod %s placed on strict Spot: %d of %d Pods already on On-Demand fallback, guardrail %s allows %s",
					podName(pod), fallbackPods, totalPods, b.guardrail, b.ratio.String())
			}
			return true
		}
	}
	return false
}

// fallbackBudget is a guardrail's maxFallbackRatio together with the guardrail it came from, for logging.
type fallbackBudget struct {
	guardrail string
	ratio     intstr.IntOrString
}

// fallbackBudgets returns the maxFallbackRatio of every gke-spot-placement constraint across all
// WorkloadClassGuardrails.
func (d *PodPlacementDefaulter) fallbackBudgets(ctx context.Context) ([]fallbackBudget, error) {
	guardrails := &workloadsv1.WorkloadClassGuardrailList{}
	if err := d.Client.List(ctx, guardrails); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClassGuardrails: %w", err)
	}

	var budgets []fallbackBudget
	for i := range guardrails.Items {
		g := &guardrails.Items[i]
		if g.Spec.PluginConstraints == nil {
			continue
		}
		for _, c := range g.Spec.PluginConstraints.Placement {
			if c.Name != workloadsv1.PluginNameGKESpotPlacement || c.Parameters == nil || len(c.Parameters.Raw) == 0 {
				continue
			}
			params, err := utils.ParseGKESpotGuardrailParameters(c.Parameters.Raw)
			if err != nil {
				logf.FromContext(ctx).Error(err, "Skipping guardrail with invalid gke-spot-placement parameters", "guardrail", g.Name)
				continue
			}
			if params.Fallback != nil && params.Fallback.MaxFallbackRatio != nil {
				budgets = append(budgets, fallbackBudget{guardrail: g.Name, ratio: *params.Fallback.MaxFallbackRatio})
			}
		}
	}
	return budgets, nil
}

// fallbackWithinBudget reports whether fallbackPods out of totalPods on On-Demand fallback fits within a
// maxFallbackRatio, expressed either as a percentage of the workload's Pods ("25%") or an absolute count (5).
func fallbackWithinBudget(fallbackPods, totalPods int, ratio intstr.IntOrString) (bool, error) {
	switch ratio.Type {
	case intstr.String:
		pct, err := utils.ParsePercentage(ratio.StrVal)
		if err != nil {
			return false, fmt.Errorf("invalid maxFallbackRatio %q: %w", ratio.StrVal, err)
		}
		return fallbackPods*100 <= totalPods*pct, nil
	default:
		if ratio.IntVal < 0 {
			return false, fmt.Errorf("invalid maxFallbackRatio %d: must be non-negative", ratio.IntVal)
		}
		return fallbackPods <= int(ratio.IntVal), nil
	}
}
