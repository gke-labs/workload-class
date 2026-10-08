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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

var percentagePattern = regexp.MustCompile(`^(100|[1-9]?[0-9])%$`)

// WorkloadClassGuardrailReconciler reconciles a WorkloadClassGuardrail object
type WorkloadClassGuardrailReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *WorkloadClassGuardrailReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	g := &workloadsv1.WorkloadClassGuardrail{}
	if err := r.Get(ctx, req.NamespacedName, g); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	validationCondition := r.validate(ctx, g)
	if meta.SetStatusCondition(&g.Status.Conditions, validationCondition) {
		err = r.Status().Update(ctx, g)
	}

	return ctrl.Result{}, err
}

func (r *WorkloadClassGuardrailReconciler) validate(ctx context.Context, g *workloadsv1.WorkloadClassGuardrail) metav1.Condition {
	log := logf.FromContext(ctx)
	var violations []string
	if err := utils.WeekdaysValid(g.Spec.Constraints.Disruption.AllowedDisruptionDays); err != nil {
		log.Error(err, "validation of AllowedDisruptionDays failed")
		violations = append(violations, err.Error())
	}

	if pluginViolations := validatePluginConstraints(g.Spec.PluginConstraints); len(pluginViolations) > 0 {
		log.Info("validation of PluginConstraints failed", "violations", pluginViolations)
		violations = append(violations, pluginViolations...)
	}

	return condition(g.Generation, violations)
}

func validatePluginConstraints(pc *workloadsv1.PluginConstraints) []string {
	if pc == nil {
		return nil
	}

	var violations []string
	for _, c := range pc.Placement {
		violations = append(violations, validatePlacementPluginConstraint(c)...)
	}
	return violations
}

func validatePlacementPluginConstraint(c workloadsv1.PluginConstraint) []string {
	if strings.TrimSpace(c.Name) == "" {
		return []string{"placement pluginConstraint name must not be empty"}
	}

	if c.Parameters == nil || len(c.Parameters.Raw) == 0 {
		return nil
	}

	var rawObj map[string]any
	if err := json.Unmarshal(c.Parameters.Raw, &rawObj); err != nil {
		return []string{fmt.Sprintf("pluginConstraint %q parameters must be a valid JSON object: %v", c.Name, err)}
	}

	if c.Name == workloadsv1.PluginNameGKESpotPlacement {
		return validateGKESpotGuardrailParameters(c.Parameters.Raw)
	}

	return nil
}

func validateGKESpotGuardrailParameters(raw []byte) []string {
	params, err := utils.ParseGKESpotGuardrailParameters(raw)
	if err != nil {
		return []string{fmt.Sprintf("invalid %s parameters: %v", workloadsv1.PluginNameGKESpotPlacement, err)}
	}

	var violations []string

	switch params.EnforcementMode {
	case "", workloadsv1.AllowedEnforcementMode, workloadsv1.RequiredEnforcementMode, workloadsv1.ForbiddenEnforcementMode:
	default:
		violations = append(violations, fmt.Sprintf("invalid enforcementMode %q: must be one of Allowed, Required, Forbidden", params.EnforcementMode))
	}

	if params.MinSpotRatio != nil && !percentagePattern.MatchString(*params.MinSpotRatio) {
		violations = append(violations, fmt.Sprintf("invalid minSpotRatio %q: must be an integer percentage between 0%% and 100%%", *params.MinSpotRatio))
	}

	if params.Fallback != nil && params.Fallback.MaxFallbackRatio != nil {
		mfr := params.Fallback.MaxFallbackRatio
		switch mfr.Type {
		case intstr.Int:
			if mfr.IntVal < 0 {
				violations = append(violations, fmt.Sprintf("invalid fallback.maxFallbackRatio %d: must be non-negative", mfr.IntVal))
			}
		case intstr.String:
			if !percentagePattern.MatchString(mfr.StrVal) {
				violations = append(violations, fmt.Sprintf("invalid fallback.maxFallbackRatio %q: must be an integer percentage between 0%% and 100%%", mfr.StrVal))
			}
		}
	}

	if params.Reversion != nil {
		if action := params.Reversion.RequiredReversionAction; action != nil {
			switch *action {
			case workloadsv1.ActiveReversionAction, workloadsv1.LazyReversionAction, workloadsv1.NoneReversionAction:
			default:
				violations = append(violations, fmt.Sprintf("invalid reversion.requiredReversionAction %q: must be one of Active, Lazy, None", *action))
			}
		}

		if params.Reversion.MaxFallbackDuration != nil && params.Reversion.MaxFallbackDuration.Duration < 0 {
			violations = append(violations, fmt.Sprintf("invalid reversion.maxFallbackDuration %q: must be non-negative", params.Reversion.MaxFallbackDuration.Duration))
		}
	}

	return violations
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassGuardrailReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClassGuardrail{}).
		Named("workloadclassguardrail").
		Complete(r)
}
