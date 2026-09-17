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
	"regexp"

	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// nolint:unused
// log is for logging in this package.
var workloadclassguardraillog = logf.Log.WithName("workloadclassguardrail-resource")

// SetupWorkloadClassGuardrailWebhookWithManager registers the webhook for WorkloadClassGuardrail in the manager.
func SetupWorkloadClassGuardrailWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &workloadsv1.WorkloadClassGuardrail{}).
		WithValidator(&WorkloadClassGuardrailCustomValidator{Client: mgr.GetClient()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-workloads-gke-io-v1-workloadclassguardrail,mutating=false,failurePolicy=fail,sideEffects=None,groups=workloads.gke.io,resources=workloadclassguardrails,verbs=create;update,versions=v1,name=vworkloadclassguardrail-v1.kb.io,admissionReviewVersions=v1

// WorkloadClassGuardrailCustomValidator struct is responsible for validating the WorkloadClassGuardrail resource
// when it is created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type WorkloadClassGuardrailCustomValidator struct {
	Client client.Client
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClassGuardrail.
func (v *WorkloadClassGuardrailCustomValidator) ValidateCreate(ctx context.Context, obj *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	workloadclassguardraillog.Info("Validation for WorkloadClassGuardrail upon creation", "name", obj.GetName())

	if err := utils.WeekdaysValid(obj.Spec.Constraints.Disruption.AllowedDisruptionDays); err != nil {
		return []string{err.Error()}, err
	}

	if warnings, err := v.validatePlacement(obj); err != nil {
		return warnings, err
	}

	return v.validateAgainstWorkloadClasses(ctx, obj)
}

var percentageRegex = regexp.MustCompile(`^(100|[1-9]?[0-9])%$`)

// validatePlacement validates the guardrail's Placement field.
func (v *WorkloadClassGuardrailCustomValidator) validatePlacement(obj *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	return v.validateSpotPlacement(obj)
}

func (v *WorkloadClassGuardrailCustomValidator) validateSpotPlacement(obj *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	sp := obj.Spec.Constraints.Placement.SpotPlacement

	switch sp.EnforcementMode {
	case "", workloadsv1.EnforcementAllowed, workloadsv1.EnforcementRequired, workloadsv1.EnforcementForbidden:
	default:
		err := fmt.Errorf("invalid enforcementMode %q: must be Allowed, Required, or Forbidden", sp.EnforcementMode)
		return []string{err.Error()}, err
	}

	if sp.MinSpotRatio != "" {
		if !percentageRegex.MatchString(sp.MinSpotRatio) {
			err := fmt.Errorf("invalid minSpotRatio %q: must be a percentage between 0%% and 100%%", sp.MinSpotRatio)
			return []string{err.Error()}, err
		}
		if sp.EnforcementMode == workloadsv1.EnforcementForbidden {
			err := fmt.Errorf("minSpotRatio cannot be set when enforcementMode is Forbidden")
			return []string{err.Error()}, err
		}
		if sp.EnforcementMode == workloadsv1.EnforcementRequired && sp.MinSpotRatio == "0%" {
			err := fmt.Errorf("minSpotRatio cannot be 0%% when enforcementMode is Required")
			return []string{err.Error()}, err
		}
	}

	if warnings, err := v.validateFallback(obj); err != nil {
		return warnings, err
	}

	if warnings, err := v.validateReversion(obj); err != nil {
		return warnings, err
	}

	return nil, nil
}

func (v *WorkloadClassGuardrailCustomValidator) validateFallback(obj *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	sp := obj.Spec.Constraints.Placement.SpotPlacement
	fb := sp.Fallback

	// allowFallbackToOnDemand default value is `true`
	allowFallback := fb.AllowFallbackToOnDemand == nil || *fb.AllowFallbackToOnDemand

	if sp.EnforcementMode == workloadsv1.EnforcementForbidden && !allowFallback {
		err := fmt.Errorf("allowFallbackToOnDemand cannot be false when enforcementMode is Forbidden")
		return []string{err.Error()}, err
	}

	if fb.MaxFallbackRatio != nil {
		if !allowFallback {
			err := fmt.Errorf("maxFallbackRatio cannot be set when allowFallbackToOnDemand is false")
			return []string{err.Error()}, err
		}
		if sp.EnforcementMode == workloadsv1.EnforcementForbidden {
			err := fmt.Errorf("maxFallbackRatio cannot be set when enforcementMode is Forbidden")
			return []string{err.Error()}, err
		}
		switch fb.MaxFallbackRatio.Type {
		case intstr.Int:
			if fb.MaxFallbackRatio.IntVal < 0 {
				err := fmt.Errorf("maxFallbackRatio integer value must be non-negative, got %d", fb.MaxFallbackRatio.IntVal)
				return []string{err.Error()}, err
			}
		case intstr.String:
			if !percentageRegex.MatchString(fb.MaxFallbackRatio.StrVal) {
				err := fmt.Errorf("maxFallbackRatio string value %q must be a percentage between 0%% and 100%%", fb.MaxFallbackRatio.StrVal)
				return []string{err.Error()}, err
			}
		}
	}

	return nil, nil
}

func (v *WorkloadClassGuardrailCustomValidator) validateReversion(obj *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	sp := obj.Spec.Constraints.Placement.SpotPlacement
	rev := sp.Reversion
	if rev == nil {
		return nil, nil
	}

	hasReversionConfig := rev.RequiredReversionAction != "" || rev.MaxFallbackDuration != nil
	if !hasReversionConfig {
		return nil, nil
	}

	if sp.EnforcementMode == workloadsv1.EnforcementForbidden {
		err := fmt.Errorf("reversion cannot be configured when enforcementMode is Forbidden")
		return []string{err.Error()}, err
	}

	allowFallback := sp.Fallback.AllowFallbackToOnDemand == nil || *sp.Fallback.AllowFallbackToOnDemand
	if !allowFallback {
		err := fmt.Errorf("reversion cannot be configured when allowFallbackToOnDemand is false")
		return []string{err.Error()}, err
	}

	switch rev.RequiredReversionAction {
	case "", workloadsv1.ReversionActionActive, workloadsv1.ReversionActionLazy, workloadsv1.ReversionActionNone:
	default:
		err := fmt.Errorf("invalid requiredReversionAction %q: must be Active, Lazy, or None", rev.RequiredReversionAction)
		return []string{err.Error()}, err
	}

	if rev.MaxFallbackDuration != nil {
		if rev.MaxFallbackDuration.Duration <= 0 {
			err := fmt.Errorf("maxFallbackDuration must be greater than 0")
			return []string{err.Error()}, err
		}
		if rev.RequiredReversionAction == workloadsv1.ReversionActionNone {
			err := fmt.Errorf("maxFallbackDuration cannot be set when requiredReversionAction is None")
			return []string{err.Error()}, err
		}
	}

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClassGuardrail.
func (v *WorkloadClassGuardrailCustomValidator) ValidateUpdate(ctx context.Context, _, newObj *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	workloadclassguardraillog.Info("Validation for WorkloadClassGuardrail upon update", "name", newObj.GetName())

	if err := utils.WeekdaysValid(newObj.Spec.Constraints.Disruption.AllowedDisruptionDays); err != nil {
		return []string{err.Error()}, err
	}

	if warnings, err := v.validatePlacement(newObj); err != nil {
		return warnings, err
	}

	return v.validateAgainstWorkloadClasses(ctx, newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClassGuardrail.
func (v *WorkloadClassGuardrailCustomValidator) ValidateDelete(_ context.Context, obj *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	workloadclassguardraillog.Info("Validation for WorkloadClassGuardrail upon deletion", "name", obj.GetName())
	// No validation on Delete
	return nil, nil
}

func (v *WorkloadClassGuardrailCustomValidator) validateAgainstWorkloadClasses(ctx context.Context, g *workloadsv1.WorkloadClassGuardrail) ([]string, error) {
	// Fetch all WorkloadClasses to see if this new/updated guardrail invalidates any of them
	wcs := &workloadsv1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs); err != nil {
		return []string{fmt.Sprintf("failed to list WorkloadClasses: %v", err)}, err
	}

	// For each WorkloadClass, check if it violates the new guardrail
	blockingWorkloadClasses := map[string][]string{}
	for _, wc := range wcs.Items {
		if violations := v.violatesGuardrail(&wc, g); len(violations) > 0 {
			workloadclassguardraillog.Info("Guardrail rejected", "violations", violations, "workloadclass", wc.Name)
			blockingWorkloadClasses[wc.Name] = violations
		}
	}

	if len(blockingWorkloadClasses) > 0 {
		err := fmt.Errorf("guardrail is too restrictive, would invalidate WorkloadClass(es): %v", blockingWorkloadClasses)
		return []string{err.Error()}, err
	}

	workloadclassguardraillog.Info("WorkloadClassGuardrail is valid for all WorkloadClasses", "name", g.GetName())
	return nil, nil
}

// violatesGuardrail checks if a WorkloadClass violates the given WorkloadClassGuardrail.
func (v *WorkloadClassGuardrailCustomValidator) violatesGuardrail(wc *workloadsv1.WorkloadClass, g *workloadsv1.WorkloadClassGuardrail) []string {
	var (
		allowedDisruptionDays        = g.Spec.Constraints.Disruption.AllowedDisruptionDays
		maxAllowedWindows            = g.Spec.Constraints.Disruption.MaxAllowedWindows
		maxNonDisruptionDurationDays = g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays
		violations                   = []string{}
	)

	if len(allowedDisruptionDays) > 0 {
		for _, dw := range wc.Spec.DisruptionPolicy.AllowedDisruptionWindows {
			if !utils.IsSubset(dw.DaysOfWeek, allowedDisruptionDays) {
				violations = append(violations, fmt.Sprintf("guardrail limits disruption days to %v, but existing WorkloadClass '%s' requires %v", allowedDisruptionDays, wc.Name, dw.DaysOfWeek))
			}
		}
	}

	if maxAllowedWindows > 0 && len(wc.Spec.DisruptionPolicy.AllowedDisruptionWindows) > int(maxAllowedWindows) {
		violations = append(violations, fmt.Sprintf("guardrail limits number AllowedDisruptionWindows to %d, but existing WorkloadClass '%s' requires %d", maxAllowedWindows, wc.Name, len(wc.Spec.DisruptionPolicy.AllowedDisruptionWindows)))
	}

	if maxNonDisruptionDurationDays > 0 && wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays > maxNonDisruptionDurationDays {
		violations = append(violations, fmt.Sprintf("guardrail limits MaxNonDisruptionDurationDays to %d, but existing WorkloadClass '%s' requires %d", maxNonDisruptionDurationDays, wc.Name, wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays))
	}

	violations = append(violations, utils.ValidatePlacementAgainstGuardrail(wc, g)...)

	return violations
}
