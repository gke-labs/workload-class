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
	"strings"

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

	if warnings, err := validateGuardrailSpec(obj); err != nil {
		return warnings, err
	}

	return v.validateAgainstWorkloadClasses(ctx, obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClassGuardrail.
func (v *WorkloadClassGuardrailCustomValidator) ValidateUpdate(ctx context.Context, _, newObj *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	workloadclassguardraillog.Info("Validation for WorkloadClassGuardrail upon update", "name", newObj.GetName())

	if warnings, err := validateGuardrailSpec(newObj); err != nil {
		return warnings, err
	}

	return v.validateAgainstWorkloadClasses(ctx, newObj)
}

// validateGuardrailSpec checks the guardrail on its own, independent of existing WorkloadClasses. It applies the
// same checks as the WorkloadClassGuardrail controller, so invalid guardrails are rejected at admission rather
// than only surfacing later as Validated=False.
func validateGuardrailSpec(g *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	if err := utils.WeekdaysValid(g.Spec.Constraints.Disruption.DaysOfWeek); err != nil {
		return []string{err.Error()}, err
	}

	if violations := utils.ValidatePluginConstraints(g.Spec.PluginConstraints); len(violations) > 0 {
		err := fmt.Errorf("invalid pluginConstraints: %s", strings.Join(violations, "; "))
		return []string{err.Error()}, err
	}

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClassGuardrail.
func (v *WorkloadClassGuardrailCustomValidator) ValidateDelete(_ context.Context, obj *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	workloadclassguardraillog.Info("Validation for WorkloadClassGuardrail upon deletion", "name", obj.GetName())
	// No validation on Delete
	return nil, nil
}

// validateAgainstWorkloadClasses admits the WorkloadClassGuardrail and returns an admission warning for each
// existing WorkloadClass that does not comply with it.
//
// Existing WorkloadClasses never block a guardrail change: otherwise a single non-compliant, namespace-scoped
// WorkloadClass could prevent cluster administrators from creating or updating a cluster-scoped guardrail, for
// example to set emergencyOverride during an incident. Non-compliant WorkloadClasses are instead reported as
// Validated=False on their status by the WorkloadClass controller.
func (v *WorkloadClassGuardrailCustomValidator) validateAgainstWorkloadClasses(ctx context.Context, g *workloadsv1.WorkloadClassGuardrail) (admission.Warnings, error) {
	wcs := &workloadsv1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs); err != nil {
		return []string{fmt.Sprintf("failed to list WorkloadClasses: %v", err)}, err
	}

	var warnings admission.Warnings
	for _, wc := range wcs.Items {
		if violations := utils.GuardrailViolations(&wc, g); len(violations) > 0 {
			workloadclassguardraillog.Info("Existing WorkloadClass violates guardrail", "violations", violations, "workloadclass", wc.Name, "namespace", wc.Namespace)
			warnings = append(warnings, fmt.Sprintf("existing WorkloadClass '%s/%s' does not comply with this guardrail: %s",
				wc.Namespace, wc.Name, strings.Join(violations, "; ")))
		}
	}

	if len(warnings) > 0 {
		return warnings, nil
	}

	workloadclassguardraillog.Info("WorkloadClassGuardrail is valid for all WorkloadClasses", "name", g.GetName())
	return nil, nil
}
