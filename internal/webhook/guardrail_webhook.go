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
package webhook

import (
	"context"
	"fmt"
	"net/http"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// GuardrailWebhook handles validation for WorkloadClassGuardrail.
type GuardrailWebhook struct {
	Client  client.Client
	decoder *admission.Decoder
}

// +kubebuilder:webhook:path=/validate-workloadclassguardrail,mutating=false,failurePolicy=fail,sideEffects=None,groups=workloads.gke.io,resources=workloadclassguardrails,verbs=create;update,versions=v1,name=vworkloadclassguardrail.gke.io,admissionReviewVersions=v1
// Handle handles admission requests for WorkloadClassGuardrail.
func (v *GuardrailWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name)
	guardrail := &workloadsv1.WorkloadClassGuardrail{}
	err := (*v.decoder).Decode(req, guardrail)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	log.Info("Validating WorkloadClassGuardrail")

	// Fetch all WorkloadClasses to see if this new/updated guardrail invalidates any of them
	wcs := &workloadsv1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs); err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("failed to list WorkloadClasses: %w", err))
	}

	// For each WorkloadClass, check if it violates the new guardrail
	blockingWorkloadClasses := map[string][]string{}
	for _, wc := range wcs.Items {
		if violations := v.violatesGuardrail(&wc, guardrail); len(violations) > 0 {
			log.Info("Guardrail rejected", "violations", violations, "workloadclass", wc.Name)
			blockingWorkloadClasses[wc.Name] = violations
		}
	}

	if len(blockingWorkloadClasses) > 0 {
		return admission.Denied(fmt.Sprintf("Guardrail is too restrictive, would invalidate WorkloadClass(es): %v", blockingWorkloadClasses))
	}

	return admission.Allowed("Guardrail is valid")
}

// violatesGuardrail checks if a WorkloadClass violates the given WorkloadClassGuardrail.
func (v *GuardrailWebhook) violatesGuardrail(wc *workloadsv1.WorkloadClass, g *workloadsv1.WorkloadClassGuardrail) []string {
	var (
		allowedDisruptionDays        = g.Spec.Constraints.Disruption.AllowedDisruptionDays
		maxAllowedWindows            = g.Spec.Constraints.Disruption.MaxAllowedWindows
		maxNonDisruptionDurationDays = g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays
		violations                   = []string{}
	)

	if len(allowedDisruptionDays) > 0 {
		for _, dw := range wc.Spec.DisruptionPolicy.AllowedDisruptionWindows {
			if utils.IsSubset(dw.DaysOfWeek, allowedDisruptionDays) {
				violations = append(violations, fmt.Sprintf("guardrail limits allowed disruption days to %v, but but existing WorkloadClass '%s' requires %v", allowedDisruptionDays, wc.Name, dw.DaysOfWeek))
			}
		}
	}

	if maxAllowedWindows > 0 && len(wc.Spec.DisruptionPolicy.AllowedDisruptionWindows) > int(maxAllowedWindows) {
		violations = append(violations, fmt.Sprintf("guardrail limits number AllowedDisruptionWindows to %d, but existing WorkloadClass '%s' requires %d", maxAllowedWindows, wc.Name, len(wc.Spec.DisruptionPolicy.AllowedDisruptionWindows)))
	}

	if maxNonDisruptionDurationDays > 0 && wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays > maxNonDisruptionDurationDays {
		violations = append(violations, fmt.Sprintf("guardrail limits MaxNonDisruptionDurationDays to %d, but existing WorkloadClass '%s' requires %d", maxNonDisruptionDurationDays, wc.Name, wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays))
	}

	return violations
}

// InjectDecoder injects the decoder.
func (v *GuardrailWebhook) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}
