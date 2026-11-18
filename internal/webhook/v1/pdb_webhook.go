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

	policyv1 "k8s.io/api/policy/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

var pdblog = logf.Log.WithName("poddisruptionbudget-resource")

// SetupPodDisruptionBudgetWebhookWithManager registers the validating webhook for user-authored
// PodDisruptionBudgets in the manager.
func SetupPodDisruptionBudgetWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &policyv1.PodDisruptionBudget{}).
		WithValidator(&PodDisruptionBudgetCustomValidator{Client: mgr.GetClient()}).
		WithValidatorCustomPath("/validate-policy-v1-poddisruptionbudget").
		Complete()
}

// +kubebuilder:webhook:path=/validate-policy-v1-poddisruptionbudget,mutating=false,failurePolicy=ignore,sideEffects=None,groups=policy,resources=poddisruptionbudgets,verbs=create;update,versions=v1,name=vpoddisruptionbudget-v1.kb.io,admissionReviewVersions=v1

// PodDisruptionBudgetCustomValidator validates user-authored PodDisruptionBudgets against cluster
// WorkloadClassGuardrail UserPDBPolicy rules.
type PodDisruptionBudgetCustomValidator struct {
	Client client.Client
}

// ValidateCreate implements webhook.CustomValidator for PodDisruptionBudget creation.
func (v *PodDisruptionBudgetCustomValidator) ValidateCreate(ctx context.Context, pdb *policyv1.PodDisruptionBudget) (admission.Warnings, error) {
	return v.validate(ctx, pdb)
}

// ValidateUpdate implements webhook.CustomValidator for PodDisruptionBudget updates.
func (v *PodDisruptionBudgetCustomValidator) ValidateUpdate(ctx context.Context, _, newPDB *policyv1.PodDisruptionBudget) (admission.Warnings, error) {
	return v.validate(ctx, newPDB)
}

// ValidateDelete implements webhook.CustomValidator for PodDisruptionBudget deletion.
func (v *PodDisruptionBudgetCustomValidator) ValidateDelete(_ context.Context, _ *policyv1.PodDisruptionBudget) (admission.Warnings, error) {
	return nil, nil
}

func (v *PodDisruptionBudgetCustomValidator) validate(ctx context.Context, pdb *policyv1.PodDisruptionBudget) (admission.Warnings, error) {
	if pdb == nil || utils.IsWorkloadClassManagedPDB(pdb) {
		return nil, nil
	}

	guardrails := &workloadsv1.WorkloadClassGuardrailList{}
	if err := v.Client.List(ctx, guardrails); err != nil {
		pdblog.Error(err, "Failed to list WorkloadClassGuardrails while validating PodDisruptionBudget", "name", pdb.Name, "namespace", pdb.Namespace)
		return nil, nil
	}

	mode, _, _ := utils.EffectiveUserPDBPolicy(guardrails.Items)
	if mode == workloadsv1.UserPDBModeAllow {
		return nil, nil
	}

	existing := &policyv1.PodDisruptionBudgetList{}
	if err := v.Client.List(ctx, existing, client.InNamespace(pdb.Namespace)); err != nil {
		pdblog.Error(err, "Failed to list PodDisruptionBudgets while validating PodDisruptionBudget", "name", pdb.Name, "namespace", pdb.Namespace)
		return nil, nil
	}

	mode, violations := utils.ValidateUserPDB(pdb, existing.Items, guardrails.Items)
	if len(violations) == 0 {
		return nil, nil
	}

	switch mode {
	case workloadsv1.UserPDBModeEnforce:
		return nil, fmt.Errorf("PodDisruptionBudget %s/%s violates cluster WorkloadClassGuardrail userPDBPolicy: %s",
			pdb.Namespace, pdb.Name, strings.Join(violations, "; "))
	case workloadsv1.UserPDBModeWarn:
		warnings := make(admission.Warnings, 0, len(violations))
		for _, violation := range violations {
			warnings = append(warnings, fmt.Sprintf("PodDisruptionBudget violates cluster WorkloadClassGuardrail userPDBPolicy: %s", violation))
		}
		return warnings, nil
	default:
		return nil, nil
	}
}
