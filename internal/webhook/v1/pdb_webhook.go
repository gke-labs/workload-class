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

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/sets"
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
	return v.validate(ctx, nil, pdb)
}

// ValidateUpdate implements webhook.CustomValidator for PodDisruptionBudget updates.
// Terminating PDBs are always allowed so finalizer removal cannot be blocked. Pre-existing
// violations are downgraded to warnings unless the update introduces a new violation.
func (v *PodDisruptionBudgetCustomValidator) ValidateUpdate(ctx context.Context, oldPDB, newPDB *policyv1.PodDisruptionBudget) (admission.Warnings, error) {
	if newPDB == nil || !newPDB.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	return v.validate(ctx, oldPDB, newPDB)
}

// ValidateDelete implements webhook.CustomValidator for PodDisruptionBudget deletion.
func (v *PodDisruptionBudgetCustomValidator) ValidateDelete(_ context.Context, _ *policyv1.PodDisruptionBudget) (admission.Warnings, error) {
	return nil, nil
}

func (v *PodDisruptionBudgetCustomValidator) validate(ctx context.Context, oldPDB, pdb *policyv1.PodDisruptionBudget) (admission.Warnings, error) {
	if pdb == nil || utils.IsWorkloadClassManagedPDB(pdb) {
		return nil, nil
	}

	guardrails := &workloadsv1.WorkloadClassGuardrailList{}
	if err := v.Client.List(ctx, guardrails); err != nil {
		pdblog.Error(err, "Failed to list WorkloadClassGuardrails while validating PodDisruptionBudget", "name", pdb.Name, "namespace", pdb.Namespace)
		return nil, nil
	}

	decision := utils.EffectiveUserPDBPolicy(guardrails.Items)
	if !decision.IsActive() {
		return nil, nil
	}

	var (
		existingPDBs  []policyv1.PodDisruptionBudget
		namespacePods []corev1.Pod
	)
	if decision.OverlappingMode != workloadsv1.UserPDBModeAllow {
		existing := &policyv1.PodDisruptionBudgetList{}
		if err := v.Client.List(ctx, existing, client.InNamespace(pdb.Namespace)); err != nil {
			pdblog.Error(err, "Failed to list PodDisruptionBudgets while validating PodDisruptionBudget", "name", pdb.Name, "namespace", pdb.Namespace)
			return nil, nil
		}
		existingPDBs = existing.Items

		pods := &corev1.PodList{}
		if err := v.Client.List(ctx, pods, client.InNamespace(pdb.Namespace)); err != nil {
			pdblog.Error(err, "Failed to list Pods while validating PodDisruptionBudget", "name", pdb.Name, "namespace", pdb.Namespace)
			return nil, nil
		}
		namespacePods = pods.Items
	}

	if oldPDB == nil {
		rawWarns, rejects := utils.ValidateUserPDB(pdb, existingPDBs, namespacePods, guardrails.Items)
		return formatPDBResult(pdb, rawWarns, rejects)
	}

	if equality.Semantic.DeepEqual(oldPDB.Spec, pdb.Spec) {
		rawWarns, rejects := utils.ValidateUserPDB(pdb, existingPDBs, namespacePods, guardrails.Items)
		return formatPDBResult(pdb, append(rawWarns, rejects...), nil)
	}

	var warns, rejects []string
	if decision.ZeroDisruptionMode != workloadsv1.UserPDBModeAllow {
		if zero, reason := utils.IsZeroDisruptionPDB(pdb); zero {
			oldZero, _ := utils.IsZeroDisruptionPDB(oldPDB)
			if decision.ZeroDisruptionMode == workloadsv1.UserPDBModeEnforce && !oldZero {
				rejects = append(rejects, reason)
			} else {
				warns = append(warns, reason)
			}
		}
	}
	if decision.OverlappingMode != workloadsv1.UserPDBModeAllow {
		newOverlaps := utils.OverlappingUserPDBNames(pdb, existingPDBs, namespacePods)
		if len(newOverlaps) > 0 {
			msg := fmt.Sprintf(
				"matches active Pod(s) already covered by user PodDisruptionBudget(s) %s; Pods matched by multiple PDBs cannot be evicted",
				utils.QuotedList(newOverlaps),
			)
			oldOverlaps := sets.New(utils.OverlappingUserPDBNames(oldPDB, existingPDBs, namespacePods)...)
			if decision.OverlappingMode == workloadsv1.UserPDBModeEnforce && !oldOverlaps.HasAll(newOverlaps...) {
				rejects = append(rejects, msg)
			} else {
				warns = append(warns, msg)
			}
		}
	}

	return formatPDBResult(pdb, warns, rejects)
}

func formatPDBResult(pdb *policyv1.PodDisruptionBudget, rawWarns, rejects []string) (admission.Warnings, error) {
	var warnings admission.Warnings
	for _, w := range rawWarns {
		warnings = append(warnings, fmt.Sprintf("PodDisruptionBudget violates cluster WorkloadClassGuardrail userPDBPolicy: %s", w))
	}
	if len(rejects) > 0 {
		return warnings, fmt.Errorf("PodDisruptionBudget %s/%s violates cluster WorkloadClassGuardrail userPDBPolicy: %s",
			pdb.Namespace, pdb.Name, strings.Join(rejects, "; "))
	}
	return warnings, nil
}
