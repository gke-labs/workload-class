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
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	VPA = "VPA"
	CA  = "ClusterAutoscaler"

	VPAServiceAccount = "system:serviceaccount:kube-system:vpa-updater"
	CAServiceAccount  = "system:serviceaccount:kube-system:cluster-autoscaler"
)

// nolint:unused
// log is for logging in this package.
var workloadclasslog = logf.Log.WithName("workloadclass-resource")

// SetupWorkloadClassWebhookWithManager registers the webhook for WorkloadClass in the manager.
func SetupWorkloadClassWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &workloadsv1.WorkloadClass{}).
		WithValidator(&WorkloadClassCustomValidator{}).
		Complete()
}

// NOTE: If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
// +kubebuilder:webhook:path=/validate-workloads-gke-io-v1-workloadclass,mutating=false,failurePolicy=fail,sideEffects=None,groups=workloads.gke.io,resources=workloadclasses,verbs=create;update,versions=v1,name=vworkloadclass-v1.kb.io,admissionReviewVersions=v1

// WorkloadClassCustomValidator struct is responsible for validating the WorkloadClass resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type WorkloadClassCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateCreate(_ context.Context, obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon creation", "name", obj.GetName())

	if err := validateAllowedDisruptions(obj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow); err != nil {
		return []string{err.Error()}, err
	}

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon update", "name", newObj.GetName())

	if err := validateAllowedDisruptions(newObj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow); err != nil {
		return []string{err.Error()}, err
	}

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateDelete(_ context.Context, obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon deletion", "name", obj.GetName())
	// No validation on delete
	return nil, nil
}

// validateAllowedDisruptions validates that strings in the allowedDisruptionsOutsideOfWindowField are either "VPA" or "ClusterAutoscaler"
func validateAllowedDisruptions(allowedDisruptions []string) error {
	if len(allowedDisruptions) == 0 {
		return nil
	}

	allowedIdentities := map[string]struct{}{VPA: {}, CA: {}}
	invalidIdentities := []string{}

	for _, i := range allowedDisruptions {
		if _, ok := allowedIdentities[i]; !ok {
			invalidIdentities = append(invalidIdentities, i)
		}
	}

	if len(invalidIdentities) > 0 {
		return fmt.Errorf("invalid identities found in allowedDisruptionsOutsideOfWindow: %s", strings.Join(invalidIdentities, ", "))
	}

	return nil
}
