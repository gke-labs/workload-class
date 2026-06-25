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

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
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

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
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

	// TODO(user): fill in your validation logic upon object creation.

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon update", "name", newObj.GetName())

	// TODO(user): fill in your validation logic upon object update.

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateDelete(_ context.Context, obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon deletion", "name", obj.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}
