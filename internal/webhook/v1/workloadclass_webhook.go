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
)

// nolint:unused
// log is for logging in this package.
var workloadclasslog = logf.Log.WithName("workloadclass-resource")

// SetupWorkloadClassWebhookWithManager registers the webhook for WorkloadClass in the manager.
func SetupWorkloadClassWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &workloadsv1.WorkloadClass{}).
		WithValidator(&WorkloadClassCustomValidator{Client: mgr.GetClient()}).
		WithDefaulter(&WorkloadClassCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-workloads-gke-io-v1-workloadclass,mutating=true,failurePolicy=fail,sideEffects=None,groups=workloads.gke.io,resources=workloadclasses,verbs=create;update,versions=v1,name=mworkloadclass-v1.kb.io,admissionReviewVersions=v1

// WorkloadClassCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind WorkloadClass when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type WorkloadClassCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind WorkloadClass.
func (d *WorkloadClassCustomDefaulter) Default(_ context.Context, obj *workloadsv1.WorkloadClass) error {
	workloadclasslog.Info("Defaulting for WorkloadClass", "name", obj.GetName())
	return nil
}

// change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
// +kubebuilder:webhook:path=/validate-workloads-gke-io-v1-workloadclass,mutating=false,failurePolicy=fail,sideEffects=None,groups=workloads.gke.io,resources=workloadclasses,verbs=create;update,versions=v1,name=vworkloadclass-v1.kb.io,admissionReviewVersions=v1

// WorkloadClassCustomValidator struct is responsible for validating the WorkloadClass resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type WorkloadClassCustomValidator struct {
	Client client.Client
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateCreate(ctx context.Context, obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon creation", "name", obj.GetName())
	return v.validateNamespaceDefaultLabel(ctx, obj)
}

func (v *WorkloadClassCustomValidator) validateNamespaceDefaultLabel(ctx context.Context, wc *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	const namespaceDefaultLabel = "workloads.gke.io/default-class"
	// If the WorkloadClass does not have the namespace default, this check can be skipped
	if _, ok := wc.Labels[namespaceDefaultLabel]; !ok {
		return nil, nil
	}

	// Fetch all WorkloadClasses to see if this any others are the namespace default
	wcs := &workloadsv1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs,
		client.InNamespace(wc.Namespace),
		client.HasLabels{namespaceDefaultLabel},
	); err != nil {
		return []string{fmt.Sprintf("failed to list WorkloadClasses: %v", err)}, err
	}

	namespaceDefaults := getNamespaceDefaultNames(wc, wcs.Items)

	// If no other WorkloadClasses have the label, then this WorkloadClass can be admitted
	if len(namespaceDefaults) == 0 {
		return nil, nil
	}

	return formatNamespaceDefaultError(wc, namespaceDefaults)
}

func getNamespaceDefaultNames(wc *workloadsv1.WorkloadClass, wcs []workloadsv1.WorkloadClass) []string {
	namespaceDefaults := []string{}
	for _, d := range wcs {
		if d.Name == wc.Name {
			continue
		}
		namespaceDefaults = append(namespaceDefaults, d.Name)
	}
	return namespaceDefaults
}

func formatNamespaceDefaultError(wc *workloadsv1.WorkloadClass, wcs []string) (admission.Warnings, error) {
	joinedNames := strings.Join(wcs, ", ")
	err := fmt.Errorf("WorkloadClass %s is invalid, namespace %s already contains a namespace default: %s", wc.Name, wc.Namespace, joinedNames)
	return []string{err.Error()}, err
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon update", "name", newObj.GetName())
	return v.validateNamespaceDefaultLabel(ctx, newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateDelete(_ context.Context, obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon deletion", "name", obj.GetName())
	return nil, nil
}
