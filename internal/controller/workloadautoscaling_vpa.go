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
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	// reasonInPlaceUnsupported is the event reason recorded on a workload when its WorkloadClass asks for
	// in-place rightsizing, but the VerticalPodAutoscaler installation does not accept the InPlaceOrRecreate
	// update mode, so the Recreate update mode is used instead.
	reasonInPlaceUnsupported = "InPlaceUnsupported"

	vpaKind = "VerticalPodAutoscaler"
)

// IsVPAInstalled reports whether the VerticalPodAutoscaler API is served by the cluster, by asking the REST
// mapper for its kind. It is checked once when the manager starts: installing the VPA CRD later needs a
// restart of the manager before rightsizing is applied.
func IsVPAInstalled(mapper meta.RESTMapper) (bool, error) {
	_, err := mapper.RESTMapping(utils.VPAGroupVersionKind.GroupKind(), utils.VPAGroupVersionKind.Version)
	if err == nil {
		return true, nil
	}
	if meta.IsNoMatchError(err) {
		return false, nil
	}
	return false, fmt.Errorf("failed to look up the %s API: %w", vpaKind, err)
}

// newVPA returns an empty VerticalPodAutoscaler object.
func newVPA() *unstructured.Unstructured {
	vpa := &unstructured.Unstructured{}
	vpa.SetGroupVersionKind(utils.VPAGroupVersionKind)
	return vpa
}

// vpaCapabilities returns what the VPA installation and the platform are known to support.
func (r *WorkloadAutoscalingReconciler) vpaCapabilities() utils.VPACapabilities {
	return utils.VPACapabilities{
		InPlaceUnsupported:   r.inPlaceUnsupported.Load(),
		CoordinatedResources: r.CoordinatedResources,
	}
}

// reconcileVPA makes the managed VerticalPodAutoscaler of the workload match the rightsizing of the
// WorkloadClass (wc may be nil). It follows the same rules as reconcileHPA: a user-created VPA that targets
// the workload takes precedence, the managed VPA is deleted when the WorkloadClass does not ask for one, and
// an object that uses the managed name but is not managed by a WorkloadClass is never modified.
//
// If the VPA installation rejects the InPlaceOrRecreate update mode, the VPA is applied with the Recreate
// update mode instead, and an InPlaceUnsupported event is recorded.
//
// Nothing is done when the VerticalPodAutoscaler CRD is not installed.
func (r *WorkloadAutoscalingReconciler) reconcileVPA(ctx context.Context, workload client.Object, ref utils.WorkloadRef, wc *workloadsv1.WorkloadClass) error {
	if !r.VPAInstalled {
		return nil
	}
	log := logf.FromContext(ctx)
	name := utils.ManagedAutoscalerName(ref.Kind, ref.Name)

	var effective utils.EffectiveResourceManagement
	var desired *unstructured.Unstructured
	if wc != nil {
		guardrails := &workloadsv1.WorkloadClassGuardrailList{}
		if err := r.List(ctx, guardrails); err != nil {
			return fmt.Errorf("failed to list WorkloadClassGuardrails: %w", err)
		}
		effective = utils.EffectiveResourceManagementConstraints(guardrails.Items)
		desired = utils.BuildVPA(wc, ref, effective, r.vpaCapabilities())
	}
	if desired != nil {
		userVPA, err := r.userVPAForWorkload(ctx, ref)
		if err != nil {
			return err
		}
		if userVPA != nil {
			log.Info("User-created VerticalPodAutoscaler targets the workload; WorkloadClass rightsizing will not be applied",
				"verticalPodAutoscaler", userVPA.GetName(), "workloadClass", wc.Name)
			r.Recorder.Eventf(workload, userVPA, corev1.EventTypeNormal, reasonUserAutoscalerPresent, actionReconcileAutoscaler,
				"VerticalPodAutoscaler %s already targets this %s, so the rightsizing of WorkloadClass %s is not applied",
				userVPA.GetName(), ref.Kind, wc.Name)
			desired = nil
		}
	}
	if desired == nil {
		return r.deleteManagedVPA(ctx, workload, name)
	}

	vpa, op, conflict, err := r.applyVPA(ctx, workload, desired)
	if err != nil && utils.VPAUpdateModeOf(desired) == utils.VPAUpdateModeInPlaceOrRecreate && isRejected(err) {
		// Older VPA versions do not know the InPlaceOrRecreate update mode, and newer ones reject it unless
		// in-place updates are enabled. Fall back to Recreate, and remember the outcome only if Recreate is
		// accepted, which shows that the update mode was the reason for the rejection.
		log.Info("VerticalPodAutoscaler with the InPlaceOrRecreate update mode was rejected; retrying with Recreate",
			"verticalPodAutoscaler", name, "error", err.Error())
		fallback := r.vpaCapabilities()
		fallback.InPlaceUnsupported = true
		desired = utils.BuildVPA(wc, ref, effective, fallback)
		vpa, op, conflict, err = r.applyVPA(ctx, workload, desired)
		if err == nil && !conflict {
			r.inPlaceUnsupported.Store(true)
		}
	}
	if err != nil {
		return fmt.Errorf("failed to reconcile VerticalPodAutoscaler %s: %w", name, err)
	}
	if conflict {
		log.Info("VerticalPodAutoscaler with the managed name is not managed by a WorkloadClass; it will not be modified",
			"verticalPodAutoscaler", name)
		r.Recorder.Eventf(workload, vpa, corev1.EventTypeWarning, reasonAutoscalerNameConflict, actionReconcileAutoscaler,
			"VerticalPodAutoscaler %s already exists and is not managed by a WorkloadClass, so the rightsizing of WorkloadClass %s is not applied",
			name, wc.Name)
		return nil
	}
	if op == controllerutil.OperationResultNone {
		return nil
	}

	log.Info("Reconciled VerticalPodAutoscaler", "verticalPodAutoscaler", name, "operation", op,
		"workloadClass", wc.Name, "updateMode", utils.VPAUpdateModeOf(desired))
	r.Recorder.Eventf(workload, vpa, corev1.EventTypeNormal, reasonAutoscalerApplied, actionReconcileAutoscaler,
		"VerticalPodAutoscaler %s %s from the rightsizing of WorkloadClass %s", name, op, wc.Name)
	if wantsInPlace(wc) && utils.VPAUpdateModeOf(desired) == utils.VPAUpdateModeRecreate {
		r.Recorder.Eventf(workload, vpa, corev1.EventTypeWarning, reasonInPlaceUnsupported, actionReconcileAutoscaler,
			"The VerticalPodAutoscaler installation does not accept the %s update mode, so WorkloadClass %s rightsizes this %s by recreating Pods",
			utils.VPAUpdateModeInPlaceOrRecreate, wc.Name, ref.Kind)
	}
	return nil
}

// wantsInPlace reports whether the rightsizing of the WorkloadClass asks for in-place updates.
func wantsInPlace(wc *workloadsv1.WorkloadClass) bool {
	rs := wc.Spec.ResourceManagement.Rightsizing
	return utils.EffectiveRightsizingMode(rs) == workloadsv1.RightsizingModeAuto &&
		utils.EffectiveUpdateStrategy(rs) == workloadsv1.RightsizingUpdateStrategyInPlacePreferred
}

// isRejected reports whether the API server refused an object because of its content: it failed the schema
// of its CRD (Invalid), or an admission webhook denied it (BadRequest or Forbidden).
func isRejected(err error) bool {
	return apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsForbidden(err)
}

// applyVPA creates or updates the managed VerticalPodAutoscaler so that the fields managed by utils.BuildVPA
// match the desired VPA, with the workload as its controller owner. It reports a conflict, and changes
// nothing, if a VPA with that name exists but is not managed by a WorkloadClass.
func (r *WorkloadAutoscalingReconciler) applyVPA(ctx context.Context, workload client.Object, desired *unstructured.Unstructured) (*unstructured.Unstructured, controllerutil.OperationResult, bool, error) {
	vpa := newVPA()
	vpa.SetName(desired.GetName())
	vpa.SetNamespace(desired.GetNamespace())

	conflict := false
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, vpa, func() error {
		if vpa.GetResourceVersion() != "" && !utils.IsWorkloadClassManagedAutoscaler(vpa) {
			conflict = true
			return nil
		}
		labels := vpa.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		maps.Copy(labels, desired.GetLabels())
		vpa.SetLabels(labels)
		if err := utils.ApplyVPASpec(vpa, desired); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(workload, vpa, r.Scheme)
	})
	return vpa, op, conflict, err
}

// userVPAForWorkload returns a user-created VerticalPodAutoscaler that targets the workload, or nil if there
// is none. VPAs without the managed-by label are user-created.
func (r *WorkloadAutoscalingReconciler) userVPAForWorkload(ctx context.Context, ref utils.WorkloadRef) (*unstructured.Unstructured, error) {
	vpas := &unstructured.UnstructuredList{}
	vpas.SetGroupVersionKind(utils.VPAGroupVersionKind.GroupVersion().WithKind(vpaKind + "List"))
	if err := r.List(ctx, vpas, client.InNamespace(ref.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list VerticalPodAutoscalers: %w", err)
	}
	for i := range vpas.Items {
		vpa := &vpas.Items[i]
		if utils.IsWorkloadClassManagedAutoscaler(vpa) || !vpa.GetDeletionTimestamp().IsZero() {
			continue
		}
		if utils.VPATargetsWorkload(vpa, ref) {
			return vpa, nil
		}
	}
	return nil, nil
}

// deleteManagedVPA deletes the managed VerticalPodAutoscaler of the workload, if it exists. It never deletes
// a VPA that is not managed by a WorkloadClass or that is controlled by another object. Deleting is safe:
// the containers of the workload keep their current sizes.
func (r *WorkloadAutoscalingReconciler) deleteManagedVPA(ctx context.Context, workload client.Object, name string) error {
	vpa := newVPA()
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: workload.GetNamespace()}, vpa); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !utils.IsWorkloadClassManagedAutoscaler(vpa) || !metav1.IsControlledBy(vpa, workload) {
		return nil
	}
	uid := vpa.GetUID()
	if err := r.Delete(ctx, vpa, client.Preconditions{UID: &uid}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete VerticalPodAutoscaler %s: %w", name, err)
	}
	logf.FromContext(ctx).Info("Deleted VerticalPodAutoscaler", "verticalPodAutoscaler", name)
	return nil
}
