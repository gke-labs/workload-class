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
	"errors"
	"fmt"
	"maps"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	// EventReasonUserAutoscalerPresent is recorded on a workload when a user-created autoscaler targets it, so the
	// controller does not create its own autoscaler of that kind.
	EventReasonUserAutoscalerPresent = "UserAutoscalerPresent"

	// EventReasonRightsizingSkipped is recorded on a WorkloadClass when no resource is left to rightsize, because
	// autoscaling targets the utilization of every rightsized resource and the platform does not coordinate them.
	EventReasonRightsizingSkipped = "RightsizingSkipped"

	// EventReasonInPlaceUnsupported is recorded on a WorkloadClass when the installed VerticalPodAutoscaler does not
	// support the InPlaceOrRecreate update mode, so the InPlacePreferred update strategy falls back to Recreate.
	EventReasonInPlaceUnsupported = "InPlaceUnsupported"

	// EventReasonVPANotInstalled is recorded on a WorkloadClass that enables rightsizing when the
	// VerticalPodAutoscaler is not installed.
	EventReasonVPANotInstalled = "VPANotInstalled"

	eventActionReconcileAutoscaler = "ReconcileAutoscaler"
)

// WorkloadKind describes a kind of workload that resourceManagement applies to.
type WorkloadKind struct {
	// Kind is utils.WorkloadKindDeployment or utils.WorkloadKindStatefulSet.
	Kind string
	// NewObject returns an empty object of the kind.
	NewObject func() client.Object
	// NewList returns an empty list of the kind.
	NewList func() client.ObjectList
}

var (
	// DeploymentWorkload describes Deployments.
	DeploymentWorkload = WorkloadKind{
		Kind:      utils.WorkloadKindDeployment,
		NewObject: func() client.Object { return &appsv1.Deployment{} },
		NewList:   func() client.ObjectList { return &appsv1.DeploymentList{} },
	}
	// StatefulSetWorkload describes StatefulSets.
	StatefulSetWorkload = WorkloadKind{
		Kind:      utils.WorkloadKindStatefulSet,
		NewObject: func() client.Object { return &appsv1.StatefulSet{} },
		NewList:   func() client.ObjectList { return &appsv1.StatefulSetList{} },
	}
	// WorkloadKinds lists every kind of workload that resourceManagement applies to.
	WorkloadKinds = []WorkloadKind{DeploymentWorkload, StatefulSetWorkload}
)

// WorkloadAutoscalingReconciler creates, updates, and deletes the HorizontalPodAutoscaler and the
// VerticalPodAutoscaler of each workload of one kind (Deployment or StatefulSet), following the resourceManagement of
// the workload's best-match WorkloadClass.
//
// It is keyed by workload rather than by WorkloadClass: each workload has exactly one best-match WorkloadClass and
// owns at most one managed autoscaler of each kind, so a workload that moves to another WorkloadClass is handled
// like any other change. Validation stays in the WorkloadClass controller, and this reconciler only reads its
// ResourceManagementValidated condition, as the Pod webhook does with PlacementValidated.
type WorkloadAutoscalingReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// Workload is the kind of workload that the reconciler handles.
	Workload WorkloadKind

	// Capabilities describes what the cluster's autoscalers support. VerticalPodAutoscalers are only watched and
	// reconciled when Capabilities.VPAInstalled is true.
	Capabilities utils.AutoscalerCapabilities
}

// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments/finalizers;statefulsets/finalizers,verbs=update
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling.k8s.io,resources=verticalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses;workloadclassguardrails,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile brings the managed HorizontalPodAutoscaler and VerticalPodAutoscaler of a workload in line with its
// best-match WorkloadClass:
//
//  1. If the workload is gone, nothing is left to do: owner references garbage-collect its autoscalers.
//  2. If no WorkloadClass governs the workload, the WorkloadClass has no resourceManagement, or its
//     ResourceManagementValidated condition is failing, the managed autoscalers are deleted. Deleting is safe: the
//     workload keeps its replica count, and nothing scales it until the WorkloadClass is valid again.
//  3. While the condition is missing or stale (behind the WorkloadClass generation), the managed autoscalers are
//     left as they are. The WorkloadClass controller updates the condition shortly, which triggers a new reconcile,
//     so editing a WorkloadClass doesn't delete and recreate the autoscalers of every workload it governs.
//  4. Otherwise the desired autoscaler is built and applied with the workload as its controller owner, unless a
//     user-created autoscaler already targets the workload, which takes precedence.
func (r *WorkloadAutoscalingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	obj := r.Workload.NewObject()
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !obj.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, nil
	}
	workload, ok := utils.WorkloadRefFor(obj)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("unsupported workload type %T", obj)
	}

	wc, err := r.governingWorkloadClass(ctx, workload)
	if err != nil {
		return ctrl.Result{}, err
	}

	gate, reason := autoscalingGateFor(wc)
	switch gate {
	case autoscalingRemove:
		log.V(1).Info("Removing managed autoscalers", "reason", reason)
		return ctrl.Result{}, errors.Join(r.deleteManagedHPA(ctx, obj, workload), r.deleteManagedVPA(ctx, obj, workload))
	case autoscalingPending:
		log.V(1).Info("Waiting for the WorkloadClass to be validated", "workloadClass", wc.Name, "reason", reason)
		return ctrl.Result{}, nil
	}

	ctx = logf.IntoContext(ctx, log.WithValues("workloadClass", wc.Name))
	hpaErr := r.reconcileHPA(ctx, obj, workload, utils.BuildHPA(wc, workload))
	vpaErr := r.reconcileVPA(ctx, obj, wc, workload)
	return ctrl.Result{}, errors.Join(hpaErr, vpaErr)
}

// governingWorkloadClass returns the best-match WorkloadClass of the workload's Pod template, or nil if none matches.
func (r *WorkloadAutoscalingReconciler) governingWorkloadClass(ctx context.Context, workload utils.WorkloadRef) (*workloadsv1.WorkloadClass, error) {
	enforcedWC, err := utils.NamespaceEnforcedWorkloadClass(ctx, r.Client, workload.Namespace)
	if err != nil {
		return nil, err
	}
	wcs := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, wcs, client.InNamespace(workload.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}
	return utils.BestMatchWorkloadClassForTemplate(workload, enforcedWC, wcs.Items), nil
}

// autoscalingGate is what the reconciler does with the managed autoscalers of a workload.
type autoscalingGate int

const (
	// autoscalingRemove deletes the managed autoscalers.
	autoscalingRemove autoscalingGate = iota
	// autoscalingPending leaves the managed autoscalers as they are.
	autoscalingPending
	// autoscalingApply creates or updates the managed autoscalers.
	autoscalingApply
)

// autoscalingGateFor decides what to do with the managed autoscalers of a workload governed by wc (nil if none), and
// why. Autoscalers are applied only when the ResourceManagementValidated condition is current and passing: True,
// or False only because of a PodSelector overlap (SelectorOverlap), since each workload still resolves to a single
// best-match WorkloadClass.
func autoscalingGateFor(wc *workloadsv1.WorkloadClass) (autoscalingGate, string) {
	if wc == nil {
		return autoscalingRemove, "no WorkloadClass governs the workload"
	}
	if wc.Spec.ResourceManagement == nil {
		return autoscalingRemove, fmt.Sprintf("WorkloadClass %s has no resourceManagement", wc.Name)
	}
	cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated)
	switch {
	case cond == nil:
		return autoscalingPending, fmt.Sprintf("WorkloadClass %s has no %s condition yet", wc.Name, workloadsv1.ConditionTypeResourceManagementValidated)
	case wc.Generation > 0 && cond.ObservedGeneration != wc.Generation:
		return autoscalingPending, fmt.Sprintf("the %s condition of WorkloadClass %s is stale (observedGeneration %d, generation %d)",
			workloadsv1.ConditionTypeResourceManagementValidated, wc.Name, cond.ObservedGeneration, wc.Generation)
	case !utils.IsValidated(cond):
		return autoscalingRemove, fmt.Sprintf("WorkloadClass %s is not valid: %s", wc.Name, cond.Message)
	}
	return autoscalingApply, ""
}

// reconcileHPA creates or updates the managed HorizontalPodAutoscaler of the workload, or deletes it if desired is
// nil (the WorkloadClass has no autoscaling) or a user-created HorizontalPodAutoscaler targets the workload.
func (r *WorkloadAutoscalingReconciler) reconcileHPA(ctx context.Context, owner client.Object, workload utils.WorkloadRef, desired *autoscalingv2.HorizontalPodAutoscaler) error {
	hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
	if err := r.List(ctx, hpas, client.InNamespace(workload.Namespace)); err != nil {
		return fmt.Errorf("failed to list HorizontalPodAutoscalers: %w", err)
	}
	for i := range hpas.Items {
		hpa := &hpas.Items[i]
		if !utils.IsWorkloadClassManagedAutoscaler(hpa) && targetsWorkload(hpa.Spec.ScaleTargetRef.APIVersion, hpa.Spec.ScaleTargetRef.Kind, hpa.Spec.ScaleTargetRef.Name, workload) {
			if desired != nil {
				r.recordUserAutoscalerPresent(owner, workload, "HorizontalPodAutoscaler", hpa.Name)
			}
			return r.deleteManagedHPA(ctx, owner, workload)
		}
	}

	if desired == nil {
		return r.deleteManagedHPA(ctx, owner, workload)
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, hpa, func() error {
		if err := r.claimAutoscaler(owner, hpa, "HorizontalPodAutoscaler"); err != nil {
			return err
		}
		hpa.Labels = withLabels(hpa.Labels, desired.Labels)
		hpa.Spec = desired.Spec
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile HorizontalPodAutoscaler %s: %w", desired.Name, err)
	}
	if op != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled HorizontalPodAutoscaler", "horizontalPodAutoscaler", hpa.Name, "operation", op)
	}
	return nil
}

func (r *WorkloadAutoscalingReconciler) deleteManagedHPA(ctx context.Context, owner client.Object, workload utils.WorkloadRef) error {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	return r.deleteManagedAutoscaler(ctx, owner, workload, hpa, "HorizontalPodAutoscaler")
}

// reconcileVPA creates or updates the managed VerticalPodAutoscaler of the workload, or deletes it if the
// WorkloadClass describes none (see utils.BuildVPA) or a user-created VerticalPodAutoscaler targets the workload.
// When the VerticalPodAutoscaler is not installed, it does nothing: the WorkloadClass controller reports that
// rightsizing is not applied.
func (r *WorkloadAutoscalingReconciler) reconcileVPA(ctx context.Context, owner client.Object, wc *workloadsv1.WorkloadClass, workload utils.WorkloadRef) error {
	if !r.Capabilities.VPAInstalled {
		return nil
	}

	var desired *vpav1.VerticalPodAutoscaler
	var outcome utils.VPAOutcome
	if wc.Spec.ResourceManagement.Rightsizing != nil {
		guardrails := &workloadsv1.WorkloadClassGuardrailList{}
		if err := r.List(ctx, guardrails); err != nil {
			return fmt.Errorf("failed to list WorkloadClassGuardrails: %w", err)
		}
		desired, outcome = utils.BuildVPA(wc, workload, utils.EffectiveResourceManagementConstraints(guardrails.Items), r.Capabilities)
	}

	vpas := &vpav1.VerticalPodAutoscalerList{}
	if err := r.List(ctx, vpas, client.InNamespace(workload.Namespace)); err != nil {
		return fmt.Errorf("failed to list VerticalPodAutoscalers: %w", err)
	}
	for i := range vpas.Items {
		vpa := &vpas.Items[i]
		if ref := vpa.Spec.TargetRef; ref != nil && !utils.IsWorkloadClassManagedAutoscaler(vpa) && targetsWorkload(ref.APIVersion, ref.Kind, ref.Name, workload) {
			if desired != nil {
				r.recordUserAutoscalerPresent(owner, workload, "VerticalPodAutoscaler", vpa.Name)
			}
			return r.deleteManagedVPA(ctx, owner, workload)
		}
	}

	if desired == nil {
		if len(outcome.ExcludedResources) > 0 && r.Recorder != nil {
			r.Recorder.Eventf(wc, nil, corev1.EventTypeNormal, EventReasonRightsizingSkipped, eventActionReconcileAutoscaler,
				"Autoscaling targets the utilization of %s, so no resource is left to rightsize and no VerticalPodAutoscaler is created for %s %s",
				utils.QuotedList(resourceNames(outcome.ExcludedResources)), workload.Kind, workload.Name)
		}
		return r.deleteManagedVPA(ctx, owner, workload)
	}
	if outcome.InPlaceFallback && r.Recorder != nil {
		r.Recorder.Eventf(wc, nil, corev1.EventTypeWarning, EventReasonInPlaceUnsupported, eventActionReconcileAutoscaler,
			"The installed VerticalPodAutoscaler does not support the InPlaceOrRecreate update mode, so the InPlacePreferred "+
				"update strategy falls back to Recreate for %s %s", workload.Kind, workload.Name)
	}

	vpa := &vpav1.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, vpa, func() error {
		if err := r.claimAutoscaler(owner, vpa, "VerticalPodAutoscaler"); err != nil {
			return err
		}
		vpa.Labels = withLabels(vpa.Labels, desired.Labels)
		vpa.Spec = desired.Spec
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile VerticalPodAutoscaler %s: %w", desired.Name, err)
	}
	if op != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled VerticalPodAutoscaler", "verticalPodAutoscaler", vpa.Name, "operation", op)
	}
	return nil
}

func (r *WorkloadAutoscalingReconciler) deleteManagedVPA(ctx context.Context, owner client.Object, workload utils.WorkloadRef) error {
	if !r.Capabilities.VPAInstalled {
		return nil
	}
	vpa := &vpav1.VerticalPodAutoscaler{}
	return r.deleteManagedAutoscaler(ctx, owner, workload, vpa, "VerticalPodAutoscaler")
}

func resourceNames(names []corev1.ResourceName) []string {
	strs := make([]string, 0, len(names))
	for _, name := range names {
		strs = append(strs, string(name))
	}
	return strs
}

// deleteManagedAutoscaler deletes the managed autoscaler of the workload into which obj is read, if it exists, is
// managed by a WorkloadClass, and is controlled by the workload. The UID and resourceVersion preconditions keep it
// from deleting an object that changed after it was read.
func (r *WorkloadAutoscalingReconciler) deleteManagedAutoscaler(ctx context.Context, owner client.Object, workload utils.WorkloadRef, obj client.Object, kind string) error {
	key := client.ObjectKey{Name: utils.ManagedAutoscalerName(workload.Kind, workload.Name), Namespace: workload.Namespace}
	if err := r.Get(ctx, key, obj); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("failed to get %s %s: %w", kind, key.Name, err)
	}
	if !utils.IsWorkloadClassManagedAutoscaler(obj) || !metav1.IsControlledBy(obj, owner) {
		return nil
	}
	uid, resourceVersion := obj.GetUID(), obj.GetResourceVersion()
	if err := r.Delete(ctx, obj, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete %s %s: %w", kind, key.Name, err)
	}
	logf.FromContext(ctx).Info("Deleted "+kind, "name", key.Name)
	return nil
}

// claimAutoscaler sets the workload as the controller owner of a managed autoscaler. It refuses to modify an existing
// object with the managed name that is not managed by a WorkloadClass.
func (r *WorkloadAutoscalingReconciler) claimAutoscaler(owner, obj client.Object, kind string) error {
	if obj.GetResourceVersion() != "" && !utils.IsWorkloadClassManagedAutoscaler(obj) {
		return fmt.Errorf("%s %s/%s already exists and is not managed by a WorkloadClass", kind, obj.GetNamespace(), obj.GetName())
	}
	return controllerutil.SetControllerReference(owner, obj, r.Scheme)
}

func (r *WorkloadAutoscalingReconciler) recordUserAutoscalerPresent(owner client.Object, workload utils.WorkloadRef, kind, name string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(owner, nil, corev1.EventTypeNormal, EventReasonUserAutoscalerPresent, eventActionReconcileAutoscaler,
		"%s %s targets the %s and is not managed by a WorkloadClass, so the WorkloadClass does not create one",
		kind, name, workload.Kind)
}

// targetsWorkload reports whether an autoscaler target reference (apiVersion, kind, and name) points at the workload.
func targetsWorkload(apiVersion, kind, name string, workload utils.WorkloadRef) bool {
	gv, err := schema.ParseGroupVersion(apiVersion)
	return err == nil && gv.Group == appsv1.GroupName && kind == workload.Kind && name == workload.Name
}

// withLabels returns existing with the given labels added or replaced, keeping any other labels.
func withLabels(existing, labels map[string]string) map[string]string {
	if existing == nil {
		existing = make(map[string]string, len(labels))
	}
	maps.Copy(existing, labels)
	return existing
}

// SetupWithManager sets up the controller with the Manager. VerticalPodAutoscalers are only watched when they are
// installed, because watching a type that isn't served would stop the manager from starting.
func (r *WorkloadAutoscalingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		// The best-match WorkloadClass depends only on the workload's namespace and Pod template labels.
		For(r.Workload.NewObject(), builder.WithPredicates(workloadTemplateLabelsPredicate())).
		// Revert changes to, and recreate, the managed autoscalers.
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}, builder.WithPredicates(managedHPAPredicate())).
		// A WorkloadClass being created, deleted, changing its spec, or changing its ResourceManagementValidated
		// condition can change what every workload in its namespace needs.
		Watches(
			&workloadsv1.WorkloadClass{},
			handler.EnqueueRequestsFromMapFunc(r.workloadsInObjectNamespace),
			builder.WithPredicates(workloadClassAutoscalingPredicate()),
		).
		// Changing the namespace's enforced WorkloadClass changes the WorkloadClass of every workload in it.
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.workloadsInNamespace),
			builder.WithPredicates(namespaceEnforcedClassPredicate()),
		).
		// User-created HorizontalPodAutoscalers take precedence: step back when one targets a workload, and take
		// over again when it is removed or retargeted.
		Watches(
			&autoscalingv2.HorizontalPodAutoscaler{},
			handler.EnqueueRequestsFromMapFunc(r.hpaTarget),
			builder.WithPredicates(userAutoscalerPredicate()),
		)
	if r.Capabilities.VPAInstalled {
		b = b.
			Owns(&vpav1.VerticalPodAutoscaler{}, builder.WithPredicates(managedVPAPredicate())).
			Watches(
				&vpav1.VerticalPodAutoscaler{},
				handler.EnqueueRequestsFromMapFunc(r.vpaTarget),
				builder.WithPredicates(userAutoscalerPredicate()),
			).
			// The effective guardrail containerBounds apply to VerticalPodAutoscalers whose WorkloadClass sets no
			// bounds. Guardrail status updates are ignored.
			Watches(
				&workloadsv1.WorkloadClassGuardrail{},
				handler.EnqueueRequestsFromMapFunc(r.allWorkloads),
				builder.WithPredicates(predicate.GenerationChangedPredicate{}),
			)
	}
	return b.Named("workloadautoscaling-" + strings.ToLower(r.Workload.Kind)).Complete(r)
}

// workloadsInObjectNamespace enqueues every workload of the reconciler's kind in the object's namespace.
func (r *WorkloadAutoscalingReconciler) workloadsInObjectNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.workloadRequests(ctx, obj.GetNamespace())
}

// workloadsInNamespace enqueues every workload of the reconciler's kind in the Namespace.
func (r *WorkloadAutoscalingReconciler) workloadsInNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.workloadRequests(ctx, obj.GetName())
}

// allWorkloads enqueues every workload of the reconciler's kind in the cluster.
func (r *WorkloadAutoscalingReconciler) allWorkloads(ctx context.Context, _ client.Object) []reconcile.Request {
	return r.workloadRequests(ctx, metav1.NamespaceAll)
}

func (r *WorkloadAutoscalingReconciler) workloadRequests(ctx context.Context, namespace string) []reconcile.Request {
	list := r.Workload.NewList()
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list workloads", "kind", r.Workload.Kind, "namespace", namespace)
		return nil
	}
	var requests []reconcile.Request
	if err := meta.EachListItem(list, func(o runtime.Object) error {
		if obj, ok := o.(client.Object); ok {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
		}
		return nil
	}); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to read workload list", "kind", r.Workload.Kind, "namespace", namespace)
		return nil
	}
	return requests
}

// hpaTarget enqueues the workload that a HorizontalPodAutoscaler targets, if it is of the reconciler's kind.
func (r *WorkloadAutoscalingReconciler) hpaTarget(_ context.Context, obj client.Object) []reconcile.Request {
	hpa, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler)
	if !ok {
		return nil
	}
	ref := hpa.Spec.ScaleTargetRef
	return r.targetRequests(hpa.Namespace, ref.APIVersion, ref.Kind, ref.Name)
}

// vpaTarget enqueues the workload that a VerticalPodAutoscaler targets, if it is of the reconciler's kind.
func (r *WorkloadAutoscalingReconciler) vpaTarget(_ context.Context, obj client.Object) []reconcile.Request {
	vpa, ok := obj.(*vpav1.VerticalPodAutoscaler)
	if !ok || vpa.Spec.TargetRef == nil {
		return nil
	}
	ref := vpa.Spec.TargetRef
	return r.targetRequests(vpa.Namespace, ref.APIVersion, ref.Kind, ref.Name)
}

func (r *WorkloadAutoscalingReconciler) targetRequests(namespace, apiVersion, kind, name string) []reconcile.Request {
	if !targetsWorkload(apiVersion, kind, name, utils.WorkloadRef{Kind: r.Workload.Kind, Name: name}) || name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}}
}

// workloadTemplateLabelsPredicate passes workload creation, changes to the workload's Pod template labels, and
// periodic resyncs.
func workloadTemplateLabelsPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		// Owner references garbage-collect the autoscalers of a deleted workload.
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld.GetResourceVersion() == e.ObjectNew.GetResourceVersion() {
				// A periodic resync: reconcile to repair any drift.
				return true
			}
			oldRef, okOld := utils.WorkloadRefFor(e.ObjectOld)
			newRef, okNew := utils.WorkloadRefFor(e.ObjectNew)
			return okOld && okNew && !maps.Equal(oldRef.TemplateLabels, newRef.TemplateLabels)
		},
	}
}

// managedHPAPredicate passes managed HorizontalPodAutoscaler deletions and changes to their spec, labels, or owner
// references, but not their frequent status updates.
func managedHPAPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldHPA, okOld := e.ObjectOld.(*autoscalingv2.HorizontalPodAutoscaler)
			newHPA, okNew := e.ObjectNew.(*autoscalingv2.HorizontalPodAutoscaler)
			if !okOld || !okNew {
				return false
			}
			return !equality.Semantic.DeepEqual(oldHPA.Spec, newHPA.Spec) || metadataChanged(oldHPA, newHPA)
		},
	}
}

// managedVPAPredicate passes managed VerticalPodAutoscaler deletions and changes to their spec, labels, or owner
// references, but not the recommendations that the VerticalPodAutoscaler writes to its status.
func managedVPAPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldVPA, okOld := e.ObjectOld.(*vpav1.VerticalPodAutoscaler)
			newVPA, okNew := e.ObjectNew.(*vpav1.VerticalPodAutoscaler)
			if !okOld || !okNew {
				return false
			}
			return !equality.Semantic.DeepEqual(oldVPA.Spec, newVPA.Spec) || metadataChanged(oldVPA, newVPA)
		},
	}
}

// metadataChanged reports whether the labels or owner references of an object changed.
func metadataChanged(oldObj, newObj client.Object) bool {
	return !maps.Equal(oldObj.GetLabels(), newObj.GetLabels()) ||
		!equality.Semantic.DeepEqual(oldObj.GetOwnerReferences(), newObj.GetOwnerReferences())
}

// userAutoscalerPredicate passes events for autoscalers that are not managed by a WorkloadClass.
func userAutoscalerPredicate() predicate.Funcs {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return !utils.IsWorkloadClassManagedAutoscaler(obj)
	})
}

// workloadClassAutoscalingPredicate passes WorkloadClass creation and deletion, spec changes, and changes to the
// ResourceManagementValidated condition, but not the other status updates of a WorkloadClass.
func workloadClassAutoscalingPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldWC, okOld := e.ObjectOld.(*workloadsv1.WorkloadClass)
			newWC, okNew := e.ObjectNew.(*workloadsv1.WorkloadClass)
			if !okOld || !okNew {
				return false
			}
			if oldWC.Generation != newWC.Generation || oldWC.DeletionTimestamp.IsZero() != newWC.DeletionTimestamp.IsZero() {
				return true
			}
			oldCond := meta.FindStatusCondition(oldWC.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated)
			newCond := meta.FindStatusCondition(newWC.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated)
			if oldCond == nil || newCond == nil {
				return oldCond != newCond
			}
			return oldCond.Status != newCond.Status || oldCond.Reason != newCond.Reason || oldCond.ObservedGeneration != newCond.ObservedGeneration
		},
	}
}

// SetupWorkloadAutoscalingControllers registers a WorkloadAutoscalingReconciler for each kind of workload that
// resourceManagement applies to.
func SetupWorkloadAutoscalingControllers(mgr ctrl.Manager, caps utils.AutoscalerCapabilities) error {
	for _, kind := range WorkloadKinds {
		r := &WorkloadAutoscalingReconciler{
			Client:       mgr.GetClient(),
			Scheme:       mgr.GetScheme(),
			Recorder:     mgr.GetEventRecorder("workloadautoscaling-controller"),
			Workload:     kind,
			Capabilities: caps,
		}
		if err := r.SetupWithManager(mgr); err != nil {
			return fmt.Errorf("failed to set up the %s autoscaling controller: %w", kind.Kind, err)
		}
	}
	return nil
}
