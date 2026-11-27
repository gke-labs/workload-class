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
	"sync/atomic"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
	// reasonAutoscalerApplied is the event reason recorded on a workload when its managed autoscaler is
	// created or updated.
	reasonAutoscalerApplied = "AutoscalerApplied"
	// reasonUserAutoscalerPresent is the event reason recorded on a workload when a user-created autoscaler
	// already targets it, so the autoscaler of its WorkloadClass is not applied.
	reasonUserAutoscalerPresent = "UserAutoscalerPresent"
	// reasonAutoscalerNameConflict is the event reason recorded on a workload when an object that is not
	// managed by a WorkloadClass already uses the name of its managed autoscaler.
	reasonAutoscalerNameConflict = "AutoscalerNameConflict"

	actionReconcileAutoscaler = "ReconcileAutoscaler"

	hpaKindName = "HorizontalPodAutoscaler"
)

// WorkloadAutoscalingReconciler reconciles the managed autoscalers of one kind of workload (Deployments or
// StatefulSets) from the resourceManagement of the WorkloadClass that governs each workload.
//
// It is keyed by workload: each workload has exactly one best-match WorkloadClass and owns at most one
// managed HorizontalPodAutoscaler and one managed VerticalPodAutoscaler, so each reconcile is independent and
// idempotent. The workload, not the
// WorkloadClass, is the controller owner of its autoscalers. Deleting the workload garbage-collects them,
// and a workload that moves to another WorkloadClass has them updated in place.
//
// Validation stays with the WorkloadClass controller. This controller only acts on a WorkloadClass whose
// ResourceManagementValidated condition is passing for its current generation.
type WorkloadAutoscalingReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// Kind is the kind of workload that this reconciler handles: utils.DeploymentKind or
	// utils.StatefulSetKind.
	Kind string

	// VPAInstalled reports whether the VerticalPodAutoscaler CRD is installed on the cluster. If it is not,
	// rightsizing is not applied, and VerticalPodAutoscalers are neither read nor watched.
	VPAInstalled bool

	// CoordinatedResources are the resources for which the platform coordinates horizontal scaling on
	// utilization with rightsizing, so that rightsizing is not left out for them (see
	// utils.RightsizedResources).
	CoordinatedResources []corev1.ResourceName

	// inPlaceUnsupported is set once the VPA installation has rejected the InPlaceOrRecreate update mode and
	// accepted Recreate instead. From then on, the InPlacePreferred update strategy uses Recreate directly.
	inPlaceUnsupported atomic.Bool
}

// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling.k8s.io,resources=verticalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses;workloadclassguardrails,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile creates, updates or deletes the managed HorizontalPodAutoscaler and VerticalPodAutoscaler of a
// workload so that they match the autoscaling and rightsizing of the WorkloadClass that governs the workload.
func (r *WorkloadAutoscalingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workload := r.newWorkload()
	if err := r.Get(ctx, req.NamespacedName, workload); err != nil {
		// The autoscalers of a deleted workload are garbage collected through their owner references.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !workload.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, nil
	}
	ref, ok := utils.WorkloadRefFor(workload)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("unsupported workload kind %q", r.Kind)
	}

	wc, err := r.governingWorkloadClass(ctx, ref)
	if err != nil {
		return ctrl.Result{}, err
	}

	// The two autoscalers are independent: a failure to reconcile one never blocks the other.
	return ctrl.Result{}, errors.Join(
		r.reconcileHPA(ctx, workload, ref, wc),
		r.reconcileVPA(ctx, workload, ref, wc),
	)
}

// governingWorkloadClass returns the WorkloadClass whose resourceManagement applies to the workload: the best
// match for its pod template, if it sets resourceManagement and the configuration is validated for its
// current generation. Otherwise it returns nil, and the managed autoscalers of the workload are removed.
func (r *WorkloadAutoscalingReconciler) governingWorkloadClass(ctx context.Context, ref utils.WorkloadRef) (*workloadsv1.WorkloadClass, error) {
	log := logf.FromContext(ctx)

	enforcedWC, err := utils.NamespaceEnforcedWorkloadClass(ctx, r.Client, ref.Namespace)
	if err != nil {
		return nil, err
	}
	wcs := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, wcs, client.InNamespace(ref.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}

	best := utils.BestMatchWorkloadClassForTemplate(ref, enforcedWC, wcs.Items)
	switch {
	case best == nil:
		log.V(1).Info("No WorkloadClass governs the workload")
		return nil, nil
	case best.Spec.ResourceManagement == nil:
		log.V(1).Info("WorkloadClass does not set resourceManagement", "workloadClass", best.Name)
		return nil, nil
	case !utils.IsResourceManagementReady(best):
		log.Info("WorkloadClass resource management is not validated for its current generation; it will not be applied",
			"workloadClass", best.Name)
		return nil, nil
	}
	return best, nil
}

// reconcileHPA makes the managed HorizontalPodAutoscaler of the workload match the autoscaling of the
// WorkloadClass (wc may be nil).
//
//   - A user-created HPA that targets the workload takes precedence: no managed HPA is kept for the workload.
//   - If the WorkloadClass does not ask for an HPA, the managed HPA is deleted. The workload keeps its
//     current number of replicas.
//   - An object that uses the managed name but is not managed by a WorkloadClass, or that another object
//     controls, is never modified.
//   - A managed HPA left behind by a previous workload with the same name is replaced.
func (r *WorkloadAutoscalingReconciler) reconcileHPA(ctx context.Context, workload client.Object, ref utils.WorkloadRef, wc *workloadsv1.WorkloadClass) error {
	log := logf.FromContext(ctx)
	name := utils.ManagedAutoscalerName(ref.Kind, ref.Name)

	desired := utils.BuildHPA(wc, ref)
	if desired != nil {
		userHPA, err := r.userHPAForWorkload(ctx, ref)
		if err != nil {
			return err
		}
		if userHPA != nil {
			log.Info("User-created HorizontalPodAutoscaler targets the workload; WorkloadClass autoscaling will not be applied",
				"horizontalPodAutoscaler", userHPA.Name, "workloadClass", wc.Name)
			r.Recorder.Eventf(workload, userHPA, corev1.EventTypeNormal, reasonUserAutoscalerPresent, actionReconcileAutoscaler,
				"HorizontalPodAutoscaler %s already targets this %s, so the autoscaling of WorkloadClass %s is not applied",
				userHPA.Name, ref.Kind, wc.Name)
			desired = nil
		}
	}
	if desired == nil {
		return r.deleteManagedHPA(ctx, workload, name)
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ref.Namespace}}
	ownership := ownedByWorkload
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, hpa, func() error {
		if ownership = autoscalerOwnership(hpa, workload, ref); ownership != ownedByWorkload {
			return nil
		}
		if hpa.Labels == nil {
			hpa.Labels = map[string]string{}
		}
		maps.Copy(hpa.Labels, desired.Labels)
		hpa.Spec.ScaleTargetRef = desired.Spec.ScaleTargetRef
		hpa.Spec.MinReplicas = desired.Spec.MinReplicas
		hpa.Spec.MaxReplicas = desired.Spec.MaxReplicas
		hpa.Spec.Metrics = desired.Spec.Metrics
		hpa.Spec.Behavior = desired.Spec.Behavior
		return controllerutil.SetControllerReference(workload, hpa, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile HorizontalPodAutoscaler %s: %w", name, err)
	}
	if ownership == ownedByPreviousWorkload {
		return r.deleteStaleAutoscaler(ctx, hpa, hpaKindName)
	}
	if ownership == ownedByOther {
		log.Info("HorizontalPodAutoscaler with the managed name does not belong to the workload; it will not be modified",
			"horizontalPodAutoscaler", name)
		r.Recorder.Eventf(workload, hpa, corev1.EventTypeWarning, reasonAutoscalerNameConflict, actionReconcileAutoscaler,
			"HorizontalPodAutoscaler %s already exists and does not belong to this %s, so the autoscaling of WorkloadClass %s is not applied",
			name, ref.Kind, wc.Name)
		return nil
	}
	if op != controllerutil.OperationResultNone {
		log.Info("Reconciled HorizontalPodAutoscaler", "horizontalPodAutoscaler", name, "operation", op, "workloadClass", wc.Name)
		r.Recorder.Eventf(workload, hpa, corev1.EventTypeNormal, reasonAutoscalerApplied, actionReconcileAutoscaler,
			"HorizontalPodAutoscaler %s %s from the autoscaling of WorkloadClass %s", name, op, wc.Name)
	}
	return nil
}

// autoscalerOwner says who an existing object with the name of a managed autoscaler belongs to.
type autoscalerOwner int

const (
	// ownedByWorkload: the object does not exist yet, or it is a managed autoscaler that the workload
	// controls or that has no controller. It is created, updated or adopted.
	ownedByWorkload autoscalerOwner = iota
	// ownedByPreviousWorkload: a managed autoscaler controlled by an object with the kind and name of the
	// workload but another UID. The workload was deleted and recreated with the same name, and the
	// autoscaler of its previous incarnation has not been garbage collected yet. It is deleted, so that a
	// new one can be created for the current workload.
	ownedByPreviousWorkload
	// ownedByOther: an object that is not managed by a WorkloadClass, or that another object controls. It is
	// never modified or deleted.
	ownedByOther
)

// autoscalerOwnership classifies an existing object with the name of the workload's managed autoscaler.
func autoscalerOwnership(autoscaler, workload client.Object, ref utils.WorkloadRef) autoscalerOwner {
	if autoscaler.GetResourceVersion() == "" {
		return ownedByWorkload
	}
	if !utils.IsWorkloadClassManagedAutoscaler(autoscaler) {
		return ownedByOther
	}
	owner := metav1.GetControllerOf(autoscaler)
	switch {
	case owner == nil || owner.UID == workload.GetUID():
		return ownedByWorkload
	case owner.Kind == ref.Kind && owner.Name == ref.Name:
		return ownedByPreviousWorkload
	default:
		return ownedByOther
	}
}

// deleteStaleAutoscaler deletes a managed autoscaler that belongs to a previous incarnation of the workload.
// Its deletion re-queues the workload, which then gets a new autoscaler.
func (r *WorkloadAutoscalingReconciler) deleteStaleAutoscaler(ctx context.Context, autoscaler client.Object, kind string) error {
	uid := autoscaler.GetUID()
	if err := r.Delete(ctx, autoscaler, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete %s %s of a previous workload: %w", kind, autoscaler.GetName(), err)
	}
	logf.FromContext(ctx).Info("Deleted autoscaler of a previous workload with the same name", "kind", kind, "name", autoscaler.GetName())
	return nil
}

// userHPAForWorkload returns a user-created HorizontalPodAutoscaler that targets the workload, or nil if
// there is none. HPAs without the managed-by label are user-created.
func (r *WorkloadAutoscalingReconciler) userHPAForWorkload(ctx context.Context, ref utils.WorkloadRef) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
	if err := r.List(ctx, hpas, client.InNamespace(ref.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list HorizontalPodAutoscalers: %w", err)
	}
	for i := range hpas.Items {
		hpa := &hpas.Items[i]
		if utils.IsWorkloadClassManagedAutoscaler(hpa) || !hpa.DeletionTimestamp.IsZero() {
			continue
		}
		target := hpa.Spec.ScaleTargetRef
		if utils.ScaleTargetsWorkload(target.APIVersion, target.Kind, target.Name, ref) {
			return hpa, nil
		}
	}
	return nil, nil
}

// deleteManagedHPA deletes the managed HorizontalPodAutoscaler of the workload, if it exists. It never deletes
// an HPA that is not managed by a WorkloadClass or that is controlled by another object.
func (r *WorkloadAutoscalingReconciler) deleteManagedHPA(ctx context.Context, workload client.Object, name string) error {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: workload.GetNamespace()}, hpa); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !utils.IsWorkloadClassManagedAutoscaler(hpa) || !metav1.IsControlledBy(hpa, workload) {
		return nil
	}
	if err := r.Delete(ctx, hpa, client.Preconditions{UID: &hpa.UID}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete HorizontalPodAutoscaler %s: %w", name, err)
	}
	logf.FromContext(ctx).Info("Deleted HorizontalPodAutoscaler", "horizontalPodAutoscaler", name)
	return nil
}

// newWorkload returns an empty workload of the kind that this reconciler handles.
func (r *WorkloadAutoscalingReconciler) newWorkload() client.Object {
	if r.Kind == utils.StatefulSetKind {
		return &appsv1.StatefulSet{}
	}
	return &appsv1.Deployment{}
}

// workloadKeys returns the keys of the workloads of this reconciler's kind in the namespace, or in every
// namespace if it is empty.
func (r *WorkloadAutoscalingReconciler) workloadKeys(ctx context.Context, namespace string) ([]client.ObjectKey, error) {
	var keys []client.ObjectKey
	if r.Kind == utils.StatefulSetKind {
		list := &appsv1.StatefulSetList{}
		if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
			return nil, err
		}
		for i := range list.Items {
			keys = append(keys, client.ObjectKeyFromObject(&list.Items[i]))
		}
		return keys, nil
	}
	list := &appsv1.DeploymentList{}
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	for i := range list.Items {
		keys = append(keys, client.ObjectKeyFromObject(&list.Items[i]))
	}
	return keys, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadAutoscalingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		// Reconcile a workload when it is created or the labels of its pod template change, which can change
		// the WorkloadClass that governs it. Replica changes made by the HPA are ignored.
		For(r.newWorkload(), builder.WithPredicates(workloadTemplateLabelsPredicate())).
		// A WorkloadClass being created, deleted, changing its spec, or changing its resource management
		// validation can change the autoscalers of every workload in its namespace.
		Watches(
			&workloadsv1.WorkloadClass{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadsInObjectNamespace),
			builder.WithPredicates(workloadClassResourceManagementPredicate()),
		).
		// Changing the namespace's enforced WorkloadClass changes which WorkloadClass governs every workload in it.
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadsInNamespace),
			builder.WithPredicates(namespaceEnforcedClassPredicate()),
		).
		// Re-sync managed HPAs that are modified or deleted externally, and yield to (or reclaim from)
		// user-created HPAs as they are created, retargeted, or deleted. HPA status updates are ignored.
		Watches(
			&autoscalingv2.HorizontalPodAutoscaler{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadForAutoscaler),
			builder.WithPredicates(autoscalerChangedPredicate()),
		)

	if r.VPAInstalled {
		// The same for managed and user-created VPAs. Watching a kind that the API server does not serve
		// would keep the manager from starting, so VPAs are only watched when their CRD is installed.
		b = b.Watches(
			newVPA(),
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadForAutoscaler),
			builder.WithPredicates(autoscalerChangedPredicate()),
		).
			// The guardrail containerBounds are the default bounds of managed VPAs, so a guardrail spec
			// change can change the VPA of any workload.
			Watches(
				&workloadsv1.WorkloadClassGuardrail{},
				handler.EnqueueRequestsFromMapFunc(r.findAllWorkloads),
				builder.WithPredicates(predicate.GenerationChangedPredicate{}),
			)
	}

	return b.Named("workloadautoscaling-" + strings.ToLower(r.Kind)).Complete(r)
}

// findAllWorkloads enqueues every workload of this reconciler's kind in the cluster.
func (r *WorkloadAutoscalingReconciler) findAllWorkloads(ctx context.Context, _ client.Object) []reconcile.Request {
	keys, err := r.workloadKeys(ctx, "")
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list workloads", "kind", r.Kind)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(keys))
	for _, key := range keys {
		requests = append(requests, reconcile.Request{NamespacedName: key})
	}
	return requests
}

// findWorkloadsInObjectNamespace enqueues every workload of this reconciler's kind in the object's namespace.
func (r *WorkloadAutoscalingReconciler) findWorkloadsInObjectNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.workloadRequestsInNamespace(ctx, obj.GetNamespace())
}

// findWorkloadsInNamespace enqueues every workload of this reconciler's kind in the Namespace.
func (r *WorkloadAutoscalingReconciler) findWorkloadsInNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.workloadRequestsInNamespace(ctx, obj.GetName())
}

func (r *WorkloadAutoscalingReconciler) workloadRequestsInNamespace(ctx context.Context, namespace string) []reconcile.Request {
	if namespace == "" {
		return nil
	}
	keys, err := r.workloadKeys(ctx, namespace)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list workloads", "kind", r.Kind, "namespace", namespace)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(keys))
	for _, key := range keys {
		requests = append(requests, reconcile.Request{NamespacedName: key})
	}
	return requests
}

// findWorkloadForAutoscaler enqueues the workload that an HPA or VPA belongs to: the controller owner of a
// managed autoscaler, or the target of a user-created one. Autoscalers of other kinds of workloads are
// ignored.
func (r *WorkloadAutoscalingReconciler) findWorkloadForAutoscaler(_ context.Context, obj client.Object) []reconcile.Request {
	if utils.IsWorkloadClassManagedAutoscaler(obj) {
		owner := metav1.GetControllerOf(obj)
		if owner == nil || owner.Kind != r.Kind {
			return nil
		}
		return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: owner.Name, Namespace: obj.GetNamespace()}}}
	}

	var apiVersion, kind, name string
	switch autoscaler := obj.(type) {
	case *autoscalingv2.HorizontalPodAutoscaler:
		target := autoscaler.Spec.ScaleTargetRef
		apiVersion, kind, name = target.APIVersion, target.Kind, target.Name
	case *unstructured.Unstructured:
		apiVersion, _, _ = unstructured.NestedString(autoscaler.Object, "spec", "targetRef", "apiVersion")
		kind, _, _ = unstructured.NestedString(autoscaler.Object, "spec", "targetRef", "kind")
		name, _, _ = unstructured.NestedString(autoscaler.Object, "spec", "targetRef", "name")
	default:
		return nil
	}
	if !utils.ScaleTargetsWorkload(apiVersion, kind, name, utils.WorkloadRef{Kind: r.Kind, Name: name}) {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: name, Namespace: obj.GetNamespace()}}}
}

// workloadTemplateLabelsPredicate passes the creation of a workload and changes to the labels of its pod
// template. Deletion is ignored, because the autoscalers of a workload are garbage collected with it.
func workloadTemplateLabelsPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldRef, okOld := utils.WorkloadRefFor(e.ObjectOld)
			newRef, okNew := utils.WorkloadRefFor(e.ObjectNew)
			if !okOld || !okNew {
				return false
			}
			return !maps.Equal(oldRef.TemplateLabels, newRef.TemplateLabels)
		},
	}
}

// workloadClassResourceManagementPredicate passes WorkloadClass creation and deletion, spec changes, and
// changes to the ResourceManagementValidated condition. Other status updates, such as maintenance readiness
// changes, do not re-queue the workloads of the namespace.
func workloadClassResourceManagementPredicate() predicate.Funcs {
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
			if oldWC.Generation != newWC.Generation {
				return true
			}
			return !sameValidation(
				meta.FindStatusCondition(oldWC.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated),
				meta.FindStatusCondition(newWC.Status.Conditions, workloadsv1.ConditionTypeResourceManagementValidated),
			)
		},
	}
}

// sameValidation reports whether two validation conditions gate consumers in the same way.
func sameValidation(a, b *metav1.Condition) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Status == b.Status && a.Reason == b.Reason && a.ObservedGeneration == b.ObservedGeneration
}

// autoscalerChangedPredicate passes the creation and deletion of autoscalers, and updates that change their
// spec, labels or owner references. It ignores status updates, which autoscaler controllers write
// continuously.
func autoscalerChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !maps.Equal(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) ||
				!equality.Semantic.DeepEqual(e.ObjectOld.GetOwnerReferences(), e.ObjectNew.GetOwnerReferences()) ||
				e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero() {
				return true
			}
			oldHPA, okOld := e.ObjectOld.(*autoscalingv2.HorizontalPodAutoscaler)
			newHPA, okNew := e.ObjectNew.(*autoscalingv2.HorizontalPodAutoscaler)
			if okOld && okNew {
				return !equality.Semantic.DeepEqual(oldHPA.Spec, newHPA.Spec)
			}
			oldVPA, okOld := e.ObjectOld.(*unstructured.Unstructured)
			newVPA, okNew := e.ObjectNew.(*unstructured.Unstructured)
			if okOld && okNew {
				return !equality.Semantic.DeepEqual(oldVPA.Object["spec"], newVPA.Object["spec"])
			}
			return true
		},
	}
}
