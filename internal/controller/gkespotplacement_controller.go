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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// GKESpotPlacementReconciler is the runtime half of the gke-spot-placement plugin. It is keyed on
// WorkloadClass and owns the GKE-specific Spot placement behavior that happens after a WorkloadClass has been
// validated and (if it references one) its GKESpotPlacementPolicy has attached:
//
//   - Spot capacity detection (kube-system/cluster-autoscaler-status, Spot node readiness).
//   - Tracking the InFallback condition on WorkloadClass status.
//   - Active reversion: evicting Pods from On-Demand fallback nodes once Spot capacity returns.
//
// The central WorkloadClassReconciler stays provider-agnostic (guardrail validation, PlacementPluginAttached
// bookkeeping, maintenance readiness, PDB management). The GKESpotPlacementPolicyReconciler validates policies
// and attaches them to WorkloadClasses. This reconciler handles the WorkloadClasses whose Spot placement the
// plugin is responsible for: those with a CapacityStrategy that reference a GKESpotPlacementPolicy or have no
// InfrastructureProfileRef at all (matching the Pod placement webhook).
type GKESpotPlacementReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=gkespotplacementpolicies;workloadclassguardrails,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create

// Reconcile runs the GKE Spot placement runtime loop for a single WorkloadClass.
func (r *GKESpotPlacementReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	wc := &workloadsv1.WorkloadClass{}
	if err := r.Get(ctx, req.NamespacedName, wc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !utils.IsGKESpotManaged(wc) {
		// The predicate filters these out on events, but a WorkloadClass can be requeued from a watch mapping,
		// or have just left the plugin's scope via a spec change. Drop any status this plugin owns.
		return ctrl.Result{}, r.clearFallbackState(ctx, wc)
	}

	// Only act once the central controller has validated the WorkloadClass and, when a GKESpotPlacementPolicy
	// is referenced, once the policy controller has attached it. Until then the Pod placement webhook does not
	// mutate this WorkloadClass's Pods either, so any InFallback condition would be stale and is removed.
	//
	// The exception is PluginPending: the central controller resets PlacementPluginAttached to that state on
	// every generation bump until the policy controller reconciles the new generation. That is transient and
	// happens on any spec edit, so the condition (and with it a latched None fallback) is left untouched.
	if !isReady(wc) {
		if isPlacementPluginPending(wc) {
			log.V(1).Info("Placement plugin is pending; leaving GKE Spot placement state untouched")
			return ctrl.Result{}, nil
		}
		log.V(1).Info("WorkloadClass is not validated or its policy is not attached; clearing GKE Spot placement state")
		return ctrl.Result{}, r.clearFallbackState(ctx, wc)
	}

	var policy *workloadsv1.GKESpotPlacementPolicy
	if tracksFallback(wc) {
		var err error
		policy, err = r.attachedPolicy(ctx, wc)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// Track whether Spot-targeted Pods have fallen back to On-Demand (InFallback condition).
	pods, err := r.reconcileFallbackState(ctx, wc, policy)
	if err != nil {
		log.Error(err, "Failed to reconcile Spot fallback state")
		return ctrl.Result{}, err
	}

	// Spot reversion: when Pods are running on On-Demand fallback, evict them once Spot capacity is back so
	// they are recreated on Spot.
	requeue, err := r.reconcileSpotReversion(ctx, wc, policy, pods, time.Now().UTC())
	if err != nil {
		log.Error(err, "Failed to reconcile Spot reversion")
		return ctrl.Result{}, err
	}

	log.V(1).Info("Reconciled GKE Spot placement", "requeueAfter", requeue)
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *GKESpotPlacementReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClass{}, builder.WithPredicates(gkeSpotManagedPredicate())).
		Watches(
			&workloadsv1.GKESpotPlacementPolicy{}, // Re-evaluate fallback/reversion when the referenced policy changes
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesForPolicy),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Watches(
			&workloadsv1.WorkloadClassGuardrail{}, // Re-evaluate maxFallbackDuration when guardrails change
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesForGuardrail),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Watches(
			&corev1.ConfigMap{}, // Re-check Spot capacity when kube-system/cluster-autoscaler-status changes
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesForClusterAutoscalerStatus),
			builder.WithPredicates(clusterAutoscalerStatusPredicate()),
		).
		Watches(
			&corev1.Node{}, // Re-check Spot capacity when a Spot node's schedulable+Ready state changes
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesForSpotNode),
			builder.WithPredicates(spotNodeReadinessPredicate()),
		).
		Watches(
			&corev1.Pod{}, // Re-evaluate InFallback when Pods are bound, terminate, or are deleted
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassForPod),
			builder.WithPredicates(podPlacementChangePredicate()),
		).
		Named("gkespotplacement").
		Complete(r)
}

// isReady reports whether the central controller has validated the WorkloadClass and, when it references a
// GKESpotPlacementPolicy, whether the policy controller has attached it. This is the same gate the Pod
// placement webhook applies before mutating Pods.
func isReady(wc *workloadsv1.WorkloadClass) bool {
	if !meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeValidated) {
		return false
	}
	return !utils.ReferencesGKESpotPlacementPolicy(wc.Spec.InfrastructureProfileRef) ||
		meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached)
}

// isPlacementPluginPending reports whether the WorkloadClass is waiting for its placement plugin to (re)attach,
// i.e. PlacementPluginAttached is False with reason PluginPending.
func isPlacementPluginPending(wc *workloadsv1.WorkloadClass) bool {
	cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached)
	return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == workloadsv1.ReasonPluginPending
}

// gkeSpotManagedPredicate passes WorkloadClass events for WorkloadClasses the gke-spot-placement plugin is
// responsible for. Updates also pass when a WorkloadClass stops being managed, so the reconciler can observe
// the transition (e.g. to clean up status it owns).
func gkeSpotManagedPredicate() predicate.Predicate {
	managed := func(obj client.Object) bool {
		wc, ok := obj.(*workloadsv1.WorkloadClass)
		return ok && utils.IsGKESpotManaged(wc)
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return managed(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return managed(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return managed(e.ObjectOld) || managed(e.ObjectNew) },
	}
}
