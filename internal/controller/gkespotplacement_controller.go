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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
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
// +kubebuilder:rbac:groups=workloads.gke.io,resources=gkespotplacementpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;nodes,verbs=get;list;watch

// Reconcile runs the GKE Spot placement runtime loop for a single WorkloadClass.
func (r *GKESpotPlacementReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	wc := &workloadsv1.WorkloadClass{}
	if err := r.Get(ctx, req.NamespacedName, wc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !utils.IsGKESpotManaged(wc) {
		// The predicate filters these out on events, but a WorkloadClass can be requeued from a watch mapping
		// after its spec changed. Nothing to do.
		return ctrl.Result{}, nil
	}

	// Only act once the central controller has validated the WorkloadClass and, when a GKESpotPlacementPolicy
	// is referenced, once the policy controller has attached it. Until then the Pod placement webhook does not
	// mutate this WorkloadClass's Pods either.
	if !meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeValidated) {
		log.V(1).Info("WorkloadClass is not validated; skipping GKE Spot placement reconcile")
		return ctrl.Result{}, nil
	}
	if utils.ReferencesGKESpotPlacementPolicy(wc.Spec.InfrastructureProfileRef) &&
		!meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached) {
		log.V(1).Info("GKESpotPlacementPolicy is not attached; skipping GKE Spot placement reconcile")
		return ctrl.Result{}, nil
	}

	// TODO: InFallback tracking.

	// Spot capacity drives reversion; for now the result is only logged. A failed
	// check is logged rather than returned so it never blocks the rest of the loop.
	if utils.TargetsSpot(wc) {
		spotCapacityAvailable, err := r.checkSpotCapacity(ctx)
		if err != nil {
			log.Error(err, "Failed to check Spot capacity")
		} else {
			log.V(1).Info("Checked Spot capacity", "spotCapacityAvailable", spotCapacityAvailable)
		}
	}

	// TODO: Active reversion.
	log.V(1).Info("Reconciled GKE Spot placement")

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *GKESpotPlacementReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClass{}, builder.WithPredicates(gkeSpotManagedPredicate())).
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
		Named("gkespotplacement").
		Complete(r)
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
