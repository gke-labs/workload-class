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
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	"sigs.k8s.io/controller-runtime/pkg/source"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// WorkloadClassReconciler reconciles a WorkloadClass object
type WorkloadClassReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// LeaseTracker holds Per-Pod PDB lease requests from the disruption webhook. The reconciler watches its
	// event channel and applies the pending leases of a WorkloadClass when reconciling it. If nil, no leases
	// are applied.
	LeaseTracker *utils.PendingLeaseTracker
}

// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses/finalizers,verbs=update
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop.
func (r *WorkloadClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	wc := &workloadsv1.WorkloadClass{}
	if err := r.Get(ctx, req.NamespacedName, wc); err != nil {
		if apierrors.IsNotFound(err) {
			// Discard any lease requests for a deleted WorkloadClass.
			r.LeaseTracker.Drain(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1.1 Fetch Guardrails and validate
	validationCond, err := r.validateAgainstGuardrails(ctx, wc)
	if err != nil {
		log.Error(err, "Failed to validate against guardrails")
		return ctrl.Result{}, err
	}

	// 1.2 Check for plugin placement attachment conditions
	changed, removed := false, false
	pluginPlacementCondition := placementPluginCondition(ctx, wc)
	if pluginPlacementCondition != nil {
		changed = meta.SetStatusCondition(&wc.Status.Conditions, *pluginPlacementCondition)
	} else {
		// Clean up stale PluginPlacementAttached condition
		removed = meta.RemoveStatusCondition(&wc.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached)
	}

	// 1.3 If the plugin has not attached, mark the WorkloadClass as not yet validated
	reconcileConditions(&validationCond, pluginPlacementCondition)
	changed = meta.SetStatusCondition(&wc.Status.Conditions, validationCond) || changed

	// 2. Check if other existing WorkloadClasses have the same PodSelector
	if _, err := r.validateSelectors(ctx, wc); err != nil {
		// Emit a warning event
		r.Recorder.Eventf(
			wc,
			nil,
			corev1.EventTypeWarning,
			"ValidationFailed",
			"SelectorValidation",
			"%s",
			err.Error(),
		)
	}

	// 2. Calculate Readiness
	readiness, nextReconcile, err := r.calculateReadiness(ctx, wc)
	if err != nil {
		log.Error(err, "Failed to calculate readiness")
		return ctrl.Result{}, err
	}

	// 3. Update Status
	if wc.Status.MaintenanceReadiness != readiness || changed || removed {
		wc.Status.MaintenanceReadiness = readiness
		log.Info(fmt.Sprintf("Workload is now %s for maintenance", readiness))
		if err := r.Status().Update(ctx, wc); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 4. Reconcile the Per-Pod PDBs, applying any leases requested by the disruption webhook.
	// Leases are drained only now, so earlier failures (e.g., status update conflicts) do not discard them.
	pendingLeases := r.LeaseTracker.Drain(req.NamespacedName)
	leaseRequeue, err := r.reconcilePDBs(ctx, wc, validationCond, pendingLeases)
	if err != nil {
		r.Recorder.Eventf(
			wc,
			nil,
			corev1.EventTypeWarning,
			"ReconcilePDBFailed",
			"Reconciling PodDisruptionBudget",
			"Failed to reconcile PDB: %s",
			err.Error(),
		)
		return ctrl.Result{}, err
	}

	// Requeue at the next readiness change, or when the earliest active lease expires so its PDB is re-synced.
	return ctrl.Result{RequeueAfter: minPositiveDuration(nextReconcile, leaseRequeue)}, nil
}

// reconcilePDBs reconciles the Per-Pod PDBs (workload-<pod.Name>) of the Pods governed by the WorkloadClass.
//
// A Pod is governed by the WorkloadClass if the WorkloadClass is its best match (the namespace default, or the
// most specific matching WorkloadClass) and the Pod is not already covered by a user-created PDB.
//
//   - If the WorkloadClass is invalid, its existing PDBs are opened (MaxUnavailable = 100%) rather than deleted.
//   - Pending leases from the disruption webhook are applied to the PDBs of the requested Pods.
//   - PDBs with an active lease are left untouched until the lease expires.
//   - Owned PDBs whose Pod no longer exists, is terminating or in a terminal phase, is covered by a user-created PDB,
//     or is no longer governed by any valid WorkloadClass are deleted. PDBs of Pods governed by another WorkloadClass
//     are left for that WorkloadClass to adopt.
//
// It returns the time until the earliest active lease expires, or 0 if there are none.
func (r *WorkloadClassReconciler) reconcilePDBs(ctx context.Context, wc *workloadsv1.WorkloadClass, condition metav1.Condition, pendingLeases []utils.PendingLease) (time.Duration, error) {
	log := logf.FromContext(ctx)

	if !wc.DeletionTimestamp.IsZero() {
		// Owned PDBs are garbage collected through their owner references.
		log.Info("WorkloadClass is being deleted, skipping PDB reconciliation")
		return 0, nil
	}

	pdbs := &policyv1.PodDisruptionBudgetList{}
	if err := r.List(ctx, pdbs, client.InNamespace(wc.Namespace)); err != nil {
		r.reenqueueLeases(wc, pendingLeases...)
		return 0, fmt.Errorf("failed to list PodDisruptionBudgets: %w", err)
	}
	var owned, userPDBs []policyv1.PodDisruptionBudget
	for _, pdb := range pdbs.Items {
		switch {
		case metav1.IsControlledBy(&pdb, wc):
			owned = append(owned, pdb)
		case !utils.IsWorkloadClassManagedPDB(&pdb):
			userPDBs = append(userPDBs, pdb)
		}
	}

	if condition.Reason != workloadsv1.ReasonValidationPassed {
		log.Info("WorkloadClass is invalid, opening its PDBs")
		return 0, r.openOwnedPDBs(ctx, wc, owned, userPDBs)
	}

	defaultWC, err := utils.NamespaceDefaultWorkloadClass(ctx, r.Client, wc.Namespace)
	if err != nil {
		r.reenqueueLeases(wc, pendingLeases...)
		return 0, err
	}
	wcs := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, wcs, client.InNamespace(wc.Namespace)); err != nil {
		r.reenqueueLeases(wc, pendingLeases...)
		return 0, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}

	governed, err := r.governedPods(ctx, wc, defaultWC, wcs.Items, userPDBs)
	if err != nil {
		r.reenqueueLeases(wc, pendingLeases...)
		return 0, err
	}

	leases := make(map[string]utils.PendingLease, len(pendingLeases))
	for _, lease := range pendingLeases {
		leases[lease.PodName] = lease
	}

	var errs []error
	var requeue time.Duration
	governedPDBs := make(map[string]bool, len(governed))
	for _, pod := range governed {
		governedPDBs[utils.PDBName(pod.Name)] = true
		remaining, err := r.reconcilePodPDB(ctx, wc, pod, leases)
		if err != nil {
			if lease, ok := leases[pod.Name]; ok && lease.PodUID == pod.UID {
				r.reenqueueLeases(wc, lease)
			}
		}
		errs = append(errs, err)
		requeue = minPositiveDuration(requeue, remaining)
	}

	for i := range owned {
		pdb := &owned[i]
		if governedPDBs[pdb.Name] {
			continue
		}

		pod, err := r.getPodForPDB(ctx, wc.Namespace, pdb)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if utils.IsPodActive(pod) && !utils.IsPodCoveredByUserPDB(pod, userPDBs) {
			if bestMatch := utils.BestMatchWorkloadClassForPod(pod, defaultWC, wcs.Items); bestMatch != nil && bestMatch.Name != wc.Name && passedValidation(bestMatch) {
				// Another WorkloadClass governs the Pod now and will adopt its PDB.
				continue
			}
		}
		errs = append(errs, r.deletePDB(ctx, wc, pdb))
	}

	return requeue, errors.Join(errs...)
}

// governedPods returns the active Pods governed by the WorkloadClass that are not covered by a user-created PDB.
func (r *WorkloadClassReconciler) governedPods(ctx context.Context, wc, defaultWC *workloadsv1.WorkloadClass, wcs []workloadsv1.WorkloadClass, userPDBs []policyv1.PodDisruptionBudget) ([]*corev1.Pod, error) {
	if defaultWC != nil && defaultWC.Name != wc.Name {
		// The namespace default WorkloadClass governs every Pod in the namespace.
		return nil, nil
	}

	selector := labels.Everything()
	if defaultWC == nil {
		if wc.Spec.PodSelector == nil {
			// A WorkloadClass without a PodSelector is never the best match for a Pod.
			return nil, nil
		}
		s, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid podSelector: %w", err)
		}
		selector = s
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list Pods: %w", err)
	}

	var governed []*corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !utils.IsPodActive(pod) || utils.IsPodCoveredByUserPDB(pod, userPDBs) {
			continue
		}
		if bestMatch := utils.BestMatchWorkloadClassForPod(pod, defaultWC, wcs); bestMatch == nil || bestMatch.Name != wc.Name {
			continue
		}
		governed = append(governed, pod)
	}
	return governed, nil
}

// reconcilePodPDB reconciles the PDB of a single governed Pod and returns the remaining time of its active lease,
// or 0 if it has none.
func (r *WorkloadClassReconciler) reconcilePodPDB(ctx context.Context, wc *workloadsv1.WorkloadClass, pod *corev1.Pod, leases map[string]utils.PendingLease) (time.Duration, error) {
	log := logf.FromContext(ctx).WithValues("pod", pod.Name)

	if err := r.ensurePodUIDLabel(ctx, pod); err != nil {
		return 0, err
	}

	existing := &policyv1.PodDisruptionBudget{}
	if err := r.Get(ctx, client.ObjectKey{Name: utils.PDBName(pod.Name), Namespace: pod.Namespace}, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("failed to get PDB for Pod %s: %w", pod.Name, err)
		}
		existing = nil
	}

	if lease, ok := leases[pod.Name]; ok && lease.PodUID == pod.UID {
		switch {
		case existing != nil && utils.SubjectAlreadyLeasing(existing, pod, lease.Subject):
			return utils.LeaseTimeRemaining(existing), nil
		case existing == nil || utils.AllowLease(existing, pod):
			if err := r.leasePodPDB(ctx, wc, pod, lease.Subject); err != nil {
				return 0, err
			}
			log.Info("Leased PDB", "subject", lease.Subject)
			return utils.LeaseDurationForPod(pod), nil
		default:
			log.Info("Discarding lease request, another subject holds an active lease", "subject", lease.Subject, "owner", existing.Annotations[utils.BypassOwner])
		}
	}

	if remaining := activeLeaseRemaining(existing, pod); remaining > 0 {
		// Leave the leased PDB open until the lease expires.
		return remaining, nil
	}

	return 0, r.syncPodPDB(ctx, wc, pod, true)
}

// activeLeaseRemaining returns the remaining time of an active lease on the PDB for this Pod instance, or 0.
func activeLeaseRemaining(pdb *policyv1.PodDisruptionBudget, pod *corev1.Pod) time.Duration {
	if pdb == nil || pdb.Labels[utils.PodUIDLabel] != string(pod.UID) || pdb.Annotations[utils.BypassOwner] == "" {
		return 0
	}
	return utils.LeaseTimeRemaining(pdb)
}

func (r *WorkloadClassReconciler) reenqueueLeases(wc *workloadsv1.WorkloadClass, leases ...utils.PendingLease) {
	for _, lease := range leases {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: lease.PodName, Namespace: wc.Namespace, UID: lease.PodUID}}
		r.LeaseTracker.Enqueue(wc, pod, lease.Subject)
	}
}

// openOwnedPDBs opens the PDBs owned by an invalid WorkloadClass, and deletes those whose Pod no longer exists,
// is terminating or in a terminal phase, or is covered by a user-created PDB.
func (r *WorkloadClassReconciler) openOwnedPDBs(ctx context.Context, wc *workloadsv1.WorkloadClass, owned, userPDBs []policyv1.PodDisruptionBudget) error {
	var errs []error
	for i := range owned {
		pdb := &owned[i]
		pod, err := r.getPodForPDB(ctx, wc.Namespace, pdb)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !utils.IsPodActive(pod) || utils.IsPodCoveredByUserPDB(pod, userPDBs) {
			errs = append(errs, r.deletePDB(ctx, wc, pdb))
			continue
		}
		errs = append(errs, r.syncPodPDB(ctx, wc, pod, false))
	}
	return errors.Join(errs...)
}

// ensurePodUIDLabel labels the Pod with workloads.gke.io/pod-uid so that its Per-Pod PDB can select it.
func (r *WorkloadClassReconciler) ensurePodUIDLabel(ctx context.Context, pod *corev1.Pod) error {
	if pod.Labels[utils.PodUIDLabel] == string(pod.UID) {
		return nil
	}
	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[utils.PodUIDLabel] = string(pod.UID)
	if err := r.Patch(ctx, pod, patch); err != nil {
		return fmt.Errorf("failed to label Pod %s: %w", pod.Name, err)
	}
	return nil
}

// syncPodPDB creates or updates the Pod's PDB to match the WorkloadClass' disruption policy.
func (r *WorkloadClassReconciler) syncPodPDB(ctx context.Context, wc *workloadsv1.WorkloadClass, pod *corev1.Pod, isValid bool) error {
	pdb := utils.PDBBase(pod)
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := r.claimPDB(wc, pdb); err != nil {
			return err
		}
		return utils.SyncPDBWithWorkloadClass(wc, pod, pdb, isValid)
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile PDB %s: %w", pdb.Name, err)
	}
	logf.FromContext(ctx).V(1).Info("Reconciled PDB", "pdb", pdb.Name, "operation", op)
	return nil
}

// leasePodPDB creates or updates the Pod's PDB with a lease for the subject.
func (r *WorkloadClassReconciler) leasePodPDB(ctx context.Context, wc *workloadsv1.WorkloadClass, pod *corev1.Pod, subject workloadsv1.Subject) error {
	pdb := utils.PDBBase(pod)
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := r.claimPDB(wc, pdb); err != nil {
			return err
		}
		return utils.PDBWithLease(pdb, pod, subject)
	}); err != nil {
		return fmt.Errorf("failed to lease PDB %s: %w", pdb.Name, err)
	}
	return nil
}

// claimPDB sets the WorkloadClass as the controller of the PDB, taking over ownership from another WorkloadClass
// if needed (e.g., when the Pod's best-match WorkloadClass changes). It refuses to modify a PDB that exists but is
// not managed by a WorkloadClass.
func (r *WorkloadClassReconciler) claimPDB(wc *workloadsv1.WorkloadClass, pdb *policyv1.PodDisruptionBudget) error {
	if pdb.ResourceVersion != "" && !utils.IsWorkloadClassManagedPDB(pdb) {
		return fmt.Errorf("PDB %s/%s already exists and is not managed by a WorkloadClass", pdb.Namespace, pdb.Name)
	}
	if owner := metav1.GetControllerOf(pdb); owner != nil && owner.UID != wc.UID {
		refs := make([]metav1.OwnerReference, 0, len(pdb.OwnerReferences))
		for _, ref := range pdb.OwnerReferences {
			if ref.Controller == nil || !*ref.Controller {
				refs = append(refs, ref)
			}
		}
		pdb.OwnerReferences = refs
	}
	return controllerutil.SetControllerReference(wc, pdb, r.Scheme)
}

// deletePDB deletes a PDB if it is still controlled by the WorkloadClass. The resourceVersion precondition
// prevents deleting a PDB that another WorkloadClass adopted after it was read.
func (r *WorkloadClassReconciler) deletePDB(ctx context.Context, wc *workloadsv1.WorkloadClass, pdb *policyv1.PodDisruptionBudget) error {
	current := &policyv1.PodDisruptionBudget{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pdb), current); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(current, wc) {
		return nil
	}
	if err := r.Delete(ctx, current, client.Preconditions{UID: &current.UID, ResourceVersion: &current.ResourceVersion}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete PDB %s: %w", pdb.Name, err)
	}
	logf.FromContext(ctx).Info("Deleted PDB", "pdb", pdb.Name)
	return nil
}

// getPod returns the Pod, or nil if it does not exist.
func (r *WorkloadClassReconciler) getPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get Pod %s: %w", name, err)
	}
	return pod, nil
}

// getPodForPDB returns the Pod targeted by a WorkloadClass-managed Per-Pod PDB, or nil if it does not exist.
// For PDB names that were truncated to MaxPDBNameLength, it falls back to matching Pods in the namespace by
// utils.PDBName(pod.Name) if the direct lookup by stripped prefix finds no Pod.
func (r *WorkloadClassReconciler) getPodForPDB(ctx context.Context, namespace string, pdb *policyv1.PodDisruptionBudget) (*corev1.Pod, error) {
	pod, err := r.getPod(ctx, namespace, utils.PodNameFromPDBName(pdb.Name))
	if err != nil || pod != nil || len(pdb.Name) < utils.MaxPDBNameLength {
		return pod, err
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list Pods in namespace %s: %w", namespace, err)
	}
	for i := range pods.Items {
		if utils.PDBName(pods.Items[i].Name) == pdb.Name {
			return &pods.Items[i], nil
		}
	}
	return nil, nil
}

// passedValidation returns true if the WorkloadClass' status shows it passed guardrail validation.
func passedValidation(wc *workloadsv1.WorkloadClass) bool {
	cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeValidated)
	return cond != nil && cond.Reason == workloadsv1.ReasonValidationPassed
}

func (r *WorkloadClassReconciler) calculateReadiness(ctx context.Context, wc *workloadsv1.WorkloadClass) (workloadsv1.MaintenanceReadiness, time.Duration, error) {
	log := logf.FromContext(ctx)
	now := time.Now().UTC()

	// 1. Emergency Override
	if wc.Spec.DisruptionPolicy.EmergencyOverride {
		return workloadsv1.ReadinessReady, 0, nil
	}

	// 2. Check Overdue (Maximum Protected Duration)
	if overdue(wc, now) {
		log.Info(fmt.Sprintf("Time since last disruption for WorkloadClass %s/%s exceeds MaxNonDisruptionDurationDays. WorkloadClass is overdue for maintenance", wc.Namespace, wc.Name))
		return workloadsv1.ReadinessOverdue, 0, nil
	}

	// 3. Check Temporal Windows
	inWindow, nextWindow := utils.IsTimeInWindows(ctx, now, wc.Spec.DisruptionPolicy.AllowedDisruptionWindows)
	if !inWindow {
		return workloadsv1.ReadinessNotReady, nextWindow, nil
	}

	// Get pods for the next two checks
	pods := &corev1.PodList{}
	selector, err := utils.WorkloadClassPodSelector(ctx, r.Client, wc)
	if err != nil {
		return workloadsv1.ReadinessNotReady, 0, err
	}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return workloadsv1.ReadinessNotReady, 0, err
	}
	defaultWC, err := utils.NamespaceDefaultWorkloadClass(ctx, r.Client, wc.Namespace)
	if err != nil {
		return workloadsv1.ReadinessNotReady, 0, err
	}
	wcs := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, wcs, client.InNamespace(wc.Namespace)); err != nil {
		return workloadsv1.ReadinessNotReady, 0, err
	}

	var matchedPods []corev1.Pod
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if bestMatch := utils.BestMatchWorkloadClassForPod(&pod, defaultWC, wcs.Items); bestMatch == nil || bestMatch.Name != wc.Name {
			continue
		}
		matchedPods = append(matchedPods, pod)
	}

	// 4. Check Pod Ages (Min Initial Run) and if grace periods have passed (GraceTerminationDuration)
	if wc.Spec.DisruptionPolicy.MinInitialRunDurationDays > 0 {
		minRunDuration := time.Duration(wc.Spec.DisruptionPolicy.MinInitialRunDurationDays) * 24 * time.Hour
		for _, pod := range matchedPods {
			if !utils.IsPodActive(&pod) {
				continue
			}
			if now.Sub(pod.CreationTimestamp.Time) < minRunDuration {
				// Pod hasn't run long enough
				return workloadsv1.ReadinessNotReady, minRunDuration - now.Sub(pod.CreationTimestamp.Time), nil
			}
		}
	}

	// 5. Check if grace period has passed for all pods (GraceTerminationDuration)
	if wc.Spec.DisruptionPolicy.GraceTerminationDuration > 0 {
		gracePeriodsPassed := true
		maxTimeForGracePeriod := 0 * time.Second
		for _, pod := range matchedPods {
			// Check the grace period has passed for this pod. We want all grace periods to have passed.
			gracePeriodsPassed, maxTimeForGracePeriod = evaluatePodGracePeriod(wc, &pod, now, gracePeriodsPassed, maxTimeForGracePeriod)
		}

		if !gracePeriodsPassed {
			// Grace periods have not passed
			return workloadsv1.ReadinessNotReady, maxTimeForGracePeriod, nil
		}
	}

	return workloadsv1.ReadinessReady, nextWindow, nil
}

// placementPluginCondition returns a PlacementPluginAttached metav1.Condition from the WorkloadClass status if it matches
// the current Generation. If no such Condition exists, a new one is created. If the WorkloadClass does not have an
// InfrastructureProfileRef, no Condition is returned.
func placementPluginCondition(ctx context.Context, wc *workloadsv1.WorkloadClass) *metav1.Condition {
	log := logf.FromContext(ctx)
	if wc.Spec.InfrastructureProfileRef == nil {
		log.Info(fmt.Sprintf("WorkloadClass %s/%s does not have an InfrastructureProfileRef set", wc.Namespace, wc.Name))
		return nil
	}

	if existing := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached); existing != nil && existing.ObservedGeneration == wc.Generation {
		log.Info(fmt.Sprintf("WorkloadClass %s/%s has a Placement Plugin condition. Status: %s, reason: %s, message: %s", wc.Namespace, wc.Name, existing.Status, existing.Reason, existing.Message))
		cond := *existing
		return &cond
	}

	// The placement plugin has not attached for the current generation
	return &metav1.Condition{
		Type:               workloadsv1.ConditionTypePlacementPluginAttached,
		Status:             metav1.ConditionFalse,
		Reason:             workloadsv1.ReasonPluginPending,
		Message:            fmt.Sprintf("Waiting for plugin controller to reconcile %s", wc.Spec.InfrastructureProfileRef.Kind),
		ObservedGeneration: wc.Generation,
		LastTransitionTime: metav1.Now(),
	}
}

const (
	placementUnattachedMsg = "WorkloadClass references an infrastructure profile, but the plugin has not attached"
	validationPassedMsg    = "WorkloadClass adheres to all Guardrail constraints"
)

func reconcileConditions(validation, placement *metav1.Condition) {
	if validation == nil || placement == nil || placement.Status == metav1.ConditionTrue {
		return
	}

	addPlacementViolation(validation, placement)
}

func addPlacementViolation(validation, placement *metav1.Condition) {
	violationMsg := placementUnattachedMsg
	if placement.Message != "" {
		violationMsg = fmt.Sprintf("%s: %s", placementUnattachedMsg, placement.Message)
	}

	if validation.Status == metav1.ConditionFalse {
		validation.Message = validation.Message + "; " + violationMsg
	} else {
		validation.Message = violationMsg
	}

	validation.Status = metav1.ConditionFalse
	validation.Reason = workloadsv1.ReasonValidationFailed
	validation.LastTransitionTime = metav1.Now()
}

func overdue(wc *workloadsv1.WorkloadClass, now time.Time) bool {
	if wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays > 0 {
		maxDuration := time.Duration(wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays) * 24 * time.Hour
		if wc.Status.LastDisruptionTime != nil {
			return now.Sub(wc.Status.LastDisruptionTime.Time) > maxDuration
		}
		return now.Sub(wc.CreationTimestamp.Time) > maxDuration
	}
	return true
}

func evaluatePodGracePeriod(wc *workloadsv1.WorkloadClass, pod *corev1.Pod, now time.Time, gracePeriodsPassed bool, maxDuration time.Duration) (bool, time.Duration) {
	gracePeriodPassedForPod, timeUntilGracePeriodPassed := gracePeriodPassed(wc, pod, now)

	gracePeriodsPassed = gracePeriodsPassed && gracePeriodPassedForPod
	maxDurationForGracePeriod := max(maxDuration, timeUntilGracePeriodPassed)

	return gracePeriodsPassed, maxDurationForGracePeriod
}

// gracePeriodPassed returns true if the GraceTerminationDuration has passed for the Pod, indicating that maintenance should not be blocked.
//
// It checks if the pod has a DeletionTimestamp, which indicates when the pod began deletion.
// The function then compares the WorkloadClass' GraceTerminationDuration against the time passed since the DeletionTimestamp.
//
// If the time passed is not greater than or equal to the GraceTerminationDuration, the function returns false and the time remaining until the grace period expires.
// If the time passed is greater than or equal to the GraceTerminationDuration, the function returns, and the WorkloadClass is marked Ready for maintenance.
func gracePeriodPassed(wc *workloadsv1.WorkloadClass, pod *corev1.Pod, now time.Time) (bool, time.Duration) {
	gracePeriod := wc.Spec.DisruptionPolicy.GraceTerminationDuration

	// Check if Pod's deletion timestamp is set
	if pod.DeletionTimestamp == nil {
		return true, time.Duration(0) * time.Second
	}

	// Check if the grace period has passed
	// GraceTerminationDuration - time passed since deletion timestamp
	diff := (time.Duration(gracePeriod) * time.Second) - now.Sub(pod.DeletionTimestamp.Time)
	return diff <= 0, diff
}

func (r *WorkloadClassReconciler) validateAgainstGuardrails(ctx context.Context, wc *workloadsv1.WorkloadClass) (metav1.Condition, error) {
	guardrails := &workloadsv1.WorkloadClassGuardrailList{}
	if err := r.List(ctx, guardrails); err != nil {
		return metav1.Condition{}, err
	}

	if len(guardrails.Items) == 0 {
		return metav1.Condition{
			Type:               workloadsv1.ConditionTypeValidated,
			Status:             metav1.ConditionTrue,
			Reason:             workloadsv1.ReasonNoGuardrails,
			Message:            "No Guardrails found to validate against",
			ObservedGeneration: wc.Generation,
			LastTransitionTime: metav1.Now(),
		}, nil
	}

	// Determine effective constraints (pick the most restrictive)
	allowedDisruptionDays, maxAllowedWindows, maxNonDisruptionDurationDays := guardrailDisruptionConstraints(guardrails.Items)

	var violations []string
	for _, dw := range wc.Spec.DisruptionPolicy.AllowedDisruptionWindows {
		if !allowedDisruptionDaysValid(dw.DaysOfWeek, allowedDisruptionDays) {
			violations = append(violations, fmt.Sprintf("disruption window %s contains day(s) of week that are not allowed by guardrail. Found DaysOfWeek: %v, guardrail AllowedDisruptionDays: %v", dw.Name, dw.DaysOfWeek, allowedDisruptionDays))
		}
		if !utils.TimeZoneValid(dw.TimeZone) {
			violations = append(violations, fmt.Sprintf("disruption window %s has invalid time zone %s", dw.Name, dw.TimeZone))
		}
	}

	if len(wc.Spec.DisruptionPolicy.AllowedDisruptionWindows) > int(*maxAllowedWindows) {
		violations = append(violations, fmt.Sprintf("number of windows %v exceeds guardrail limit %d", wc.Spec.DisruptionPolicy.AllowedDisruptionWindows, int(*maxAllowedWindows)))
	}

	if maxNonDisruptionDurationDays != nil && wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays > *maxNonDisruptionDurationDays {
		violations = append(violations, fmt.Sprintf("maxNonDisruptionDurationDays %d exceeds guardrail limit %d", wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays, *maxNonDisruptionDurationDays))
	}

	violations = append(violations, validateCapacityStrategy(ctx, wc, guardrails)...)

	return condition(wc.Generation, violations), nil
}

// validateCapacityStrategy validates WorkloadClass.Spec.CapacityStrategy against the aggregated
// capacity constraints across all WorkloadClassGuardrails.
//
// It aggregates the most restrictive constraints (EnforcementMode, MinSpotRatio, and whether
// fallback to OnDemand is allowed) and checks:
//   - If CapacityStrategy is omitted (nil), validation fails when EnforcementMode is Required.
//   - When EnforcementMode is Required, CapacityStrategy.Type must be Spot and SpotRatio cannot be 0%.
//   - When EnforcementMode is Forbidden, CapacityStrategy.Type cannot be Spot.
//   - SpotRatio must be greater than or equal to the guardrail MinSpotRatio.
//   - FallbackAction cannot be FallbackToOnDemand when guardrails disallow fallback to OnDemand.
func validateCapacityStrategy(ctx context.Context, wc *workloadsv1.WorkloadClass, guardrails *workloadsv1.WorkloadClassGuardrailList) []string {
	if guardrails == nil || len(guardrails.Items) == 0 {
		return nil
	}

	var violations []string
	enforcementMode, minSpotRatioPct, allowFallback, err := guardrailCapacityConstraints(ctx, guardrails.Items)
	if err != nil {
		violations = append(violations, fmt.Sprintf("failed to parse guardrail parameters: %v", err))
	}

	if wc.Spec.CapacityStrategy == nil {
		if enforcementMode == workloadsv1.RequiredEnforcementMode {
			violations = append(violations, "capacityStrategy is required when guardrail enforcementMode is Required")
		}
		return violations
	}

	cs := wc.Spec.CapacityStrategy

	csType := cs.Type
	if csType == "" {
		csType = workloadsv1.SpotPlacementTypeSpot
	}

	spotRatio := 100
	spotRatioValid := true
	if csType == workloadsv1.SpotPlacementTypeOnDemand {
		spotRatio = 0
	} else if cs.SpotRatio != "" {
		parsed, err := utils.ParsePercentage(cs.SpotRatio)
		if err != nil {
			violations = append(violations, fmt.Sprintf("invalid spotRatio %q: %v", cs.SpotRatio, err))
			spotRatioValid = false
		} else {
			spotRatio = parsed
		}
	}

	fallbackAction := cs.FallbackAction
	if fallbackAction == "" {
		fallbackAction = workloadsv1.FallbackActionFail
	}

	switch enforcementMode {
	case workloadsv1.RequiredEnforcementMode:
		if csType != workloadsv1.SpotPlacementTypeSpot {
			violations = append(violations, fmt.Sprintf("capacityStrategy type %s is not allowed when guardrail enforcementMode is Required", csType))
		} else if spotRatioValid && spotRatio == 0 {
			violations = append(violations, "spotRatio cannot be 0% when guardrail enforcementMode is Required")
		}
	case workloadsv1.ForbiddenEnforcementMode:
		if csType == workloadsv1.SpotPlacementTypeSpot {
			violations = append(violations, fmt.Sprintf("capacityStrategy type %s is not allowed when guardrail enforcementMode is Forbidden", csType))
		}
	}

	if csType == workloadsv1.SpotPlacementTypeSpot && spotRatioValid && minSpotRatioPct > 0 && spotRatio < minSpotRatioPct {
		violations = append(violations, fmt.Sprintf("spotRatio %d%% is less than guardrail minSpotRatio %d%%", spotRatio, minSpotRatioPct))
	}

	if !allowFallback && fallbackAction == workloadsv1.FallbackActionFallbackToOnDemand {
		violations = append(violations, "fallback action FallbackToOnDemand is not allowed when guardrail disallows fallback to OnDemand")
	}

	return violations
}

// guardrailCapacityConstraints returns the most restrictive capacity strategy constraints from all WorkloadClassGuardrails.
// Capacity strategy constraints are: EnforcementMode, MinSpotRatio (as an integer percentage), and AllowFallbackToOnDemand.
// If there are no guardrails, default least-restrictive constraints (Allowed, 0, true) are returned.
func guardrailCapacityConstraints(ctx context.Context, guardrails []workloadsv1.WorkloadClassGuardrail) (workloadsv1.EnforcementMode, int, bool, error) {
	log := logf.FromContext(ctx)

	enforcementMode := workloadsv1.AllowedEnforcementMode
	minSpotRatioPct := 0
	allowFallback := true

	if len(guardrails) == 0 {
		log.Info("WorkloadClassGuardrailList is empty, returning default CapacityStrategy constraints")
		return enforcementMode, minSpotRatioPct, allowFallback, nil
	}

	var errs []error
	for _, g := range guardrails {
		if g.Spec.PluginConstraints == nil {
			continue
		}
		if err := applyPlacementCapacityConstraints(ctx, g.Name, g.Spec.PluginConstraints.Placement, &enforcementMode, &minSpotRatioPct, &allowFallback); err != nil {
			errs = append(errs, err)
		}
	}

	return enforcementMode, minSpotRatioPct, allowFallback, errors.Join(errs...)
}

// applyPlacementCapacityConstraints inspects the placement plugin constraints of a single WorkloadClassGuardrail,
// decodes any gke-spot-placement parameters, and updates the effective capacity constraints in-place to reflect
// the most restrictive settings seen so far:
//   - enforcementMode precedence: Forbidden > Required > Allowed
//   - minSpotRatioPct: highest minimum spot percentage (0 if enforcementMode is Forbidden)
//   - allowFallback: set to false if AllowFallbackToOnDemand is false or MaxFallbackRatio is 0 / "0%"
func applyPlacementCapacityConstraints(ctx context.Context, guardrailName string, placement []workloadsv1.PluginConstraint, enforcementMode *workloadsv1.EnforcementMode, minSpotRatioPct *int, allowFallback *bool) error {
	log := logf.FromContext(ctx)

	var errs []error
	for _, c := range placement {
		if c.Name != workloadsv1.PluginNameGKESpotPlacement || c.Parameters == nil || len(c.Parameters.Raw) == 0 {
			continue
		}

		params, err := utils.ParseGKESpotGuardrailParameters(c.Parameters.Raw)
		if err != nil {
			log.Error(err, "Failed to decode GKESpotGuardrailParameters", "guardrail", guardrailName)
			errs = append(errs, fmt.Errorf("guardrail %s: %w", guardrailName, err))
			continue
		}

		switch params.EnforcementMode {
		case "", workloadsv1.AllowedEnforcementMode:
		case workloadsv1.ForbiddenEnforcementMode:
			*enforcementMode = workloadsv1.ForbiddenEnforcementMode
			*minSpotRatioPct = 0
		case workloadsv1.RequiredEnforcementMode:
			if *enforcementMode != workloadsv1.ForbiddenEnforcementMode {
				*enforcementMode = workloadsv1.RequiredEnforcementMode
			}
		default:
			// Fail closed rather than treating an unknown mode as Allowed.
			errs = append(errs, fmt.Errorf("guardrail %s has invalid enforcementMode %q: must be one of Allowed, Required, Forbidden", guardrailName, params.EnforcementMode))
		}

		if params.MinSpotRatio != nil {
			parsed, err := utils.ParsePercentage(*params.MinSpotRatio)
			if err != nil {
				log.Error(err, "Failed to parse GKESpotGuardrailParameters minSpotRatio", "guardrail", guardrailName)
				errs = append(errs, fmt.Errorf("guardrail %s has invalid minSpotRatio %q: %w", guardrailName, *params.MinSpotRatio, err))
				continue
			}
			if *enforcementMode != workloadsv1.ForbiddenEnforcementMode && parsed > *minSpotRatioPct {
				*minSpotRatioPct = parsed
			}
		}

		if params.Fallback != nil {
			if params.Fallback.AllowFallbackToOnDemand != nil && !*params.Fallback.AllowFallbackToOnDemand {
				*allowFallback = false
			}
			if params.Fallback.MaxFallbackRatio != nil {
				mfr := params.Fallback.MaxFallbackRatio
				isZero := (mfr.Type == intstr.Int && mfr.IntVal == 0) || (mfr.Type == intstr.String && (mfr.StrVal == "0%" || mfr.StrVal == "0"))
				if isZero {
					*allowFallback = false
				}
			}
		}
	}

	return errors.Join(errs...)
}

// validateSelectors validates the workloadclass' PodSelector against existing workloadclasses in the same namespace.
// If another workloadclass has the exact same PodSelector, an error is returned to be emitted as a warning.
// If two workloadclasses match a Pod with the same specificity, the oldest workloadclass takes precedence.
func (r *WorkloadClassReconciler) validateSelectors(ctx context.Context, wc *workloadsv1.WorkloadClass) ([]workloadsv1.WorkloadClass, error) {
	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(wc.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to fetch workloadclasses: %w", err)
	}

	if len(workloadClasses.Items) == 0 {
		return nil, nil
	}

	var matches []workloadsv1.WorkloadClass
	for _, ewc := range workloadClasses.Items {
		if ewc.Name == wc.Name {
			continue
		}
		if sameLabelSelectorSemantic(wc.Spec.PodSelector, ewc.Spec.PodSelector) {
			matches = append(matches, ewc)
		}
	}

	return matches, formatError(wc, matches)
}

func formatError(wc *workloadsv1.WorkloadClass, matches []workloadsv1.WorkloadClass) error {
	if len(matches) == 0 {
		return nil
	}

	oldest := wc
	var matchNames []string
	for _, m := range matches {
		matchNames = append(matchNames, m.Name)
		if m.CreationTimestamp.Before(&oldest.CreationTimestamp) {
			oldest = &m
		}
	}

	return fmt.Errorf("the following WorkloadClasses have the same PodSelector as %s: %s", wc.Name, strings.Join(matchNames, ", "))
}

// sameLabelSelectorSemantic returns true if the two label selectors select the
// same resources, regardless of the order of rules/expressions.
func sameLabelSelectorSemantic(a, b *metav1.LabelSelector) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	selA, errA := metav1.LabelSelectorAsSelector(a)
	selB, errB := metav1.LabelSelectorAsSelector(b)
	if errA != nil || errB != nil {
		return false
	}
	// .String() returns a sorted, deterministic representation of the selector rules.
	return selA.String() == selB.String()
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClass{}).
		Watches(
			&workloadsv1.WorkloadClassGuardrail{}, // Re-trigger validation if guardrails change
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesToReconcile),
		).
		// A WorkloadClass being created, deleted, or changing its spec can change the best-match WorkloadClass of
		// Pods in its namespace, so every WorkloadClass in the namespace re-evaluates which Pods it governs.
		Watches(
			&workloadsv1.WorkloadClass{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesInObjectNamespace),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		// Re-sync managed Per-Pod PDBs if they are modified or deleted externally.
		Owns(&policyv1.PodDisruptionBudget{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// User-created PDBs take precedence: yield a Pod's PDB when a user PDB covers it, and reclaim it when the
		// user PDB is removed.
		Watches(
			&policyv1.PodDisruptionBudget{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesInObjectNamespace),
			builder.WithPredicates(userPDBPredicate()),
		).
		// Changing the namespace default WorkloadClass changes which WorkloadClass governs every Pod in it.
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesForNamespace),
			builder.WithPredicates(namespaceDefaultClassPredicate()),
		).
		// Create, update, or delete a Pod's PDB as the Pod is created, relabeled, or deleted.
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesForPod),
			builder.WithPredicates(podPredicate()),
		)

	if r.LeaseTracker != nil {
		// Apply lease requests from the disruption webhook.
		b = b.WatchesRawSource(source.Channel(r.LeaseTracker.Events(), &handler.EnqueueRequestForObject{}))
	}

	return b.Named("workloadclass").Complete(r)
}

// findWorkloadClassesInObjectNamespace enqueues every WorkloadClass in the object's namespace.
func (r *WorkloadClassReconciler) findWorkloadClassesInObjectNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.workloadClassRequestsInNamespace(ctx, obj.GetNamespace())
}

// findWorkloadClassesForNamespace enqueues every WorkloadClass in the Namespace.
func (r *WorkloadClassReconciler) findWorkloadClassesForNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.workloadClassRequestsInNamespace(ctx, obj.GetName())
}

func (r *WorkloadClassReconciler) workloadClassRequestsInNamespace(ctx context.Context, namespace string) []reconcile.Request {
	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list WorkloadClasses", "namespace", namespace)
		return nil
	}

	requests := make([]reconcile.Request, 0, len(workloadClasses.Items))
	for _, wc := range workloadClasses.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&wc)})
	}
	return requests
}

// findWorkloadClassesForPod enqueues the Pod's best-match WorkloadClass, and the WorkloadClass that currently owns
// the Pod's PDB if it differs (e.g., the Pod was deleted or no longer matches it).
func (r *WorkloadClassReconciler) findWorkloadClassesForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	names := map[string]bool{}
	if bestMatch, err := utils.FindBestMatchWorkloadClass(ctx, r.Client, nil, pod); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to find WorkloadClass for Pod", "pod", pod.Name, "namespace", pod.Namespace)
	} else if bestMatch != nil {
		names[bestMatch.Name] = true
	}

	pdb := &policyv1.PodDisruptionBudget{}
	if err := r.Get(ctx, client.ObjectKey{Name: utils.PDBName(pod.Name), Namespace: pod.Namespace}, pdb); err == nil {
		if owner := metav1.GetControllerOf(pdb); owner != nil && owner.Kind == "WorkloadClass" {
			names[owner.Name] = true
		}
	}

	requests := make([]reconcile.Request, 0, len(names))
	for name := range names {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKey{Name: name, Namespace: pod.Namespace}})
	}
	return requests
}

// podPredicate passes Pod creation and deletion, Pods starting to terminate or reaching a terminal phase,
// and Pod label changes. It ignores the controller's own update that sets the workloads.gke.io/pod-uid label.
func podPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, okOld := e.ObjectOld.(*corev1.Pod)
			newPod, okNew := e.ObjectNew.(*corev1.Pod)
			if !okOld || !okNew {
				return false
			}
			if utils.IsPodActive(oldPod) != utils.IsPodActive(newPod) {
				return true
			}
			if newPod.Labels[utils.PodUIDLabel] != string(newPod.UID) && oldPod.Labels[utils.PodUIDLabel] == string(oldPod.UID) {
				// The workloads.gke.io/pod-uid label was removed or changed; restore it.
				return true
			}
			return !maps.Equal(labelsWithoutPodUID(oldPod.Labels), labelsWithoutPodUID(newPod.Labels))
		},
	}
}

func labelsWithoutPodUID(podLabels map[string]string) map[string]string {
	if _, ok := podLabels[utils.PodUIDLabel]; !ok {
		return podLabels
	}
	filtered := maps.Clone(podLabels)
	delete(filtered, utils.PodUIDLabel)
	return filtered
}

// userPDBPredicate passes events for PDBs that are not managed by a WorkloadClass.
func userPDBPredicate() predicate.Funcs {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		pdb, ok := obj.(*policyv1.PodDisruptionBudget)
		return ok && !utils.IsWorkloadClassManagedPDB(pdb)
	})
}

// namespaceDefaultClassPredicate passes Namespace events that set, change, or remove the
// workloads.gke.io/default-class label.
func namespaceDefaultClassPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			_, ok := e.Object.GetLabels()[workloadsv1.DefaultClassLabel]
			return ok
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetLabels()[workloadsv1.DefaultClassLabel] != e.ObjectNew.GetLabels()[workloadsv1.DefaultClassLabel]
		},
		// Deleting a Namespace deletes its WorkloadClasses and PDBs.
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

func (r *WorkloadClassReconciler) findWorkloadClassesToReconcile(ctx context.Context, guardrail client.Object) []reconcile.Request {
	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, len(workloadClasses.Items))
	for i, item := range workloadClasses.Items {
		requests[i] = reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      item.GetName(),
				Namespace: item.GetNamespace(),
			},
		}
	}
	return requests
}

func condition(generation int64, violations []string) metav1.Condition {
	condition := metav1.Condition{
		Type:               workloadsv1.ConditionTypeValidated,
		Status:             metav1.ConditionTrue,
		Reason:             workloadsv1.ReasonValidationPassed,
		Message:            validationPassedMsg,
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: generation,
	}

	if len(violations) > 0 {
		condition.Status = metav1.ConditionFalse
		condition.Reason = workloadsv1.ReasonValidationFailed
		condition.Message = strings.Join(violations, "; ")
	}

	return condition
}

func guardrailDisruptionConstraints(guardrails []workloadsv1.WorkloadClassGuardrail) ([][]string, *int32, *int32) {
	if guardrails == nil {
		return nil, nil, nil
	}

	// Determine effective constraints (pick the most restrictive)
	var allowedDisruptionDays [][]string
	var maxAllowedWindows *int32
	var maxNonDisruptionDurationDays *int32

	for _, g := range guardrails {
		if maxNonDisruptionDurationDays == nil || g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays < *maxNonDisruptionDurationDays {
			maxNonDisruptionDurationDays = &g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays
		}

		allowedDisruptionDays = append(allowedDisruptionDays, g.Spec.Constraints.Disruption.AllowedDisruptionDays)

		if maxAllowedWindows == nil || g.Spec.Constraints.Disruption.MaxAllowedWindows < *maxAllowedWindows {
			maxAllowedWindows = &g.Spec.Constraints.Disruption.MaxAllowedWindows
		}
	}

	return allowedDisruptionDays, maxAllowedWindows, maxNonDisruptionDurationDays
}

func allowedDisruptionDaysValid(wcAllowedDisruptionDays []string, guardrail [][]string) bool {
	if len(guardrail) == 0 || len(wcAllowedDisruptionDays) == 0 {
		return true
	}

	valid := true
	for _, days := range guardrail {
		valid = valid && utils.IsSubset(wcAllowedDisruptionDays, days)
	}

	return valid
}
