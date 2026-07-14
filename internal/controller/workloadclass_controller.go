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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// WorkloadClassReconciler reconciles a WorkloadClass object
type WorkloadClassReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses/finalizers,verbs=update
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop.
func (r *WorkloadClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	wc := &workloadsv1.WorkloadClass{}
	if err := r.Get(ctx, req.NamespacedName, wc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1. Fetch Guardrails and validate
	validationCond, err := r.validateAgainstGuardrails(ctx, wc)
	if err != nil {
		log.Error(err, "Failed to validate against guardrails")
		return ctrl.Result{}, err
	}
	meta.SetStatusCondition(&wc.Status.Conditions, validationCond)

	// 1.1 Persist the status change
	err = r.Status().Update(ctx, wc)
	if err != nil {
		log.Error(err, "Failed to update Status conditions")
		return ctrl.Result{}, err
	}

	// 2. Check if other existing WorkloadClasses have the same PodSelector
	overlappingClasses, err := r.validateSelectors(ctx, wc)
	if err != nil {
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

	// 3. Update Status if changed
	if wc.Status.MaintenanceReadiness != readiness {
		wc.Status.MaintenanceReadiness = readiness
		log.Info(fmt.Sprintf("Workload is now %s for maintenance", readiness))
		if err := r.Status().Update(ctx, wc); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 4. Reconcile the PDB
	err = r.reconcilePDB(ctx, wc, validationCond, overlappingClasses)
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
	}

	return ctrl.Result{RequeueAfter: nextReconcile}, nil
}

func (r *WorkloadClassReconciler) reconcilePDB(ctx context.Context, wc *workloadsv1.WorkloadClass, condition metav1.Condition, overlappingClasses []workloadsv1.WorkloadClass) error {
	log := logf.FromContext(ctx)

	if !wc.DeletionTimestamp.IsZero() {
		// Do not reconcile the PDB if the WorkloadClass is being deleted.
		// The PDB will be deleted after the WorkloadClass because of the owner reference
		log.Info(fmt.Sprintf("WorkloadClass %s is being deleted, skipping PDB reconciliation", wc.Name))
		return nil
	}

	// If the WorkloadClass is invalid, delete the associated PDB
	if condition.Reason != workloadsv1.ReasonValidationPassed {
		log.Info(fmt.Sprintf("WorkloadClass %s is invalid, deleting associated PDB", wc.Name))
		return r.deletePDB(ctx, wc)
	}

	// Check if the current WorkloadClass is the namespace default
	namespaceDefaultWC, err := r.namespaceDefault(ctx, wc)
	if err != nil {
		return err
	}

	if namespaceDefaultWC != nil && namespaceDefaultWC.Name == wc.Name {
		return r.createOrUpdatePDB(ctx, wc, true)
	} else if namespaceDefaultWC != nil && namespaceDefaultWC.DeletionTimestamp.IsZero() {
		// There exists a namespace default WorkloadClass, but it is not this WorkloadClass
		return r.deletePDB(ctx, wc)
	}

	// There is either no namespace default, or the existing namespace default is being deleted. Reconcile the PDB as normal.

	// Check if other WC's have the same selector
	if len(overlappingClasses) == 0 {
		return r.createOrUpdatePDB(ctx, wc, false)
	}

	// Check if the current WorkloadClass is the oldest one with these selectors
	if oldestWorkloadClass(wc, overlappingClasses).Name == wc.Name {
		return r.createOrUpdatePDB(ctx, wc, false)
	}

	return r.deletePDB(ctx, wc)
}

func (r *WorkloadClassReconciler) namespaceDefault(ctx context.Context, wc *workloadsv1.WorkloadClass) (*workloadsv1.WorkloadClass, error) {
	ns := &corev1.Namespace{}
	reqKey := types.NamespacedName{Name: wc.Namespace}
	if err := r.Get(ctx, reqKey, ns); err != nil {
		return nil, fmt.Errorf("error getting Namespace %s: %w", wc.Namespace, err)
	}

	if labelValue, ok := ns.Labels[workloadsv1.DefaultClassLabel]; ok {
		return r.getNamespaceDefault(ctx, labelValue, ns.Name)
	}

	return nil, nil
}

func (r *WorkloadClassReconciler) getNamespaceDefault(ctx context.Context, wcName, nsName string) (*workloadsv1.WorkloadClass, error) {
	defaultWC := &workloadsv1.WorkloadClass{}
	key := types.NamespacedName{Name: wcName, Namespace: nsName}
	if err := r.Get(ctx, key, defaultWC); err != nil {
		if errors.IsNotFound(err) {
			logf.FromContext(ctx).Info("Referenced default WorkloadClass not found", "workloadClass", wcName, "namespace", nsName)
			return nil, nil
		}
		return nil, err
	}

	return defaultWC, nil
}

func oldestWorkloadClass(wc *workloadsv1.WorkloadClass, overlappingClasses []workloadsv1.WorkloadClass) *workloadsv1.WorkloadClass {
	oldest := wc
	for _, c := range overlappingClasses {
		if c.CreationTimestamp.Before(&oldest.CreationTimestamp) {
			oldest = &c
		}
	}
	return oldest
}

func (r *WorkloadClassReconciler) deletePDB(ctx context.Context, wc *workloadsv1.WorkloadClass) error {
	pdb := utils.PDBBase(wc)
	if err := r.Delete(ctx, pdb); err != nil {
		// If it's already gone, that's a success for a delete operation
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete PDB: %w", err)
	}

	return nil
}

func (r *WorkloadClassReconciler) createOrUpdatePDB(ctx context.Context, wc *workloadsv1.WorkloadClass, namespaceDefault bool) error {
	pdb := utils.PDBBase(wc)
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		// Set owner reference so the PDB gets automatically deleted if the WorkloadClass is deleted
		if err := controllerutil.SetControllerReference(wc, pdb, r.Scheme); err != nil {
			return err
		}
		return utils.SyncPDBWithWorkloadClass(wc, pdb, namespaceDefault)
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile PDB: %w", err)
	}

	logf.FromContext(ctx).Info("Reconciled PDB", "operation", op)
	return nil
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
	selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
	if err != nil {
		return workloadsv1.ReadinessNotReady, 0, err
	}
	if err := r.List(ctx, pods, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return workloadsv1.ReadinessNotReady, 0, err
	}

	// 4. Check Pod Ages (Min Initial Run) and if grace periods have passed (GraceTerminationDuration)
	if wc.Spec.DisruptionPolicy.MinInitialRunDurationDays > 0 {
		minRunDuration := time.Duration(wc.Spec.DisruptionPolicy.MinInitialRunDurationDays) * 24 * time.Hour
		for _, pod := range pods.Items {
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
		for _, pod := range pods.Items {
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

	return condition(violations), nil
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClass{}).
		Watches(&policyv1.PodDisruptionBudget{},
			handler.EnqueueRequestsFromMapFunc(r.findNonDefaultWorkloadClasses),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(e event.CreateEvent) bool { return true },
				UpdateFunc:  func(e event.UpdateEvent) bool { return false },
				GenericFunc: func(e event.GenericEvent) bool { return false },
				DeleteFunc:  func(e event.DeleteEvent) bool { return true },
			}),
		).
		Watches(&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesByNamespace),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc: func(e event.CreateEvent) bool {
					// Trigger if namespace is created with the label
					_, hasLabel := e.Object.GetLabels()[workloadsv1.DefaultClassLabel]
					return hasLabel
				},
				UpdateFunc: func(e event.UpdateEvent) bool {
					// Trigger ONLY if the DefaultClassLabel was added, removed, or modified
					oldValue, oldHas := e.ObjectOld.GetLabels()[workloadsv1.DefaultClassLabel]
					newValue, newHas := e.ObjectNew.GetLabels()[workloadsv1.DefaultClassLabel]
					return oldHas != newHas || oldValue != newValue
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					// Trigger if namespace is deleted and it had the label
					_, hasLabel := e.Object.GetLabels()[workloadsv1.DefaultClassLabel]
					return hasLabel
				},
				GenericFunc: func(e event.GenericEvent) bool { return false },
			}),
		).
		Watches(
			&workloadsv1.WorkloadClassGuardrail{}, // Re-trigger validation if guardrails change
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesToReconcile),
		).
		Named("workloadclass").
		Complete(r)
}

func (r *WorkloadClassReconciler) findWorkloadClassesByNamespace(ctx context.Context, ns client.Object) []reconcile.Request {
	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(ns.GetName())); err != nil {
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

// findNonDefaultWorkloadClasses triggers reconciliation for all non-default WorkloadClasses
// when the namespace default PDB is created or deleted.
//
// PDB Management Rules:
//   - With a Namespace Default: Its PDB uses an empty selector to target all pods. PDBs
//     for all other WorkloadClasses in the namespace must be deleted.
//   - Without a Namespace Default: Every WorkloadClass must generate its own PDB based on
//     its specific pod selector.
//
// When the default PDB is created or deleted, this handler enqueues all other WorkloadClasses
// in the namespace so they can correctly regenerate or delete their own PDBs to match
// the current namespace default state.
//
// Algorithm for fetching WorkloadClasses to reconcile:
//  1. Determine the WorkloadClass associated with the triggering PDB.
//  2. Check if this WorkloadClass is currently set as the default for the namespace.
//  3. If it is the default, list all WorkloadClasses in the namespace and enqueue
//     requests for all of them except the default one itself.
func (r *WorkloadClassReconciler) findNonDefaultWorkloadClasses(ctx context.Context, obj client.Object) []reconcile.Request {
	var workloadClassName string
	pdb := obj.(*policyv1.PodDisruptionBudget)
	if owner := metav1.GetControllerOf(pdb); owner != nil && owner.Kind == "WorkloadClass" {
		workloadClassName = owner.Name
	} else {
		workloadClassName = utils.WorkloadClassNameFromPDBName(pdb.Name)
	}

	// Get the Namespace to determine the default WorkloadClass
	ns := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: pdb.Namespace}, ns); err != nil {
		return nil
	}

	// If this PDB is not from the namespace default WorkloadClass, there is no need to reconcile all other WorkloadClasses
	if ns.Labels[workloadsv1.DefaultClassLabel] != workloadClassName {
		return nil
	}

	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(pdb.Namespace)); err != nil {
		return nil
	}

	requests := []reconcile.Request{}
	for _, wc := range workloadClasses.Items {
		if wc.Name == workloadClassName {
			continue // Skip the WorkloadClass associated with this PDB
		}

		namespacedName := client.ObjectKey{Name: wc.GetName(), Namespace: wc.GetNamespace()}
		requests = append(requests, reconcile.Request{NamespacedName: namespacedName})
	}

	return requests
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

func condition(violations []string) metav1.Condition {
	if len(violations) > 0 {
		return metav1.Condition{
			Type:               workloadsv1.ConditionTypeValidated,
			Status:             metav1.ConditionFalse,
			Reason:             workloadsv1.ReasonValidationFailed,
			Message:            strings.Join(violations, "; "),
			LastTransitionTime: metav1.Now(),
		}
	}

	return metav1.Condition{
		Type:               workloadsv1.ConditionTypeValidated,
		Status:             metav1.ConditionTrue,
		Reason:             workloadsv1.ReasonValidationPassed,
		Message:            "WorkloadClass adheres to all Guardrail constraints",
		LastTransitionTime: metav1.Now(),
	}
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
