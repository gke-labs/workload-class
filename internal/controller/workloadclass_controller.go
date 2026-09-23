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
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	clusterAutoscalerStatusNamespace = "kube-system"
	clusterAutoscalerStatusName      = "cluster-autoscaler-status"
	spotNodeLabelKey                 = "cloud.google.com/gke-spot"
	spotNodeLabelValue               = "true"
	pdbRetryRequeueDelay             = 10 * time.Second
)

type clusterAutoscalerStatus struct {
	NodeGroups []clusterAutoscalerNodeGroup `json:"nodeGroups"`
}

type clusterAutoscalerNodeGroup struct {
	Name    string                            `json:"name"`
	Health  clusterAutoscalerNodeGroupHealth  `json:"health"`
	ScaleUp clusterAutoscalerNodeGroupScaleUp `json:"scaleUp"`
}

type clusterAutoscalerNodeGroupHealth struct {
	Status string `json:"status"`
}

type clusterAutoscalerNodeGroupScaleUp struct {
	Status      string                        `json:"status"`
	BackoffInfo *clusterAutoscalerBackoffInfo `json:"backoffInfo,omitempty"`
}

type clusterAutoscalerBackoffInfo struct {
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

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
// +kubebuilder:rbac:groups="",resources=pods;namespaces;configmaps;nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
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

	// 3. Calculate Readiness
	readiness, nextReconcile, err := r.calculateReadiness(ctx, wc)
	if err != nil {
		log.Error(err, "Failed to calculate readiness")
		return ctrl.Result{}, err
	}

	// 4. Update Status if changed
	if wc.Status.MaintenanceReadiness != readiness {
		wc.Status.MaintenanceReadiness = readiness
		log.Info(fmt.Sprintf("Workload is now %s for maintenance", readiness))
		if err := r.Status().Update(ctx, wc); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 5. Check Spot capacity availability and perform Active reversion if applicable
	spotCapacityAvailable, err := r.checkSpotCapacity(ctx)
	if err != nil {
		log.Error(err, "Failed to check Spot capacity")
		return ctrl.Result{}, err
	}
	log.Info("Checked Spot capacity", "spotCapacityAvailable", spotCapacityAvailable)

	if validationCond.Status == metav1.ConditionTrue {
		if err := r.reconcileFallbackState(ctx, wc); err != nil {
			log.Error(err, "Failed to reconcile Spot fallback state")
			return ctrl.Result{}, err
		}
		if spotCapacityAvailable {
			reversionRequeue, err := r.reconcileSpotReversion(ctx, wc, time.Now().UTC())
			if err != nil {
				log.Error(err, "Failed to reconcile Spot reversion")
				return ctrl.Result{}, err
			}
			if reversionRequeue > 0 && (nextReconcile == 0 || reversionRequeue < nextReconcile) {
				nextReconcile = reversionRequeue
			}
		}
	}

	// 6. Reconcile the PDB
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

	if namespaceDefaultWC == wc.Name {
		return r.createOrUpdatePDB(ctx, wc)
	} else if namespaceDefaultWC != "" {
		// There exists a namespace default WorkloadClass, but it is not this WorkloadClass
		return r.deletePDB(ctx, wc)
	}

	// Check if other WC's have the same selector
	if len(overlappingClasses) == 0 {
		return r.createOrUpdatePDB(ctx, wc)
	}

	// Check if the current WorkloadClass is the oldest one with these selectors
	if oldestWorkloadClass(wc, overlappingClasses).Name == wc.Name {
		return r.createOrUpdatePDB(ctx, wc)
	}

	return r.deletePDB(ctx, wc)
}

func (r *WorkloadClassReconciler) namespaceDefault(ctx context.Context, wc *workloadsv1.WorkloadClass) (string, error) {
	const defaultClassLabel = "workloads.gke.io/default-class"
	ns := &corev1.Namespace{}

	// We use types.NamespacedName. Since Namespaces are cluster-scoped,
	// the Namespace field inside NamespacedName is left empty, and we just provide the Name.
	reqKey := types.NamespacedName{Name: wc.Namespace}
	if err := r.Get(ctx, reqKey, ns); err != nil {
		return "", fmt.Errorf("error getting Namespace %s: %w", wc.Namespace, err)
	}

	if value, ok := ns.Labels[defaultClassLabel]; ok {
		return value, nil
	}

	return "", nil
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

func (r *WorkloadClassReconciler) createOrUpdatePDB(ctx context.Context, wc *workloadsv1.WorkloadClass) error {
	pdb := utils.PDBBase(wc)
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		// Set owner reference so the PDB gets automatically deleted if the WorkloadClass is deleted
		if err := controllerutil.SetControllerReference(wc, pdb, r.Scheme); err != nil {
			return err
		}
		return utils.SyncPDBWithWorkloadClass(wc, pdb)
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

	if maxAllowedWindows != nil && len(wc.Spec.DisruptionPolicy.AllowedDisruptionWindows) > int(*maxAllowedWindows) {
		violations = append(violations, fmt.Sprintf("number of windows %v exceeds guardrail limit %d", wc.Spec.DisruptionPolicy.AllowedDisruptionWindows, int(*maxAllowedWindows)))
	}

	if maxNonDisruptionDurationDays != nil && wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays > *maxNonDisruptionDurationDays {
		violations = append(violations, fmt.Sprintf("maxNonDisruptionDurationDays %d exceeds guardrail limit %d", wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays, *maxNonDisruptionDurationDays))
	}

	violations = append(violations, utils.ValidatePlacementAgainstGuardrails(wc, guardrails.Items)...)

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

// checkSpotCapacity checks whether Spot capacity can be provisioned by inspecting
// the kube-system/cluster-autoscaler-status ConfigMap for Spot node groups that are
// Healthy and not in stockout Backoff, falling back to checking for Ready Spot nodes.
func (r *WorkloadClassReconciler) checkSpotCapacity(ctx context.Context) (bool, error) {
	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: clusterAutoscalerStatusNamespace,
		Name:      clusterAutoscalerStatusName,
	}, cm)
	if err != nil && !errors.IsNotFound(err) {
		return false, fmt.Errorf("failed to get %s/%s ConfigMap: %w", clusterAutoscalerStatusNamespace, clusterAutoscalerStatusName, err)
	}

	if err == nil && cm.Data != nil {
		if rawStatus, ok := cm.Data["status"]; ok && strings.TrimSpace(rawStatus) != "" {
			var caStatus clusterAutoscalerStatus
			if unmarshalErr := yaml.Unmarshal([]byte(rawStatus), &caStatus); unmarshalErr == nil {
				spotNodeGroupFound := false
				for _, ng := range caStatus.NodeGroups {
					if !strings.Contains(strings.ToLower(ng.Name), "spot") {
						continue
					}
					spotNodeGroupFound = true
					if isSpotNodeGroupAvailable(ng) {
						return true, nil
					}
				}
				if spotNodeGroupFound {
					return false, nil
				}
			}
		}
	}

	return r.hasReadySpotNodes(ctx)
}

func isSpotNodeGroupAvailable(ng clusterAutoscalerNodeGroup) bool {
	if !strings.EqualFold(ng.Health.Status, "Healthy") {
		return false
	}
	if strings.EqualFold(ng.ScaleUp.Status, "Backoff") {
		return false
	}
	if ng.ScaleUp.BackoffInfo != nil && ng.ScaleUp.BackoffInfo.ErrorCode != "" {
		return false
	}
	return true
}

func (r *WorkloadClassReconciler) hasReadySpotNodes(ctx context.Context) (bool, error) {
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, client.MatchingLabels{spotNodeLabelKey: spotNodeLabelValue}); err != nil {
		return false, fmt.Errorf("failed to list Spot nodes: %w", err)
	}

	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Spec.Unschedulable {
			continue
		}
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
	}
	return false, nil
}

// reconcileFallbackState updates ConditionTypeInFallback on the WorkloadClass status
// when Spot-targeted Pods fall back to On-Demand nodes. For ReversionActionNone, once
// ConditionTypeInFallback becomes True it stays True permanently so future Pods stay on On-Demand.
func (r *WorkloadClassReconciler) reconcileFallbackState(ctx context.Context, wc *workloadsv1.WorkloadClass) error {
	sp := wc.Spec.PlacementPolicy.SpotPlacement
	if sp.Type == workloadsv1.SpotPlacementTypeOnDemand || sp.Fallback.Action != workloadsv1.FallbackActionFallbackToOnDemand {
		return nil
	}

	spotRatio := parseSpotRatio(sp.SpotRatio)
	if spotRatio <= 0 {
		return nil
	}

	spotNodes, err := r.listSpotNodeNames(ctx)
	if err != nil {
		return err
	}

	spotPods, onDemandPods, totalActivePods, err := r.classifyWorkloadClassPodsForReversion(ctx, wc, spotNodes)
	if err != nil {
		return err
	}

	currentlyInFallback := len(onDemandPods) > 0 && spotPods*100 < totalActivePods*spotRatio
	reversionAction := sp.Reversion.Action
	if reversionAction == "" {
		reversionAction = workloadsv1.ReversionActionNone
	}

	if currentlyInFallback {
		changed := meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type:               workloadsv1.ConditionTypeInFallback,
			Status:             metav1.ConditionTrue,
			Reason:             workloadsv1.ReasonFallbackActive,
			Message:            "One or more Pods fell back to On-Demand nodes",
			LastTransitionTime: metav1.Now(),
		})
		if changed {
			return r.Status().Update(ctx, wc)
		}
		return nil
	}

	// For None reversion, once in fallback, stay in fallback permanently.
	if reversionAction == workloadsv1.ReversionActionNone &&
		meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeInFallback) {
		return nil
	}

	if totalActivePods > 0 && meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeInFallback) {
		changed := meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type:               workloadsv1.ConditionTypeInFallback,
			Status:             metav1.ConditionFalse,
			Reason:             workloadsv1.ReasonNoFallback,
			Message:            "All Spot-targeted Pods are running on Spot nodes",
			LastTransitionTime: metav1.Now(),
		})
		if changed {
			return r.Status().Update(ctx, wc)
		}
	}

	return nil
}

func parseSpotRatio(ratioStr string) int {
	spotRatio := 100
	if ratioStr != "" {
		if parsed, err := strconv.Atoi(strings.TrimSuffix(ratioStr, "%")); err == nil {
			spotRatio = parsed
		}
	}
	return spotRatio
}

// reconcileSpotReversion handles reversion when Spot capacity is available:
// - Active: proactively evicts Pods on On-Demand fallback nodes (respecting PDBs, disruption windows, and MinDurationOnFallback).
// - Lazy: does not evict running On-Demand Pods; Pods migrate back to Spot only when naturally recreated.
// - None: keeps Pods on On-Demand permanently after fallback.
func (r *WorkloadClassReconciler) reconcileSpotReversion(ctx context.Context, wc *workloadsv1.WorkloadClass, now time.Time) (time.Duration, error) {
	if err := r.reconcileFallbackState(ctx, wc); err != nil {
		return 0, err
	}

	sp := wc.Spec.PlacementPolicy.SpotPlacement
	if sp.Type == workloadsv1.SpotPlacementTypeOnDemand {
		return 0, nil
	}

	reversionAction := sp.Reversion.Action
	if reversionAction == "" {
		reversionAction = workloadsv1.ReversionActionNone
	}

	switch reversionAction {
	case workloadsv1.ReversionActionNone, workloadsv1.ReversionActionLazy:
		return 0, nil
	case workloadsv1.ReversionActionActive:
	default:
		return 0, nil
	}

	spotRatio := parseSpotRatio(sp.SpotRatio)
	if spotRatio <= 0 {
		return 0, nil
	}

	spotNodes, err := r.listSpotNodeNames(ctx)
	if err != nil {
		return 0, err
	}

	spotPods, onDemandPods, totalActivePods, err := r.classifyWorkloadClassPodsForReversion(ctx, wc, spotNodes)
	if err != nil {
		return 0, err
	}

	var minDuration time.Duration
	if sp.Reversion.MinDurationOnFallback != nil {
		minDuration = sp.Reversion.MinDurationOnFallback.Duration
	}

	var minRequeue time.Duration
	for i := range onDemandPods {
		if spotPods*100 >= totalActivePods*spotRatio {
			break
		}

		pod := &onDemandPods[i]
		if minDuration > 0 {
			elapsed := now.Sub(podFallbackStartTime(pod))
			if elapsed < minDuration {
				remaining := minDuration - elapsed
				if minRequeue == 0 || remaining < minRequeue {
					minRequeue = remaining
				}
				continue
			}
		}

		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
		}
		if err := r.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
			if errors.IsTooManyRequests(err) || errors.IsForbidden(err) {
				inWindow, nextWindow := utils.IsTimeInWindows(ctx, now, wc.Spec.DisruptionPolicy.AllowedDisruptionWindows)
				if !inWindow {
					if nextWindow > 0 && (minRequeue == 0 || nextWindow < minRequeue) {
						minRequeue = nextWindow
					}
					break
				}
				if minRequeue == 0 || pdbRetryRequeueDelay < minRequeue {
					minRequeue = pdbRetryRequeueDelay
				}
				continue
			}
			if errors.IsNotFound(err) {
				continue
			}
			return 0, fmt.Errorf("failed to evict pod %s/%s for Spot reversion: %w", pod.Namespace, pod.Name, err)
		}

		spotPods++
	}

	return minRequeue, nil
}

func (r *WorkloadClassReconciler) listSpotNodeNames(ctx context.Context) (map[string]bool, error) {
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, client.MatchingLabels{spotNodeLabelKey: spotNodeLabelValue}); err != nil {
		return nil, fmt.Errorf("failed to list Spot nodes: %w", err)
	}

	spotNodes := make(map[string]bool, len(nodes.Items))
	for i := range nodes.Items {
		spotNodes[nodes.Items[i].Name] = true
	}
	return spotNodes, nil
}

func (r *WorkloadClassReconciler) classifyWorkloadClassPodsForReversion(
	ctx context.Context,
	wc *workloadsv1.WorkloadClass,
	spotNodes map[string]bool,
) (spotPods int, onDemandPods []corev1.Pod, totalActivePods int, err error) {
	selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
	if err != nil {
		return 0, nil, 0, err
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return 0, nil, 0, err
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil ||
			pod.Status.Phase == corev1.PodSucceeded ||
			pod.Status.Phase == corev1.PodFailed {
			continue
		}

		totalActivePods++
		if pod.Spec.NodeName == "" {
			continue
		}
		if spotNodes[pod.Spec.NodeName] {
			spotPods++
		} else {
			onDemandPods = append(onDemandPods, *pod)
		}
	}

	return spotPods, onDemandPods, totalActivePods, nil
}

func podFallbackStartTime(pod *corev1.Pod) time.Time {
	if pod.Status.StartTime != nil && !pod.Status.StartTime.IsZero() {
		return pod.Status.StartTime.Time
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionTrue && !cond.LastTransitionTime.IsZero() {
			return cond.LastTransitionTime.Time
		}
	}
	return pod.CreationTimestamp.Time
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClass{}).
		Watches(
			&workloadsv1.WorkloadClassGuardrail{}, // Re-trigger validation if guardrails change
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesToReconcile),
		).
		Watches(
			&corev1.ConfigMap{}, // Re-trigger when kube-system/cluster-autoscaler-status updates
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesForClusterAutoscalerStatus),
		).
		Watches(
			&corev1.Pod{}, // Re-trigger when Pods in the namespace are created or scheduled
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesInNamespace),
		).
		Named("workloadclass").
		Complete(r)
}

func (r *WorkloadClassReconciler) findWorkloadClassesInNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(obj.GetNamespace())); err != nil {
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

func (r *WorkloadClassReconciler) findWorkloadClassesForClusterAutoscalerStatus(ctx context.Context, obj client.Object) []reconcile.Request {
	if obj.GetNamespace() != clusterAutoscalerStatusNamespace || obj.GetName() != clusterAutoscalerStatusName {
		return nil
	}
	return r.findWorkloadClassesToReconcile(ctx, obj)
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
		if g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays > 0 {
			if maxNonDisruptionDurationDays == nil || g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays < *maxNonDisruptionDurationDays {
				val := g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays
				maxNonDisruptionDurationDays = &val
			}
		}

		if len(g.Spec.Constraints.Disruption.AllowedDisruptionDays) > 0 {
			allowedDisruptionDays = append(allowedDisruptionDays, g.Spec.Constraints.Disruption.AllowedDisruptionDays)
		}

		if g.Spec.Constraints.Disruption.MaxAllowedWindows > 0 {
			if maxAllowedWindows == nil || g.Spec.Constraints.Disruption.MaxAllowedWindows < *maxAllowedWindows {
				val := g.Spec.Constraints.Disruption.MaxAllowedWindows
				maxAllowedWindows = &val
			}
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
