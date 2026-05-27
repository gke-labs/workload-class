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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// WorkloadClassReconciler reconciles a WorkloadClass object
type WorkloadClassReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses/finalizers,verbs=update
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

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

	return ctrl.Result{RequeueAfter: nextReconcile}, nil
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

	// 4. Check Pod Ages (Min Initial Run)
	if wc.Spec.DisruptionPolicy.MinInitialRunDurationDays > 0 {
		pods := &corev1.PodList{}
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			return workloadsv1.ReadinessNotReady, 0, err
		}
		if err := r.List(ctx, pods, client.MatchingLabelsSelector{Selector: selector}); err != nil {
			return workloadsv1.ReadinessNotReady, 0, err
		}

		minRunDuration := time.Duration(wc.Spec.DisruptionPolicy.MinInitialRunDurationDays) * 24 * time.Hour
		for _, pod := range pods.Items {
			if now.Sub(pod.CreationTimestamp.Time) < minRunDuration {
				// Pod hasn't run long enough
				return workloadsv1.ReadinessNotReady, minRunDuration - now.Sub(pod.CreationTimestamp.Time), nil
			}
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
		if !timeZoneValid(dw.TimeZone) {
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

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClass{}).
		Owns(&workloadsv1.WorkloadClassGuardrail{}). // Re-trigger validation if guardrails change
		Named("workloadclass").
		Complete(r)
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
		valid = valid && isSubset(wcAllowedDisruptionDays, days)
	}

	return valid
}

func timeZoneValid(timeZone string) bool {
	_, err := time.LoadLocation(timeZone)
	return err == nil
}

func isSubset(subset, superset []string) bool {
	if len(subset) == 0 {
		return true
	}

	supersetMap := make(map[string]struct{}, len(superset))
	for _, d := range superset {
		supersetMap[d] = struct{}{}
	}

	for _, d := range subset {
		if _, found := supersetMap[d]; !found {
			return false
		}
	}

	return true
}
