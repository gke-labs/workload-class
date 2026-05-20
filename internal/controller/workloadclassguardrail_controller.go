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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	Sunday    = "Sunday"
	Monday    = "Monday"
	Tuesday   = "Tuesday"
	Wednesday = "Wednesday"
	Thursday  = "Thursday"
	Friday    = "Friday"
	Saturday  = "Saturday"
)

// WorkloadClassGuardrailReconciler reconciles a WorkloadClassGuardrail object
type WorkloadClassGuardrailReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the WorkloadClassGuardrail object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *WorkloadClassGuardrailReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	g := &workloadsv1.WorkloadClassGuardrail{}
	if err := r.Get(ctx, req.NamespacedName, g); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	validationCondition, err := r.validate(ctx, g)
	if err != nil {
		log.Error(err, "Failed to validate guardrail")
		return ctrl.Result{}, err
	}
	meta.SetStatusCondition(&g.Status.Conditions, validationCondition)

	return ctrl.Result{}, nil
}

func (r *WorkloadClassGuardrailReconciler) validate(ctx context.Context, g *workloadsv1.WorkloadClassGuardrail) (metav1.Condition, error) {
	log := logf.FromContext(ctx)
	var violations []string
	allowedDisruptionDays, err := validateDisruptionDays(g.Spec.Constraints.Disruption.AllowedDisruptionDays)
	g.Spec.Constraints.Disruption.AllowedDisruptionDays = allowedDisruptionDays
	if err != nil {
		log.Error(err, "validation of AllowedDisruptionDays failed")
		violations = append(violations, err.Error())
	}

	return condition(violations), nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassGuardrailReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClassGuardrail{}).
		Named("workloadclassguardrail").
		Complete(r)
}

func validateDisruptionDays(allowedDisruptionDays []string) ([]string, error) {
	days := []string{Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday}
	if !isSubset(allowedDisruptionDays, days) {
		return allowedDisruptionDays, fmt.Errorf("allowedDisruptionDays contains invalid days, valid days are: %v, got %v", days, allowedDisruptionDays)
	}

	return toUniqueSet(allowedDisruptionDays), nil
}

func toUniqueSet(list []string) []string {
	uniqueSet := map[string]struct{}{}
	for _, s := range list {
		uniqueSet[s] = struct{}{}
	}
	result := []string{}
	for k, _ := range uniqueSet {
		result = append(result, k)
	}
	return result
}
