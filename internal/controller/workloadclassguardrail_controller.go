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
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
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
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *WorkloadClassGuardrailReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	g := &workloadsv1.WorkloadClassGuardrail{}
	if err := r.Get(ctx, req.NamespacedName, g); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	validationCondition := r.validate(ctx, g)
	meta.SetStatusCondition(&g.Status.Conditions, validationCondition)
	err := r.Status().Update(ctx, g)

	return ctrl.Result{}, err
}

func (r *WorkloadClassGuardrailReconciler) validate(ctx context.Context, g *workloadsv1.WorkloadClassGuardrail) metav1.Condition {
	log := logf.FromContext(ctx)
	var violations []string
	err := validateDisruptionDays(g.Spec.Constraints.Disruption.AllowedDisruptionDays)
	if err != nil {
		log.Error(err, "validation of AllowedDisruptionDays failed")
		violations = append(violations, err.Error())
	}

	return condition(violations)
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassGuardrailReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClassGuardrail{}).
		Named("workloadclassguardrail").
		Complete(r)
}

func validateDisruptionDays(allowedDisruptionDays []string) error {
	days := []string{
		time.Sunday.String(),
		time.Monday.String(),
		time.Tuesday.String(),
		time.Wednesday.String(),
		time.Thursday.String(),
		time.Friday.String(),
		time.Saturday.String(),
	}

	if !isSubset(allowedDisruptionDays, days) {
		return fmt.Errorf("allowedDisruptionDays contains invalid days, valid days are: %v, got %v", days, allowedDisruptionDays)
	}

	return nil
}
