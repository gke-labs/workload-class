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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// GKESpotPlacementPolicyReconciler reconciles a GKESpotPlacementPolicy object
type GKESpotPlacementPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=workloads.gke.io,resources=gkespotplacementpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.gke.io,resources=gkespotplacementpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=gkespotplacementpolicies/finalizers,verbs=update

func (r *GKESpotPlacementPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	spotPolicy := &workloadsv1.GKESpotPlacementPolicy{}
	if err := r.Get(ctx, req.NamespacedName, spotPolicy); err != nil {
		if apierrors.IsNotFound(err) {
			// Get workloadClasses that reference this specific SpotPlacementPolicy and update their status
			if updateErr := r.updateReferencingWorkloadClasses(ctx, req.Name, statusOptions{
				Status:  metav1.ConditionFalse,
				Reason:  workloadsv1.ReasonPluginResolutionFailed,
				Message: fmt.Sprintf("GKESpotPlacementPolicy '%s' not found", req.Name),
			}); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get GKESpotPlacementPolicy", "GKESpotPlacementPolicy", req.Name)
		return ctrl.Result{}, err
	}

	// Validate GKESpotPlacementPolicy against Guardrails
	validationCond, err := r.validateAgainstGuardrails(ctx, spotPolicy)
	if err != nil {
		log.Error(err, "Failed to validate GKESpotPlacementPolicy against guardrails", "GKESpotPlacementPolicy", spotPolicy.Name)
		return ctrl.Result{}, err
	}

	if meta.SetStatusCondition(&spotPolicy.Status.Conditions, validationCond) {
		if err := r.Status().Update(ctx, spotPolicy); err != nil {
			return ctrl.Result{}, err
		}
	}

	wcStatusOpts := statusOptions{
		Status:  metav1.ConditionTrue,
		Reason:  workloadsv1.ReasonProfileResolved,
		Message: fmt.Sprintf("GKESpotPlacementPolicy '%s' attached", spotPolicy.Name),
	}
	if validationCond.Status != metav1.ConditionTrue {
		wcStatusOpts = statusOptions{
			Status:  metav1.ConditionFalse,
			Reason:  workloadsv1.ReasonPluginResolutionFailed,
			Message: fmt.Sprintf("GKESpotPlacementPolicy '%s' is invalid: %s", spotPolicy.Name, validationCond.Message),
		}
	}

	if err := r.updateReferencingWorkloadClasses(ctx, spotPolicy.Name, wcStatusOpts); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *GKESpotPlacementPolicyReconciler) validateAgainstGuardrails(ctx context.Context, spotPolicy *workloadsv1.GKESpotPlacementPolicy) (metav1.Condition, error) {
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
			ObservedGeneration: spotPolicy.Generation,
			LastTransitionTime: metav1.Now(),
		}, nil
	}

	violations := validateSpotPolicyAgainstGuardrails(spotPolicy, guardrails.Items)
	return condition(spotPolicy.Generation, violations), nil
}

func validateSpotPolicyAgainstGuardrails(spotPolicy *workloadsv1.GKESpotPlacementPolicy, guardrails []workloadsv1.WorkloadClassGuardrail) []string {
	var violations []string
	seen := make(map[string]bool)

	reversionAction := spotPolicy.Spec.Reversion.Action
	if reversionAction == "" {
		reversionAction = workloadsv1.NoneReversionAction
	}

	for _, g := range guardrails {
		if g.Spec.PluginConstraints == nil {
			continue
		}
		validateAgainstPlacementConstraints(g.Spec.PluginConstraints.Placement, spotPolicy.Spec.Reversion, reversionAction, &violations, seen)
	}

	return violations
}

func validateAgainstPlacementConstraints(placement []workloadsv1.PluginConstraint, reversion workloadsv1.SpotReversionPolicy, reversionAction workloadsv1.ReversionAction, violations *[]string, seen map[string]bool) {
	for _, c := range placement {
		if c.PluginName != workloadsv1.PluginNameGKESpotPlacement || c.Parameters == nil || len(c.Parameters.Raw) == 0 {
			continue
		}

		var params workloadsv1.GKESpotGuardrailParameters
		dec := json.NewDecoder(bytes.NewReader(c.Parameters.Raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&params); err != nil {
			addViolation(fmt.Sprintf("invalid %s parameters: %v", workloadsv1.PluginNameGKESpotPlacement, err), seen, violations)
			continue
		}

		if params.EnforcementMode == workloadsv1.ForbiddenEnforcementMode {
			addViolation("GKESpotPlacementPolicy is not allowed when guardrail enforcementMode is Forbidden", seen, violations)
		}

		validateReversionConstraints(params.Reversion, reversion, reversionAction, seen, violations)
	}
}

func validateReversionConstraints(constraints *workloadsv1.GKESpotReversionConstraints, reversion workloadsv1.SpotReversionPolicy, reversionAction workloadsv1.ReversionAction, seen map[string]bool, violations *[]string) {
	if constraints == nil {
		return
	}

	if constraints.RequiredReversionAction != "" && reversionAction != constraints.RequiredReversionAction {
		addViolation(fmt.Sprintf("reversion action %s does not match guardrail requiredReversionAction %s", reversionAction, constraints.RequiredReversionAction), seen, violations)
	}

	if constraints.MaxFallbackDuration == nil {
		return
	}

	if reversionAction == workloadsv1.NoneReversionAction {
		addViolation("reversion action cannot be None when guardrail specifies maxFallbackDuration", seen, violations)
	}

	if reversion.MinDurationOnFallback != nil &&
		reversion.MinDurationOnFallback.Duration > constraints.MaxFallbackDuration.Duration {
		addViolation(fmt.Sprintf("minDurationOnFallback %s exceeds guardrail maxFallbackDuration %s", reversion.MinDurationOnFallback.Duration, constraints.MaxFallbackDuration.Duration), seen, violations)
	}
}

func addViolation(msg string, seen map[string]bool, violations *[]string) {
	if !seen[msg] {
		seen[msg] = true
		*violations = append(*violations, msg)
	}
}

type statusOptions struct {
	// Status that will be set in the WorkloadClass.Status
	Status metav1.ConditionStatus

	// Reason that will be set in the WorkloadClass.Status
	Reason string

	// Message that will be set in the WorkloadClass.Status
	Message string
}

func (r *GKESpotPlacementPolicyReconciler) updateReferencingWorkloadClasses(ctx context.Context, policyName string, options statusOptions) error {
	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses); err != nil {
		return err
	}

	var errs []error
	for i := range workloadClasses.Items {
		wc := &workloadClasses.Items[i]
		ref := wc.Spec.InfrastructureProfileRef
		if !referencesSpotPolicy(ref) || ref.Name != policyName {
			continue
		}
		if err := r.updateWorkloadClassStatus(ctx, wc, options); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func referencesSpotPolicy(ref *workloadsv1.InfrastructureProfileReference) bool {
	if ref == nil || ref.Kind != workloadsv1.GKESpotPlacementPolicyKind {
		return false
	}
	return ref.Group == "" || ref.Group == workloadsv1.GroupVersion.Group
}

func (r *GKESpotPlacementPolicyReconciler) updateWorkloadClassStatus(ctx context.Context, wc *workloadsv1.WorkloadClass, options statusOptions) error {
	key := client.ObjectKeyFromObject(wc)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &workloadsv1.WorkloadClass{}
		if err := r.Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		cond := metav1.Condition{
			Type:               workloadsv1.ConditionTypePlacementPluginAttached,
			Status:             options.Status,
			Reason:             options.Reason,
			Message:            options.Message,
			ObservedGeneration: latest.Generation,
			LastTransitionTime: metav1.Now(),
		}
		if meta.SetStatusCondition(&latest.Status.Conditions, cond) {
			return r.Status().Update(ctx, latest)
		}
		return nil
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *GKESpotPlacementPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.GKESpotPlacementPolicy{}).
		Watches(
			&workloadsv1.WorkloadClass{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueIfReferencesSpotPolicy),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Watches(
			&workloadsv1.WorkloadClassGuardrail{},
			handler.EnqueueRequestsFromMapFunc(r.findSpotPoliciesToReconcile),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Named("gkespotplacementpolicy").
		Complete(r)
}

func (r *GKESpotPlacementPolicyReconciler) enqueueIfReferencesSpotPolicy(ctx context.Context, obj client.Object) []reconcile.Request {
	wc, ok := obj.(*workloadsv1.WorkloadClass)
	if !ok {
		return nil
	}

	infraProfileRef := wc.Spec.InfrastructureProfileRef
	if !referencesSpotPolicy(infraProfileRef) {
		return nil
	}

	return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: infraProfileRef.Name}}}
}

func (r *GKESpotPlacementPolicyReconciler) findSpotPoliciesToReconcile(ctx context.Context, _ client.Object) []reconcile.Request {
	policies := &workloadsv1.GKESpotPlacementPolicyList{}
	if err := r.List(ctx, policies); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, len(policies.Items))
	for i, item := range policies.Items {
		requests[i] = reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name: item.GetName(),
			},
		}
	}
	return requests
}
