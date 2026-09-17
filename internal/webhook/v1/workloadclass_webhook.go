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

package v1

import (
	"context"
	"fmt"
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// nolint:unused
// log is for logging in this package.
var workloadclasslog = logf.Log.WithName("workloadclass-resource")

// SetupWorkloadClassWebhookWithManager registers the webhook for WorkloadClass in the manager.
func SetupWorkloadClassWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &workloadsv1.WorkloadClass{}).
		WithValidator(&WorkloadClassCustomValidator{Client: mgr.GetClient()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-workloads-gke-io-v1-workloadclass,mutating=false,failurePolicy=fail,sideEffects=None,groups=workloads.gke.io,resources=workloadclasses,verbs=create;update,versions=v1,name=vworkloadclass-v1.kb.io,admissionReviewVersions=v1

// WorkloadClassCustomValidator struct is responsible for validating the WorkloadClass resource
// when it is created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type WorkloadClassCustomValidator struct {
	Client client.Client
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateCreate(ctx context.Context, obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon creation", "name", obj.GetName())

	return v.validatePlacementPolicy(obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateUpdate(ctx context.Context, _, newObj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon update", "name", newObj.GetName())

	return v.validatePlacementPolicy(newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type WorkloadClass.
func (v *WorkloadClassCustomValidator) ValidateDelete(_ context.Context, obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	workloadclasslog.Info("Validation for WorkloadClass upon deletion", "name", obj.GetName())

	return nil, nil
}

// validatePlacementPolicy validates the WorkloadClass's PlacementPolicy field.
func (v *WorkloadClassCustomValidator) validatePlacementPolicy(obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	return v.validateSpotPlacementPolicy(obj)
}

func (v *WorkloadClassCustomValidator) validateSpotPlacementPolicy(obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	sp := obj.Spec.PlacementPolicy.SpotPlacement

	switch sp.Type {
	case "", workloadsv1.SpotPlacementTypeSpot, workloadsv1.SpotPlacementTypeOnDemand:
	default:
		err := fmt.Errorf("invalid spotPlacement type %q: must be Spot or OnDemand", sp.Type)
		return []string{err.Error()}, err
	}

	if sp.SpotRatio != "" {
		if !percentageRegex.MatchString(sp.SpotRatio) {
			err := fmt.Errorf("invalid spotRatio %q: must be a percentage between 0%% and 100%%", sp.SpotRatio)
			return []string{err.Error()}, err
		}
		isSpot := sp.Type == "" || sp.Type == workloadsv1.SpotPlacementTypeSpot
		if isSpot && sp.SpotRatio == "0%" {
			err := fmt.Errorf("spotRatio cannot be 0%% when spotPlacement type is Spot")
			return []string{err.Error()}, err
		}
		// SpotRatio defaults to 100%
		if sp.Type == workloadsv1.SpotPlacementTypeOnDemand && sp.SpotRatio != "0%" && sp.SpotRatio != "100%" {
			err := fmt.Errorf("spotRatio cannot be set to %q when spotPlacement type is OnDemand", sp.SpotRatio)
			return []string{err.Error()}, err
		}
	}

	if warnings, err := v.validateSpotFallbackPolicy(obj); err != nil {
		return warnings, err
	}

	if warnings, err := v.validateSpotReversionPolicy(obj); err != nil {
		return warnings, err
	}

	return nil, nil
}

func (v *WorkloadClassCustomValidator) validateSpotFallbackPolicy(obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	sp := obj.Spec.PlacementPolicy.SpotPlacement
	fb := sp.Fallback

	switch fb.Action {
	case "", workloadsv1.FallbackActionFail, workloadsv1.FallbackActionFallbackToOnDemand:
	default:
		err := fmt.Errorf("invalid fallback action %q: must be Fail or FallbackToOnDemand", fb.Action)
		return []string{err.Error()}, err
	}

	if sp.Type == workloadsv1.SpotPlacementTypeOnDemand {
		if fb.Action == workloadsv1.FallbackActionFallbackToOnDemand || len(fb.AllowedMachineFamilies) > 0 {
			err := fmt.Errorf("fallback cannot be configured when spotPlacement type is OnDemand")
			return []string{err.Error()}, err
		}
	}

	if len(fb.AllowedMachineFamilies) > 0 {
		if fb.Action == "" || fb.Action == workloadsv1.FallbackActionFail {
			err := fmt.Errorf("allowedMachineFamilies cannot be set when fallback action is Fail")
			return []string{err.Error()}, err
		}
		for _, family := range fb.AllowedMachineFamilies {
			if strings.TrimSpace(family) == "" {
				err := fmt.Errorf("allowedMachineFamilies cannot contain empty machine family names")
				return []string{err.Error()}, err
			}
		}
	}

	return nil, nil
}

func (v *WorkloadClassCustomValidator) validateSpotReversionPolicy(obj *workloadsv1.WorkloadClass) (admission.Warnings, error) {
	sp := obj.Spec.PlacementPolicy.SpotPlacement
	rev := sp.Reversion

	switch rev.Action {
	case "", workloadsv1.ReversionActionActive, workloadsv1.ReversionActionLazy, workloadsv1.ReversionActionNone:
	default:
		err := fmt.Errorf("invalid reversion action %q: must be Active, Lazy, or None", rev.Action)
		return []string{err.Error()}, err
	}

	if rev.MinDurationOnFallback != nil && rev.MinDurationOnFallback.Duration < 0 {
		err := fmt.Errorf("minDurationOnFallback must be non-negative")
		return []string{err.Error()}, err
	}

	isActionConfigured := rev.Action == workloadsv1.ReversionActionActive || rev.Action == workloadsv1.ReversionActionLazy
	isDurationConfigured := rev.MinDurationOnFallback != nil && rev.MinDurationOnFallback.Duration > 0

	if sp.Type == workloadsv1.SpotPlacementTypeOnDemand && (isActionConfigured || isDurationConfigured) {
		err := fmt.Errorf("reversion cannot be configured when spotPlacement type is OnDemand")
		return []string{err.Error()}, err
	}

	if sp.Fallback.Action != workloadsv1.FallbackActionFallbackToOnDemand && (isActionConfigured || isDurationConfigured) {
		err := fmt.Errorf("reversion cannot be configured when fallback action is Fail")
		return []string{err.Error()}, err
	}

	if isDurationConfigured && rev.Action != workloadsv1.ReversionActionActive {
		err := fmt.Errorf("minDurationOnFallback can only be set when reversion action is Active")
		return []string{err.Error()}, err
	}

	return nil, nil
}
