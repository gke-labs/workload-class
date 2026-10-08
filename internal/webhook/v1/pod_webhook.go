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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// podlog is for logging in this package.
var podlog = logf.Log.WithName("pod-resource")

const workloadsGroup = "workloads.gke.io"

// SetupPodWebhookWithManager registers the webhook for Pod in the manager.
func SetupPodWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &corev1.Pod{}).
		WithDefaulter(&PodPlacementDefaulter{
			Client:   mgr.GetClient(),
			Recorder: mgr.GetEventRecorder("pod-placement-webhook"),
		}).
		WithDefaulterCustomPath("/mutate-pod-placement").
		Complete()
}

// +kubebuilder:webhook:path=/mutate-pod-placement,mutating=true,failurePolicy=fail,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpod-v1.kb.io,admissionReviewVersions=v1
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses;gkespotplacementpolicies,verbs=get;list;watch

// PodPlacementDefaulter mutates Pods on creation based on their best-matching WorkloadClass placement policy.
type PodPlacementDefaulter struct {
	Client   client.Client
	Recorder events.EventRecorder
}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Pod.
//
// The Pod's placement is derived from its best-matching WorkloadClass's CapacityStrategy:
//   - If the WorkloadClass references a GKESpotPlacementPolicy, the policy must be attached
//     (PlacementPluginAttached=True) and its GKE-specific refinements (custom compute class, fallback
//     machine families, reversion behavior) are applied on top of the CapacityStrategy.
//   - If the WorkloadClass has no InfrastructureProfileRef, the CapacityStrategy is applied with GKE
//     defaults (no compute class or machine family restrictions; reversion treated as Lazy).
//   - If the WorkloadClass references a different infrastructure profile kind, the Pod is left to that
//     profile's placement plugin and is not mutated.
//
// The webhook fails open: if the WorkloadClass or GKESpotPlacementPolicy cannot be resolved, or the
// WorkloadClass is not ready, the Pod is admitted unmodified. Because the webhook uses
// failurePolicy=fail, returning an error here would block Pod creation.
func (d *PodPlacementDefaulter) Default(ctx context.Context, pod *corev1.Pod) error {
	log := podlog.WithValues("pod", podName(pod), "namespace", pod.Namespace)

	bestWLC, err := utils.FindBestMatchWorkloadClass(ctx, d.Client, d.Recorder, pod)
	if err != nil {
		log.Error(err, "Failed to get best-matching WorkloadClass for Pod; Pod will not be mutated")
		return nil
	}
	if bestWLC == nil {
		log.V(1).Info("No best-matching WorkloadClass found for Pod; Pod will not be mutated")
		return nil
	}
	log = log.WithValues("workloadClass", bestWLC.Name)

	if bestWLC.Spec.CapacityStrategy == nil {
		log.V(1).Info("WorkloadClass has no CapacityStrategy; Pod will not be mutated")
		return nil
	}

	ipr := bestWLC.Spec.InfrastructureProfileRef
	usesSpotPolicy := referencesGKESpotPlacementPolicy(ipr)
	if ipr != nil && !usesSpotPolicy {
		log.V(1).Info("WorkloadClass references a different infrastructure profile; Pod will not be mutated", "kind", ipr.Kind, "group", ipr.Group)
		return nil
	}

	// Only mutate once the controllers have confirmed the WorkloadClass is valid against guardrails and,
	// when a GKESpotPlacementPolicy is referenced, that it has attached. The WorkloadClass controller
	// removes PlacementPluginAttached when there is no InfrastructureProfileRef, so it is only required
	// when a profile is referenced. Missing or Unknown conditions are treated as not ready.
	requiredConds := []string{workloadsv1.ConditionTypeValidated}
	if usesSpotPolicy {
		requiredConds = append(requiredConds, workloadsv1.ConditionTypePlacementPluginAttached)
	}
	for _, cType := range requiredConds {
		if !meta.IsStatusConditionTrue(bestWLC.Status.Conditions, cType) {
			c := meta.FindStatusCondition(bestWLC.Status.Conditions, cType)
			if c != nil {
				log.Info("WorkloadClass condition is not True; Pod will not be mutated", "condition", cType, "reason", c.Reason, "message", c.Message)
			} else {
				log.Info("WorkloadClass condition is not set; Pod will not be mutated", "condition", cType)
			}
			return nil
		}
	}

	var spotPolicy *workloadsv1.GKESpotPlacementPolicy
	if usesSpotPolicy {
		spotPolicy, err = d.fetchSpotPlacementPolicy(ctx, ipr)
		if err != nil {
			log.Error(err, "Failed to get GKESpotPlacementPolicy; Pod will not be mutated", "gkeSpotPlacementPolicy", ipr.Name)
			return nil
		}
		log = log.WithValues("gkeSpotPlacementPolicy", spotPolicy.Name)
	}

	if err := d.mutatePodPlacement(ctx, pod, bestWLC, spotPolicy); err != nil {
		log.Error(err, "Failed to compute Pod placement; Pod will not be mutated")
		return nil
	}

	log.Info("Mutated Pod placement", "targetSpot", isPodTargetedForSpot(pod))
	return nil
}

func referencesGKESpotPlacementPolicy(ipr *workloadsv1.InfrastructureProfileReference) bool {
	return ipr != nil &&
		ipr.Kind == workloadsv1.GKESpotPlacementPolicyKind &&
		(ipr.Group == "" || ipr.Group == workloadsGroup)
}

func (d *PodPlacementDefaulter) fetchSpotPlacementPolicy(ctx context.Context, ipr *workloadsv1.InfrastructureProfileReference) (*workloadsv1.GKESpotPlacementPolicy, error) {
	spotPlacementPolicy := &workloadsv1.GKESpotPlacementPolicy{}
	if err := d.Client.Get(ctx, types.NamespacedName{Name: ipr.Name}, spotPlacementPolicy); err != nil {
		return nil, err
	}
	return spotPlacementPolicy, nil
}

// mutatePodPlacement applies Spot or On-Demand placement to the Pod based on the WorkloadClass
// CapacityStrategy and, if referenced, the GKESpotPlacementPolicy (spotPolicy may be nil):
//   - Type OnDemand: the Pod is pinned to On-Demand nodes.
//   - Reversion None + FallbackToOnDemand + workload already in fallback: the Pod stays on On-Demand.
//   - SpotRatio < 100%: the Pod is placed on Spot while the existing Spot count is below SpotRatio of the
//     workload's Pods (including this one); otherwise it is placed on On-Demand.
//   - Otherwise the Pod targets Spot, either strictly (Fail) or preferentially (FallbackToOnDemand).
func (d *PodPlacementDefaulter) mutatePodPlacement(ctx context.Context, pod *corev1.Pod, wlc *workloadsv1.WorkloadClass, spotPolicy *workloadsv1.GKESpotPlacementPolicy) error {
	cs := wlc.Spec.CapacityStrategy

	if cs.Type == "" {
		cs.Type = workloadsv1.SpotPlacementTypeSpot
	}
	if cs.Type == workloadsv1.SpotPlacementTypeOnDemand {
		mutateForOnDemand(pod)
		return nil
	}

	spotRatio := 100
	if cs.SpotRatio != "" {
		parsed, err := parsePercentage(cs.SpotRatio)
		if err != nil {
			return fmt.Errorf("invalid spotRatio %q: %w", cs.SpotRatio, err)
		}
		spotRatio = parsed
	}

	fallbackAction := cs.FallbackAction
	if fallbackAction == "" {
		fallbackAction = workloadsv1.FallbackActionFail
	}

	// Without a GKESpotPlacementPolicy there is no reversion configuration; treat it as Lazy so new Pods
	// target Spot again (no fallback stickiness). With a policy, an unset action defaults to None.
	reversionAction := workloadsv1.LazyReversionAction
	if spotPolicy != nil {
		reversionAction = spotPolicy.Spec.Reversion.Action
		if reversionAction == "" {
			reversionAction = workloadsv1.NoneReversionAction
		}
	}

	// If reversion action is None and the workload has already fallen back to On-Demand,
	// keep new/recreated Pods on On-Demand permanently after fallback.
	if reversionAction == workloadsv1.NoneReversionAction &&
		fallbackAction == workloadsv1.FallbackActionFallbackToOnDemand &&
		d.isWorkloadInFallback(ctx, pod, wlc) {
		mutateForOnDemand(pod)
		return nil
	}

	if spotRatio < 100 {
		spotExisting, totalExisting := d.countExistingWorkloadClassPods(ctx, pod, wlc)
		if !shouldUseSpot(spotExisting, totalExisting, spotRatio) {
			mutateForOnDemand(pod)
			return nil
		}
	}

	mutateForSpot(pod, fallbackAction, spotPolicy)
	return nil
}

// podName returns the Pod's name, or its generateName prefix if the name is not yet assigned.
func podName(pod *corev1.Pod) string {
	if pod.Name != "" {
		return pod.Name
	}
	return pod.GenerateName
}
