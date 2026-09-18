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

package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	// SpotLabelKey is the GKE node label indicating a Spot VM node.
	SpotLabelKey = "cloud.google.com/gke-spot"
	// SpotLabelValue is the value for SpotLabelKey on Spot VM nodes.
	SpotLabelValue = "true"
	// ProvisioningLabelKey is the GKE node label indicating provisioning model ("spot" or "standard").
	ProvisioningLabelKey = "cloud.google.com/gke-provisioning"
	// MachineFamilyLabelKey is the GKE node label indicating the GCE machine family (e.g., "n2d", "t2d").
	MachineFamilyLabelKey = "cloud.google.com/machine-family"
)

// PodPlacementWebhook mutates Pods on creation based on their best-matching WorkloadClass placement policy.
type PodPlacementWebhook struct {
	Client   client.Client
	Recorder events.EventRecorder
}

// +kubebuilder:webhook:path=/mutate-v1-pod,mutating=true,failurePolicy=fail,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpodplacement.gke.io,admissionReviewVersions=v1

// Handle handles admission requests for Pod creation and mutates placement fields according to the best-matching WorkloadClass.
func (v *PodPlacementWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)

	if req.SubResource != "" || (req.Operation != "" && req.Operation != admissionv1.Create) {
		return admission.Allowed("Not a pod creation request")
	}

	if len(req.Object.Raw) == 0 {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("empty pod object in admission request"))
	}

	pod := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if pod.Namespace == "" {
		pod.Namespace = req.Namespace
	}

	bestWC, err := v.bestMatchWorkloadClass(ctx, req, pod)
	if err != nil {
		log.Error(err, "failed to get WorkloadClass for Pod", "pod", pod.Name)
		return admission.Allowed("Failed to get WorkloadClass matches this pod or namespace")
	}

	if bestWC == nil {
		return admission.Allowed("No WorkloadClass matches this pod or namespace")
	}

	log = log.WithValues("workloadClass", bestWC.Name)

	for _, cond := range bestWC.Status.Conditions {
		if cond.Type == workloadsv1.ConditionTypeValidated && cond.Status == metav1.ConditionFalse {
			log.Info("WorkloadClass failed Guardrail validation, skipping placement mutation")
			return admission.Allowed("WorkloadClass failed Guardrail validation")
		}
	}

	v.mutatePodPlacement(ctx, pod, bestWC)

	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

func (v *PodPlacementWebhook) bestMatchWorkloadClass(ctx context.Context, req admission.Request, pod *corev1.Pod) (*workloadsv1.WorkloadClass, error) {
	return findBestMatchWorkloadClass(ctx, v.Client, v.Recorder, req, pod)
}

func (v *PodPlacementWebhook) mutatePodPlacement(ctx context.Context, pod *corev1.Pod, wc *workloadsv1.WorkloadClass) {
	sp := wc.Spec.PlacementPolicy.SpotPlacement

	spType := sp.Type
	if spType == "" {
		spType = workloadsv1.SpotPlacementTypeSpot
	}

	if spType == workloadsv1.SpotPlacementTypeOnDemand {
		mutateForOnDemand(pod)
		return
	}

	spotRatio := 100
	if sp.SpotRatio != "" {
		if parsed, err := strconv.Atoi(strings.TrimSuffix(sp.SpotRatio, "%")); err == nil {
			spotRatio = parsed
		}
	}

	if spotRatio < 100 {
		spotExisting, totalExisting := v.countExistingWorkloadClassPods(ctx, pod, wc)
		shouldUseSpot := spotExisting*100 < (totalExisting+1)*spotRatio
		if !shouldUseSpot {
			mutateForOnDemand(pod)
			return
		}
	}

	mutateForSpot(pod, &sp)
}

func (v *PodPlacementWebhook) countExistingWorkloadClassPods(ctx context.Context, incomingPod *corev1.Pod, wc *workloadsv1.WorkloadClass) (spotCount, totalCount int) {
	if v == nil || v.Client == nil {
		return 0, 0
	}

	podList := &corev1.PodList{}
	if err := v.Client.List(ctx, podList, client.InNamespace(incomingPod.Namespace)); err != nil {
		return 0, 0
	}

	var selector labels.Selector
	if wc.Spec.PodSelector != nil {
		var err error
		selector, err = metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			return 0, 0
		}
	}

	for i := range podList.Items {
		existing := &podList.Items[i]
		if existing.DeletionTimestamp != nil ||
			existing.Status.Phase == corev1.PodSucceeded ||
			existing.Status.Phase == corev1.PodFailed {
			continue
		}
		if incomingPod.Name != "" && existing.Name == incomingPod.Name {
			continue
		}
		if selector != nil && !selector.Matches(labels.Set(existing.Labels)) {
			continue
		}

		totalCount++
		if isPodTargetedForSpot(existing) {
			spotCount++
		}
	}

	return spotCount, totalCount
}

func isPodTargetedForSpot(pod *corev1.Pod) bool {
	if pod.Spec.NodeSelector != nil && pod.Spec.NodeSelector[SpotLabelKey] == SpotLabelValue {
		return true
	}
	for _, t := range pod.Spec.Tolerations {
		if t.Key == SpotLabelKey && t.Value == SpotLabelValue && t.Effect == corev1.TaintEffectNoSchedule {
			return true
		}
	}
	return false
}

func mutateForOnDemand(pod *corev1.Pod) {
	if pod.Spec.NodeSelector != nil {
		delete(pod.Spec.NodeSelector, SpotLabelKey)
	}
	removeSpotToleration(pod)

	addRequiredNodeSelectorTerms(pod, []corev1.NodeSelectorTerm{
		{
			MatchExpressions: []corev1.NodeSelectorRequirement{
				{
					Key:      SpotLabelKey,
					Operator: corev1.NodeSelectorOpDoesNotExist,
				},
			},
		},
	})
}

func mutateForSpot(pod *corev1.Pod, sp *workloadsv1.SpotPlacementPolicy) {
	ensureSpotToleration(pod)

	fallbackAction := sp.Fallback.Action
	if fallbackAction == "" {
		fallbackAction = workloadsv1.FallbackActionFail
	}

	if fallbackAction == workloadsv1.FallbackActionFail {
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = make(map[string]string)
		}
		pod.Spec.NodeSelector[SpotLabelKey] = SpotLabelValue

		addRequiredNodeSelectorTerms(pod, []corev1.NodeSelectorTerm{
			{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      SpotLabelKey,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{SpotLabelValue},
					},
				},
			},
		})
		return
	}

	// FallbackToOnDemand: prefer Spot capacity (weight 100), allow On-Demand fallback
	addPreferredSchedulingTerm(pod, corev1.PreferredSchedulingTerm{
		Weight: 100,
		Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{
				{
					Key:      SpotLabelKey,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{SpotLabelValue},
				},
			},
		},
	})

	if len(sp.Fallback.AllowedMachineFamilies) > 0 {
		addRequiredNodeSelectorTerms(pod, []corev1.NodeSelectorTerm{
			{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      SpotLabelKey,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{SpotLabelValue},
					},
				},
			},
			{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      SpotLabelKey,
						Operator: corev1.NodeSelectorOpDoesNotExist,
					},
					{
						Key:      MachineFamilyLabelKey,
						Operator: corev1.NodeSelectorOpIn,
						Values:   sp.Fallback.AllowedMachineFamilies,
					},
				},
			},
		})
	}
}

func ensureSpotToleration(pod *corev1.Pod) {
	for _, t := range pod.Spec.Tolerations {
		if t.Key == SpotLabelKey &&
			(t.Operator == corev1.TolerationOpEqual || t.Operator == "") &&
			t.Value == SpotLabelValue &&
			t.Effect == corev1.TaintEffectNoSchedule {
			return
		}
	}
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
		Key:      SpotLabelKey,
		Operator: corev1.TolerationOpEqual,
		Value:    SpotLabelValue,
		Effect:   corev1.TaintEffectNoSchedule,
	})
}

func removeSpotToleration(pod *corev1.Pod) {
	if len(pod.Spec.Tolerations) == 0 {
		return
	}
	filtered := pod.Spec.Tolerations[:0]
	for _, t := range pod.Spec.Tolerations {
		if t.Key == SpotLabelKey && t.Value == SpotLabelValue && t.Effect == corev1.TaintEffectNoSchedule {
			continue
		}
		filtered = append(filtered, t)
	}
	pod.Spec.Tolerations = filtered
}

func ensureNodeAffinity(pod *corev1.Pod) *corev1.NodeAffinity {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	return pod.Spec.Affinity.NodeAffinity
}

func addRequiredNodeSelectorTerms(pod *corev1.Pod, terms []corev1.NodeSelectorTerm) {
	na := ensureNodeAffinity(pod)
	if na.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		na.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: terms,
		}
		return
	}
	na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
		na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
		terms...,
	)
}

func addPreferredSchedulingTerm(pod *corev1.Pod, term corev1.PreferredSchedulingTerm) {
	na := ensureNodeAffinity(pod)
	na.PreferredDuringSchedulingIgnoredDuringExecution = append(
		na.PreferredDuringSchedulingIgnoredDuringExecution,
		term,
	)
}
