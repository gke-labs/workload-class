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
	"math/rand"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// SpotPlacementWebhook handles Pod creation requests to inject Spot tolerations.
type SpotPlacementWebhook struct {
	Client  client.Client
	decoder *admission.Decoder
}

// +kubebuilder:webhook:path=/mutate-pod-placement,mutating=true,failurePolicy=ignore,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpodplacement.gke.io,admissionReviewVersions=v1

// Handle handles admission requests for Pod placements.
func (v *SpotPlacementWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace)

	pod := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Reuse existing bestMatchWorkloadClass method
	dw := &DisruptionWebhook{Client: v.Client}
	bestWC, _ := dw.bestMatchWorkloadClass(ctx, req, pod)

	if bestWC == nil {
		return admission.Allowed("No WorkloadClass matches this pod")
	}

	// Check if this workload class has the gke-spot-placement plugin
	placementPlugin, ok := bestWC.Spec.Plugins["placement"]
	if !ok || placementPlugin.PluginName != "gke-spot-placement" || placementPlugin.ConfigRef == nil {
		return admission.Allowed("No spot placement plugin configured")
	}

	// Fetch SpotPlacementConfig
	spotConfig := &workloadsv1.SpotPlacementConfig{}
	err := v.Client.Get(ctx, client.ObjectKey{Name: placementPlugin.ConfigRef.Name, Namespace: req.Namespace}, spotConfig)
	if err != nil {
		log.Error(err, "SpotPlacementConfig not found", "name", placementPlugin.ConfigRef.Name)
		return admission.Allowed("SpotPlacementConfig not found or error")
	}

	// Determine if this Pod should be placed on a Spot instance
	rand.Seed(time.Now().UnixNano())
	if rand.Int31n(100) < spotConfig.Spec.SpotRatioPercent {
		log.Info("Mutating Pod to be scheduled on Spot instance")

		// Add Toleration
		toleration := corev1.Toleration{
			Key:      "cloud.google.com/gke-spot",
			Operator: corev1.TolerationOpEqual,
			Value:    "true",
			Effect:   corev1.TaintEffectNoSchedule,
		}
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, toleration)

		// Add NodeAffinity (NodeSelectorRequirement)
		if pod.Spec.Affinity == nil {
			pod.Spec.Affinity = &corev1.Affinity{}
		}
		if pod.Spec.Affinity.NodeAffinity == nil {
			pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
		}
		if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
		}

		nodeSelector := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if len(nodeSelector.NodeSelectorTerms) == 0 {
			nodeSelector.NodeSelectorTerms = append(nodeSelector.NodeSelectorTerms, corev1.NodeSelectorTerm{})
		}

		requirement := corev1.NodeSelectorRequirement{
			Key:      "cloud.google.com/gke-spot",
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{"true"},
		}

		for i := range nodeSelector.NodeSelectorTerms {
			nodeSelector.NodeSelectorTerms[i].MatchExpressions = append(nodeSelector.NodeSelectorTerms[i].MatchExpressions, requirement)
		}
	}

	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

// InjectDecoder injects the decoder.
func (v *SpotPlacementWebhook) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}
