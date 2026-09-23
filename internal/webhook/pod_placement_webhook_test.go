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
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	if err := workloadsv1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add workloadsv1 to scheme: %v", err)
	}
	return s
}

func newExistingSpotPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{"app": "worker"},
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{SpotLabelKey: SpotLabelValue},
		},
	}
}

func TestMutatePodPlacement(t *testing.T) {
	selector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"app": "worker"},
	}

	onDemandNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "ondemand-node-1",
			Labels: map[string]string{},
		},
	}
	fallbackPodOnOnDemand := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels:    map[string]string{"app": "worker"},
			},
			Spec: corev1.PodSpec{
				NodeName: "ondemand-node-1",
				Tolerations: []corev1.Toleration{
					{
						Key:      SpotLabelKey,
						Operator: corev1.TolerationOpEqual,
						Value:    SpotLabelValue,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
		}
	}

	tests := []struct {
		name         string
		initial      *corev1.Pod
		existingPods []client.Object
		wcConditions []metav1.Condition
		spotSpec     workloadsv1.SpotPlacementPolicy
		verifyPod    func(t *testing.T, pod *corev1.Pod)
	}{
		{
			name:    "default Spot with Fail fallback adds Spot nodeSelector, required affinity, and toleration",
			initial: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}},
			spotSpec: workloadsv1.SpotPlacementPolicy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "100%",
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFail,
				},
			},
			verifyPod: verifySpotFailPod,
		},
		{
			name: "OnDemand removes Spot selector/toleration and adds DoesNotExist required affinity",
			initial: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						SpotLabelKey: SpotLabelValue,
						"disk":       "ssd",
					},
					Tolerations: []corev1.Toleration{
						{
							Key:      SpotLabelKey,
							Operator: corev1.TolerationOpEqual,
							Value:    SpotLabelValue,
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
				},
			},
			spotSpec: workloadsv1.SpotPlacementPolicy{
				Type: workloadsv1.SpotPlacementTypeOnDemand,
			},
			verifyPod: verifyOnDemandPod,
		},
		{
			name:    "Spot with FallbackToOnDemand and AllowedMachineFamilies sets preferred Spot and 2 OR'ed required terms",
			initial: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}},
			spotSpec: workloadsv1.SpotPlacementPolicy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "100%",
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action:                 workloadsv1.FallbackActionFallbackToOnDemand,
					AllowedMachineFamilies: []string{"n2d", "t2d"},
				},
			},
			verifyPod: verifyFallbackAllowedFamiliesPod,
		},
		{
			name:    "Spot with 80% SpotRatio assigns 4th pod to Spot when 3/3 existing pods are Spot",
			initial: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-4", Namespace: "default"}},
			existingPods: []client.Object{
				newExistingSpotPod("worker-1"),
				newExistingSpotPod("worker-2"),
				newExistingSpotPod("worker-3"),
			},
			spotSpec: workloadsv1.SpotPlacementPolicy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "80%",
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFail,
				},
			},
			verifyPod: verifySpotFailPod,
		},
		{
			name:    "Spot with 80% SpotRatio assigns 5th pod to OnDemand when 4/4 existing pods are Spot",
			initial: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-5", Namespace: "default", Labels: map[string]string{"app": "worker"}}, Spec: corev1.PodSpec{NodeSelector: map[string]string{"disk": "ssd"}}},
			existingPods: []client.Object{
				newExistingSpotPod("worker-1"),
				newExistingSpotPod("worker-2"),
				newExistingSpotPod("worker-3"),
				newExistingSpotPod("worker-4"),
			},
			spotSpec: workloadsv1.SpotPlacementPolicy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "80%",
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFail,
				},
			},
			verifyPod: verifyOnDemandPod,
		},
		{
			name:    "Lazy reversion mutates recreated pod for Spot even when 4 existing fallback pods are on OnDemand node",
			initial: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-recreated", Namespace: "default"}},
			existingPods: []client.Object{
				onDemandNode,
				fallbackPodOnOnDemand("worker-1"),
				fallbackPodOnOnDemand("worker-2"),
				fallbackPodOnOnDemand("worker-3"),
				fallbackPodOnOnDemand("worker-4"),
			},
			spotSpec: workloadsv1.SpotPlacementPolicy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "80%",
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action:                 workloadsv1.FallbackActionFallbackToOnDemand,
					AllowedMachineFamilies: []string{"n2d", "t2d"},
				},
				Reversion: workloadsv1.SpotReversionPolicy{
					Action: workloadsv1.ReversionActionLazy,
				},
			},
			verifyPod: verifyFallbackAllowedFamiliesPod,
		},
		{
			name:    "None reversion mutates recreated pod for OnDemand when WorkloadClass is in fallback",
			initial: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-after-fallback", Namespace: "default"}, Spec: corev1.PodSpec{NodeSelector: map[string]string{"disk": "ssd"}}},
			wcConditions: []metav1.Condition{
				{
					Type:   workloadsv1.ConditionTypeInFallback,
					Status: metav1.ConditionTrue,
					Reason: workloadsv1.ReasonFallbackActive,
				},
			},
			spotSpec: workloadsv1.SpotPlacementPolicy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "100%",
				Fallback: workloadsv1.SpotFallbackPolicy{
					Action: workloadsv1.FallbackActionFallbackToOnDemand,
				},
				Reversion: workloadsv1.SpotReversionPolicy{
					Action: workloadsv1.ReversionActionNone,
				},
			},
			verifyPod: verifyOnDemandPod,
		},
	}

	scheme := newTestScheme(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wc := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: selector,
					PlacementPolicy: workloadsv1.PlacementPolicy{
						SpotPlacement: tc.spotSpec,
					},
				},
				Status: workloadsv1.WorkloadClassStatus{
					Conditions: tc.wcConditions,
				},
			}
			wh := &PodPlacementWebhook{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.existingPods...).Build(),
			}
			pod := tc.initial.DeepCopy()
			wh.mutatePodPlacement(t.Context(), pod, wc)
			tc.verifyPod(t, pod)
		})
	}
}

func verifySpotFailPod(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if got := pod.Spec.NodeSelector[SpotLabelKey]; got != SpotLabelValue {
		t.Errorf("NodeSelector[%q] = %q, want %q", SpotLabelKey, got, SpotLabelValue)
	}
	if !hasSpotToleration(pod.Spec.Tolerations) {
		t.Errorf("expected Spot toleration on pod, got %v", pod.Spec.Tolerations)
	}
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatalf("expected RequiredDuringSchedulingIgnoredDuringExecution to be set")
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 {
		t.Fatalf("expected 1 required term with 1 expression, got %v", terms)
	}
	expr := terms[0].MatchExpressions[0]
	if expr.Key != SpotLabelKey || expr.Operator != corev1.NodeSelectorOpIn || !slices.Equal(expr.Values, []string{SpotLabelValue}) {
		t.Errorf("unexpected required expression: %+v", expr)
	}
}

func verifyOnDemandPod(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if _, exists := pod.Spec.NodeSelector[SpotLabelKey]; exists {
		t.Errorf("expected SpotLabelKey to be removed from NodeSelector")
	}
	if pod.Spec.NodeSelector["disk"] != "ssd" {
		t.Errorf("expected unrelated NodeSelector 'disk=ssd' to be preserved")
	}
	if hasSpotToleration(pod.Spec.Tolerations) {
		t.Errorf("expected Spot toleration to be removed for OnDemand pod, got %v", pod.Spec.Tolerations)
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 {
		t.Fatalf("expected 1 required term, got %v", terms)
	}
	expr := terms[0].MatchExpressions[0]
	if expr.Key != SpotLabelKey || expr.Operator != corev1.NodeSelectorOpDoesNotExist {
		t.Errorf("expected Spot DoesNotExist requirement, got %+v", expr)
	}
}

func verifyFallbackAllowedFamiliesPod(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if _, exists := pod.Spec.NodeSelector[SpotLabelKey]; exists {
		t.Errorf("did not expect hard SpotLabelKey in NodeSelector when fallback is enabled")
	}
	if !hasSpotToleration(pod.Spec.Tolerations) {
		t.Errorf("expected Spot toleration on pod")
	}
	na := pod.Spec.Affinity.NodeAffinity
	if len(na.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("expected 1 preferred scheduling term, got %v", na.PreferredDuringSchedulingIgnoredDuringExecution)
	}
	pref := na.PreferredDuringSchedulingIgnoredDuringExecution[0]
	if pref.Weight != 100 || pref.Preference.MatchExpressions[0].Key != SpotLabelKey {
		t.Errorf("unexpected preferred term: %+v", pref)
	}
	terms := na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 2 {
		t.Fatalf("expected 2 required terms (Spot OR OnDemand with machine families), got %d: %v", len(terms), terms)
	}
	if terms[0].MatchExpressions[0].Key != SpotLabelKey || terms[0].MatchExpressions[0].Operator != corev1.NodeSelectorOpIn {
		t.Errorf("expected Term 0 to match Spot nodes, got %+v", terms[0])
	}
	if len(terms[1].MatchExpressions) != 2 ||
		terms[1].MatchExpressions[0].Operator != corev1.NodeSelectorOpDoesNotExist ||
		terms[1].MatchExpressions[1].Key != MachineFamilyLabelKey ||
		!slices.Equal(terms[1].MatchExpressions[1].Values, []string{"n2d", "t2d"}) {
		t.Errorf("expected Term 1 to match OnDemand with allowedMachineFamilies, got %+v", terms[1])
	}
}

func TestPodPlacementWebhook_Handle(t *testing.T) {
	scheme := newTestScheme(t)
	now := time.Now()

	wcSpot := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-spot",
			Namespace:         "default",
			CreationTimestamp: metav1.Time{Time: now},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "batch"},
			},
			PlacementPolicy: workloadsv1.PlacementPolicy{
				SpotPlacement: workloadsv1.SpotPlacementPolicy{
					Type:      workloadsv1.SpotPlacementTypeSpot,
					SpotRatio: "100%",
					Fallback: workloadsv1.SpotFallbackPolicy{
						Action: workloadsv1.FallbackActionFail,
					},
				},
			},
		},
	}

	wcMoreSpecificOnDemand := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-specific-ondemand",
			Namespace:         "default",
			CreationTimestamp: metav1.Time{Time: now},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":  "batch",
					"tier": "critical",
				},
			},
			PlacementPolicy: workloadsv1.PlacementPolicy{
				SpotPlacement: workloadsv1.SpotPlacementPolicy{
					Type: workloadsv1.SpotPlacementTypeOnDemand,
				},
			},
		},
	}

	wcInvalidatedByGuardrail := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-invalid",
			Namespace:         "default",
			CreationTimestamp: metav1.Time{Time: now},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "invalid"},
			},
			PlacementPolicy: workloadsv1.PlacementPolicy{
				SpotPlacement: workloadsv1.SpotPlacementPolicy{
					Type: workloadsv1.SpotPlacementTypeSpot,
				},
			},
		},
		Status: workloadsv1.WorkloadClassStatus{
			Conditions: []metav1.Condition{
				{
					Type:   workloadsv1.ConditionTypeValidated,
					Status: metav1.ConditionFalse,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(wcSpot, wcMoreSpecificOnDemand, wcInvalidatedByGuardrail).
		Build()

	wh := &PodPlacementWebhook{
		Client:   fakeClient,
		Recorder: events.NewFakeRecorder(10),
	}

	t.Run("mutates Pod matching Spot WorkloadClass", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "batch-pod-1",
				Namespace: "default",
				Labels:    map[string]string{"app": "batch"},
			},
		}
		raw, _ := json.Marshal(pod)
		resp := wh.Handle(t.Context(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Namespace: "default",
				Object:    runtime.RawExtension{Raw: raw},
			},
		})
		if !resp.Allowed {
			t.Fatalf("expected request to be allowed, got Result=%v", resp.Result)
		}
		if len(resp.Patches) == 0 {
			t.Fatalf("expected JSON patches to be generated for matching Spot pod")
		}
	})

	t.Run("selects more specific WorkloadClass when multiple match", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "critical-pod",
				Namespace: "default",
				Labels: map[string]string{
					"app":  "batch",
					"tier": "critical",
				},
			},
		}
		raw, _ := json.Marshal(pod)
		resp := wh.Handle(t.Context(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Namespace: "default",
				Object:    runtime.RawExtension{Raw: raw},
			},
		})
		if !resp.Allowed {
			t.Fatalf("expected request to be allowed")
		}
		if len(resp.Patches) == 0 {
			t.Fatalf("expected JSON patches for OnDemand affinity")
		}
		for _, p := range resp.Patches {
			if p.Path == "/spec/nodeSelector" {
				t.Errorf("did not expect Spot nodeSelector for OnDemand best match, got patch %+v", p)
			}
		}
	})

	t.Run("does not mutate Pod when no WorkloadClass matches", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "unmatched-pod",
				Namespace: "default",
				Labels:    map[string]string{"app": "other"},
			},
		}
		raw, _ := json.Marshal(pod)
		resp := wh.Handle(t.Context(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Namespace: "default",
				Object:    runtime.RawExtension{Raw: raw},
			},
		})
		if !resp.Allowed {
			t.Fatalf("expected request to be allowed")
		}
		if len(resp.Patches) != 0 {
			t.Errorf("expected 0 patches for unmatched pod, got %v", resp.Patches)
		}
	})

	t.Run("does not mutate Pod when matching WorkloadClass failed Guardrail validation", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "invalid-wc-pod",
				Namespace: "default",
				Labels:    map[string]string{"app": "invalid"},
			},
		}
		raw, _ := json.Marshal(pod)
		resp := wh.Handle(t.Context(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Namespace: "default",
				Object:    runtime.RawExtension{Raw: raw},
			},
		})
		if !resp.Allowed {
			t.Fatalf("expected request to be allowed")
		}
		if len(resp.Patches) != 0 {
			t.Errorf("expected 0 patches when WorkloadClass failed Guardrail validation, got %v", resp.Patches)
		}
	})

	t.Run("returns BadRequest on empty raw object", func(t *testing.T) {
		resp := wh.Handle(t.Context(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Namespace: "default",
			},
		})
		if resp.Allowed || resp.Result == nil || resp.Result.Code != http.StatusBadRequest {
			t.Errorf("expected 400 BadRequest, got Allowed=%v Result=%+v", resp.Allowed, resp.Result)
		}
	})
}

func hasSpotToleration(tolerations []corev1.Toleration) bool {
	for _, t := range tolerations {
		if t.Key == SpotLabelKey && t.Value == SpotLabelValue && t.Effect == corev1.TaintEffectNoSchedule {
			return true
		}
	}
	return false
}
