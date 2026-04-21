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
	"fmt"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// DisruptionWebhook handles Pod eviction requests.
type DisruptionWebhook struct {
	Client  client.Client
	decoder *admission.Decoder
}

// +kubebuilder:webhook:path=/validate-disruption,mutating=false,failurePolicy=fail,sideEffects=None,groups="",resources=pods/eviction,verbs=create,versions=v1,name=vpoddisruption.gke.io,admissionReviewVersions=v1

// Handle handles admission requests for Pod evictions.
func (v *DisruptionWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)

	// 1. Identify the Pod
	pod := &corev1.Pod{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: req.Name, Namespace: req.Namespace}, pod); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// 2. Find matching WorkloadClasses
	wcs := &workloadsv1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	var bestWC *workloadsv1.WorkloadClass
	maxSpecificity := -1

	for _, wc := range wcs.Items {
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			spec := getSpecificity(wc.Spec.PodSelector)
			if spec > maxSpecificity {
				maxSpecificity = spec
				bestWC = &wc
			} else if spec == maxSpecificity {
				// Stability: oldest takes precedence
				if bestWC == nil || wc.CreationTimestamp.Before(&bestWC.CreationTimestamp) {
					bestWC = &wc
				}
			}
		}
	}

	if bestWC == nil {
		// No WorkloadClass matches this pod, allow eviction by default.
		return admission.Allowed("No WorkloadClass matches this pod")
	}

	log = log.WithValues("workloadClass", bestWC.Name)

	// 3. Check Guardrail validation (if it fails, we don't enforce constraints)
	for _, cond := range bestWC.Status.Conditions {
		if cond.Type == workloadsv1.ConditionTypeValidated && cond.Status == metav1.ConditionFalse {
			log.Info("WorkloadClass failed Guardrail validation, ignoring constraints")
			return admission.Allowed("WorkloadClass failed Guardrail validation")
		}
	}

	// 4. Emergency Override
	if bestWC.Spec.EmergencyOverride {
		return admission.Allowed("Emergency override active")
	}

	// 4. Identity-Based Filtering
	for _, allowedUser := range bestWC.Spec.AllowedDisruptionsOutsideOfWindow {
		// Example: "VPA" maps to its service account
		if matchesIdentity(req.UserInfo.Username, allowedUser) {
			return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s", allowedUser))
		}
	}

	// 5. Temporal Enforcement
	now := time.Now().UTC()
	inWindow, _ := utils.IsTimeInWindows(now, bestWC.Spec.AllowedDisruptionWindows)

	// 6. Maintenance Starvation (Override on Overdue)
	isOverdue := false
	if bestWC.Spec.MaxNonDisruptionDurationDays > 0 && bestWC.Status.LastDisruptionTime != nil {
		maxDuration := time.Duration(bestWC.Spec.MaxNonDisruptionDurationDays) * 24 * time.Hour
		if now.Sub(bestWC.Status.LastDisruptionTime.Time) > maxDuration {
			isOverdue = true
		}
	}

	if isOverdue {
		return admission.Allowed("Workload class is overdue for maintenance, bypassing constraints")
	}

	if !inWindow {
		return admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", bestWC.Name))
	}

	// 7. Pod Lifecycle Protection (Min Initial Run)
	if bestWC.Spec.MinInitialRunDurationDays > 0 {
		minRunDuration := time.Duration(bestWC.Spec.MinInitialRunDurationDays) * 24 * time.Hour
		if now.Sub(pod.CreationTimestamp.Time) < minRunDuration {
			return admission.Denied(fmt.Sprintf("Eviction blocked: pod is too new (running for %v, required %d days)",
				now.Sub(pod.CreationTimestamp.Time).Round(time.Minute), bestWC.Spec.MinInitialRunDurationDays))
		}
	}

	return admission.Allowed("Eviction allowed by WorkloadClass policy")
}

func getSpecificity(sel *metav1.LabelSelector) int {
	if sel == nil {
		return 0
	}
	return len(sel.MatchLabels) + len(sel.MatchExpressions)
}

func matchesIdentity(username string, allowed string) bool {
	// Simple mapping for common GKE components
	if allowed == "VPA" && strings.Contains(username, "vpa-recommender") {
		return true
	}
	if allowed == "ClusterAutoscaler" && strings.Contains(username, "cluster-autoscaler") {
		return true
	}
	// Direct match
	return username == allowed || strings.HasSuffix(username, "/"+allowed)
}

// InjectDecoder injects the decoder.
func (v *DisruptionWebhook) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}
