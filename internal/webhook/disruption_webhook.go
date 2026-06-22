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
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// DisruptionWebhook handles Pod eviction requests.
type DisruptionWebhook struct {
	Client   client.Client
	decoder  *admission.Decoder
	Recorder events.EventRecorder
}

// +kubebuilder:webhook:path=/validate-disruption,mutating=false,failurePolicy=fail,sideEffects=None,groups="",resources=pods;pods/eviction,verbs=create;delete,versions=v1,name=vpoddisruption.gke.io,admissionReviewVersions=v1

// Handle handles admission requests for Pod evictions.
func (v *DisruptionWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)

	// 1.1 Verify this is an eviction, not a standard pod create/delete
	if req.SubResource != "eviction" {
		return admission.Allowed("Not an eviction")
	}

	// 1. Identify the Pod
	pod := &corev1.Pod{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: req.Name, Namespace: req.Namespace}, pod); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// 2. Find matching WorkloadClasses
	bestWC, err := v.bestMatchWorkloadClass(ctx, req, pod)
	if err != nil {
		log.Error(err, "failed to get WorkloadClass for Pod", "pod", pod)
		return admission.Allowed("Failed to get WorkloadClass matches this pod or namespace")
	}

	if bestWC == nil {
		// No WorkloadClass matches this pod or namespace, allow eviction by default.
		return admission.Allowed("No WorkloadClass matches this pod or namespace")
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
	if bestWC.Spec.DisruptionPolicy.EmergencyOverride {
		return admission.Allowed("Emergency override active")
	}

	// 4. Identity-Based Filtering
	for _, allowedUser := range bestWC.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow {
		// Example: "VPA" maps to its service account
		if matchesIdentity(req.UserInfo.Username, allowedUser) {
			return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s", allowedUser))
		}
	}

	// 5. Temporal Enforcement
	now := time.Now().UTC()
	inWindow, _ := utils.IsTimeInWindows(ctx, now, bestWC.Spec.DisruptionPolicy.AllowedDisruptionWindows)

	// 6. Maintenance Starvation (Override on Overdue)
	if bestWC.Status.MaintenanceReadiness == workloadsv1.ReadinessOverdue {
		return admission.Allowed("Workload class is overdue for maintenance, bypassing constraints")
	}

	if !inWindow {
		return admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", bestWC.Name))
	}

	// 7. Pod Lifecycle Protection (Min Initial Run)
	if bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays > 0 {
		minRunDuration := time.Duration(bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays) * 24 * time.Hour
		if now.Sub(pod.CreationTimestamp.Time) < minRunDuration {
			return admission.Denied(fmt.Sprintf("Eviction blocked: pod is too new (running for %v, required %d days)",
				now.Sub(pod.CreationTimestamp.Time).Round(time.Minute), bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays))
		}
	}

	return admission.Allowed("Eviction allowed by WorkloadClass policy")
}

// bestMatchWorkloadClass finds the most suitable WorkloadClass for a given Pod.
//
// Selection preference is given to the WorkloadClass in the Pod's namespace
// that has the highest number of matching labels and expressions against the Pod.
// If two or more WorkloadClasses match in specificity to the Pod, the oldest
// WorkloadClass takes precedence.
//
// If no WorkloadClass matches based on labels or expressions, this function
// returns the default WorkloadClass for the Pod's namespace, if one is defined.
// If no specific or default WorkloadClass is found, it returns nil.
func (v *DisruptionWebhook) bestMatchWorkloadClass(ctx context.Context, req admission.Request, pod *corev1.Pod) (bestMatch *workloadsv1.WorkloadClass, err error) {
	wcs := &workloadsv1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %v", err)
	}

	// Keep track of all other matches to emit a warning message
	otherMatches := map[string]int{}
	maxSpecificity := -1

	for _, wc := range wcs.Items {
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			bestMatch, otherMatches = updateBestMatch(&wc, bestMatch, &maxSpecificity, otherMatches)
		}
	}

	// 2.1 Fallback to Namespace default if bestMatch is nil
	bestMatch = v.namespaceDefaultWorkloadClass(ctx, pod, bestMatch)

	// Emit warning message for WorkloadClasses that matched, but are ignored
	v.emitWarning(ctx, req, pod, bestMatch, otherMatches, maxSpecificity)

	return bestMatch, nil
}

func (v *DisruptionWebhook) emitWarning(ctx context.Context, req admission.Request, pod *corev1.Pod, bestMatch *workloadsv1.WorkloadClass, matches map[string]int, maxSpecificity int) {
	if len(matches) == 0 {
		return
	}

	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)
	log.Info(fmt.Sprintf("Multiple WorkloadClasses matched Pod %s/%s, but were not the best match: %v", pod.Namespace, pod.Name, matches))

	var matchesWithMaxSpecificity []string
	for m, s := range matches {
		if s == maxSpecificity {
			matchesWithMaxSpecificity = append(matchesWithMaxSpecificity, m)
		}
	}

	// Emit a warning specifically for those with max specificity that were not selected
	if len(matchesWithMaxSpecificity) != 0 {
		warning := fmt.Sprintf("the following WorkloadClasses match pods with the same specificity as the best match, but were not selected: %s", strings.Join(matchesWithMaxSpecificity, ", "))
		// Emit a warning event
		v.Recorder.Eventf(
			bestMatch,
			nil,
			corev1.EventTypeWarning,
			"AmbiguousMatch",
			"SelectWorkloadClass",
			"%s",
			warning,
		)
	}
}

func updateBestMatch(wc, bestMatch *workloadsv1.WorkloadClass, maxSpecificity *int, otherMatches map[string]int) (*workloadsv1.WorkloadClass, map[string]int) {
	spec := getSpecificity(wc.Spec.PodSelector)
	equalSpecOlderWC := spec == *maxSpecificity && wc.CreationTimestamp.Before(&bestMatch.CreationTimestamp)
	if spec > *maxSpecificity || equalSpecOlderWC {
		if bestMatch != nil {
			otherMatches[fmt.Sprintf("%s/%s", bestMatch.Namespace, bestMatch.Name)] = *maxSpecificity
		}
		*maxSpecificity = spec
		return wc, otherMatches
	}
	// This WC still matched the Pod, track it for logging
	otherMatches[fmt.Sprintf("%s/%s", wc.Namespace, wc.Name)] = spec
	return bestMatch, otherMatches
}

func getSpecificity(sel *metav1.LabelSelector) int {
	if sel == nil {
		return 0
	}
	return len(sel.MatchLabels) + len(sel.MatchExpressions)
}

func (v *DisruptionWebhook) namespaceDefaultWorkloadClass(ctx context.Context, pod *corev1.Pod, bestMatch *workloadsv1.WorkloadClass) *workloadsv1.WorkloadClass {
	if bestMatch == nil {
		const defaultClassAnnotation = "workloads.gke.io/default-class"
		ns := &corev1.Namespace{}
		if err := v.Client.Get(ctx, client.ObjectKey{Name: pod.Namespace}, ns); err == nil {
			if defaultClass, ok := ns.Annotations[defaultClassAnnotation]; ok {
				wc := &workloadsv1.WorkloadClass{}
				if err := v.Client.Get(ctx, client.ObjectKey{Name: defaultClass}, wc); err == nil {
					return wc
				}
			}
		}
	}
	return bestMatch
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
