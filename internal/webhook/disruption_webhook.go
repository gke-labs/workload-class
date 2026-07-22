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
	"slices"
	"strings"
	"time"

	"github.com/gke-labs/workload-class/internal/utils"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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

	// 5. Temporal Enforcement
	now := time.Now().UTC()
	inWindow, _ := utils.IsTimeInWindows(ctx, now, bestWC.Spec.DisruptionPolicy.AllowedDisruptionWindows)

	// 6. Maintenance Starvation (Override on Overdue)
	if bestWC.Status.MaintenanceReadiness == workloadsv1.ReadinessOverdue {
		return admission.Allowed("Workload class is overdue for maintenance, bypassing constraints")
	}

	if !inWindow {
		// 6.1 Indentity-Based Filtering
		return v.tryBypassWindowByIdentity(ctx, bestWC, req, pod)
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
// Selection preference is given to the namespace's default WorkloadClass.
// If no default is defined, the WorkloadClass in the Pod's namespace that has the
// highest number of matching labels and expressions against the Pod is selected.
// If two or more WorkloadClasses match in specificity to the Pod, the oldest
// WorkloadClass takes precedence.
//
// If no specific or default WorkloadClass is found, it returns nil.
func (v *DisruptionWebhook) bestMatchWorkloadClass(ctx context.Context, req admission.Request, pod *corev1.Pod) (bestMatch *workloadsv1.WorkloadClass, err error) {
	// Use the namespace's default workload class if it exists
	if bestMatch = v.namespaceDefaultWorkloadClass(ctx, pod); bestMatch != nil {
		return bestMatch, nil
	}

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

func (v *DisruptionWebhook) namespaceDefaultWorkloadClass(ctx context.Context, pod *corev1.Pod) *workloadsv1.WorkloadClass {
	ns := &corev1.Namespace{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: pod.Namespace}, ns); err == nil && len(ns.GetLabels()) > 0 {
		if defaultClass, ok := ns.Labels[workloadsv1.DefaultClassLabel]; ok {
			wc := &workloadsv1.WorkloadClass{}
			if err := v.Client.Get(ctx, client.ObjectKey{Name: defaultClass}, wc); err == nil {
				return wc
			}
		}
	}
	return nil
}

func (v *DisruptionWebhook) tryBypassWindowByIdentity(ctx context.Context, wc *workloadsv1.WorkloadClass, req admission.Request, pod *corev1.Pod) admission.Response {
	for _, allowedSubject := range wc.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow {
		matches, err := matchesIdentity(req.UserInfo, allowedSubject)
		if err != nil {
			logf.FromContext(ctx).Error(err, "Failed to check if UserInfo matches allowed Subject", "subject", allowedSubject)
			continue
		}
		if matches {
			return v.tryAcquirePDBLease(ctx, wc, pod, allowedSubject)
		}
	}
	return admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", wc.Name))
}

func (v *DisruptionWebhook) tryAcquirePDBLease(ctx context.Context, wc *workloadsv1.WorkloadClass, pod *corev1.Pod, subject workloadsv1.Subject) admission.Response {
	pdb := utils.PDBBase(wc)
	// If the PDB doesn't exist (not found), it will be created
	if err := v.Client.Get(ctx, client.ObjectKey{Name: pdb.Name, Namespace: wc.Namespace}, pdb); err != nil && !errors.IsNotFound(err) {
		return admission.Denied(fmt.Sprintf("Failed to get PDB %s: %s", pdb.Name, err))
	}

	if subjectAlreadyLeasing(pdb, pod, subject) {
		return admission.Allowed("Disruption allowed for authorized user, PDB already leased for this pod")
	}

	if !utils.AllowLease(pdb) {
		return admission.Denied(fmt.Sprintf("Disruption denied, PDB %s has an ongoing lease that expires at %s", pdb.Name, pdb.Annotations[utils.BypassExpiration]))
	}

	op, err := controllerutil.CreateOrUpdate(ctx, v.Client, pdb, func() error {
		return utils.PDBWithLease(ctx, v.Client, pdb, wc, pod, subject)
	})
	if err != nil {
		return admission.Denied(fmt.Sprintf("Disruption denied, failed to lease PDB: %s", err))
	}

	return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user, PDB leased: %v", op))
}

// subjectAlreadyLeasing returns true if the PDB has a lease for the same Pod, the same Subject, and it hasn't expired yet
func subjectAlreadyLeasing(pdb *policyv1.PodDisruptionBudget, pod *corev1.Pod, subject workloadsv1.Subject) bool {
	if pdb.Annotations == nil {
		return false
	}

	sameBypassPod := pdb.Annotations[utils.BypassPod] == pod.Name
	sameBypassSubject := pdb.Annotations[utils.BypassOwner] == utils.BypassOwnerValue(subject)

	return sameBypassPod && sameBypassSubject && !utils.LeaseExpired(pdb)
}

func matchesIdentity(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	if subject.Name == "" {
		return false, fmt.Errorf("subject name cannot be empty")
	}

	switch subject.Kind {
	case rbacv1.UserKind:
		return matchesUserKind(userInfo, subject)
	case rbacv1.GroupKind:
		return matchesGroupKind(userInfo, subject)
	case rbacv1.ServiceAccountKind:
		return matchesServiceAccountKind(userInfo, subject)
	default:
		return false, fmt.Errorf("subject has invalid Kind: %s", subject.Kind)
	}
}

func matchesUserKind(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	if subject.Namespace != "" {
		return false, fmt.Errorf("subject is kind %s, but has Namespace: %s", subject.Kind, subject.Namespace)
	}

	return userInfo.Username == subject.Name, nil
}

func matchesGroupKind(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	if subject.Namespace != "" {
		return false, fmt.Errorf("subject is kind %s, but has Namespace: %s", subject.Kind, subject.Namespace)
	}

	return slices.Contains(userInfo.Groups, subject.Name), nil
}

func matchesServiceAccountKind(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	expectedUsername := fmt.Sprintf("system:serviceaccount:%s:%s", subject.Namespace, subject.Name)
	return userInfo.Username == expectedUsername, nil
}

// InjectDecoder injects the decoder.
func (v *DisruptionWebhook) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}
