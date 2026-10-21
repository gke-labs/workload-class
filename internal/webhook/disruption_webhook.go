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
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	// LeaseTracker receives Per-Pod PDB lease requests for authorized subjects, which are applied
	// asynchronously by the WorkloadClass controller. If nil, no leases are requested.
	LeaseTracker *utils.PendingLeaseTracker
}

// +kubebuilder:webhook:path=/validate-disruption,mutating=false,failurePolicy=fail,sideEffects=None,groups="",resources=pods/eviction,verbs=create,versions=v1,name=vpoddisruption.gke.io,admissionReviewVersions=v1

// Handle handles admission requests for Pod evictions.
func (v *DisruptionWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)

	// Verify this is an eviction, not a standard pod create/delete
	if req.SubResource != "eviction" {
		return admission.Allowed("Not an eviction")
	}

	// Identify the Pod
	pod := &corev1.Pod{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: req.Name, Namespace: req.Namespace}, pod); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// 2. Find matching WorkloadClasses
	bestWC, err := utils.FindBestMatchWorkloadClass(ctx, v.Client, v.Recorder, pod)
	if err != nil {
		log.Error(err, "failed to get WorkloadClass for Pod", "pod", pod)
		return admission.Allowed("Failed to get WorkloadClass matches this pod or namespace")
	}

	if bestWC == nil {
		// No WorkloadClass matches this pod or namespace, allow eviction by default.
		return admission.Allowed("No WorkloadClass matches this pod or namespace")
	}

	log = log.WithValues("workloadClass", bestWC.Name)

	// Check Guardrail validation (if it fails, we don't enforce constraints)
	for _, cond := range bestWC.Status.Conditions {
		if cond.Type == workloadsv1.ConditionTypeValidated && cond.Status == metav1.ConditionFalse {
			log.Info("WorkloadClass failed Guardrail validation, ignoring constraints")
			return admission.Allowed("WorkloadClass failed Guardrail validation")
		}
	}

	// Emergency Override
	if bestWC.Spec.DisruptionPolicy.EmergencyOverride {
		return admission.Allowed("Emergency override active")
	}

	// Identity-Based Filtering
	for _, allowedSubject := range bestWC.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow {
		allowed, err := matchesIdentity(req.UserInfo, allowedSubject)
		if err != nil {
			log.Error(err, "Failed to check if UserInfo matches allowed Subject", "subject", allowedSubject)
		}
		if allowed {
			return v.handleAuthorizedSubject(ctx, bestWC, pod, allowedSubject)
		}
	}

	// Temporal Enforcement
	now := time.Now().UTC()
	inWindow, _ := utils.IsTimeInWindows(ctx, now, bestWC.Spec.DisruptionPolicy.AllowedDisruptionWindows)

	// Maintenance Starvation (Override on Overdue)
	if bestWC.Status.MaintenanceReadiness == workloadsv1.ReadinessOverdue {
		return admission.Allowed("Workload class is overdue for maintenance, bypassing constraints")
	}

	if !inWindow {
		return admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", bestWC.Name))
	}

	// Pod Lifecycle Protection (Min Initial Run)
	if bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays > 0 {
		minRunDuration := time.Duration(bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays) * 24 * time.Hour
		if now.Sub(pod.CreationTimestamp.Time) < minRunDuration {
			return admission.Denied(fmt.Sprintf("Eviction blocked: pod is too new (running for %v, required %d days)",
				now.Sub(pod.CreationTimestamp.Time).Round(time.Minute), bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays))
		}
	}

	return admission.Allowed("Eviction allowed by WorkloadClass policy")
}

// handleAuthorizedSubject handles an eviction request from a subject listed in the WorkloadClass'
// AllowedDisruptionsOutsideOfWindow.
//
// The webhook never writes to the PDB. Instead, it checks the Pod's Per-Pod PDB (workload-<pod.Name>) for a
// conflicting lease and, if the PDB is closed, asks the WorkloadClass controller to lease it asynchronously.
// The first eviction attempt is then rejected by the closed PDB, and the subject's retry succeeds once the
// controller has opened the PDB.
//
//   - If there is no WorkloadClass-managed PDB for the Pod (e.g., the Pod is covered by a user-created PDB, or
//     its PDB has not been created yet), the eviction is allowed and the API server evaluates any existing PDBs.
//   - If the subject already holds an active lease for this Pod, the eviction is allowed.
//   - If another subject holds an active lease for this Pod, the eviction is denied.
//   - If the PDB for this Pod instance is already open, the eviction is allowed without requesting a lease.
//   - Otherwise, a lease is requested from the controller and the eviction is allowed.
func (v *DisruptionWebhook) handleAuthorizedSubject(ctx context.Context, wc *workloadsv1.WorkloadClass, pod *corev1.Pod, subject workloadsv1.Subject) admission.Response {
	log := logf.FromContext(ctx).WithValues("pod", pod.Name, "namespace", pod.Namespace, "workloadClass", wc.Name, "subject", subject)

	pdbName := utils.PDBName(pod.Name)
	pdb := &policyv1.PodDisruptionBudget{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: pdbName, Namespace: pod.Namespace}, pdb); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to get Per-Pod PDB, allowing eviction without a lease", "pdb", pdbName)
		}
		return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s", subject))
	}

	if !utils.IsWorkloadClassManagedPDB(pdb) {
		// The PDB with this name was not created by a WorkloadClass, so it must not be leased.
		return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s", subject))
	}

	if utils.SubjectAlreadyLeasing(pdb, pod, subject) {
		return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s, PDB %s already leased for this Pod", subject, pdbName))
	}

	if !utils.AllowLease(pdb, pod) {
		return admission.Denied(fmt.Sprintf("Eviction blocked: PDB %s has an ongoing lease held by %s that expires at %s",
			pdbName, pdb.Annotations[utils.BypassOwner], pdb.Annotations[utils.BypassExpiration]))
	}

	if utils.IsPDBOpen(pdb) && pdb.Labels[utils.PodUIDLabel] == string(pod.UID) {
		// The PDB for this Pod instance does not block evictions, so no lease is needed. A stale PDB left behind
		// by a previous Pod with the same name is still leased, since the controller will soon re-sync it for
		// this Pod and may close it.
		return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s", subject))
	}

	if v.LeaseTracker == nil {
		log.Info("No lease tracker configured, allowing eviction without requesting a PDB lease")
		return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s", subject))
	}

	if !v.LeaseTracker.Enqueue(wc, pod, subject) {
		log.Info("Lease tracker event channel is full, lease will be applied on the next WorkloadClass reconcile", "pdb", pdbName)
	}
	return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s, lease on PDB %s requested", subject, pdbName))
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
