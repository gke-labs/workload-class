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
	"sync"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// recentEvictionTTL bounds how long a recently allowed eviction remains tracked against the
// in-memory pacing budget before assuming it either took effect (pod left cache / became unready)
// or was dropped upstream (e.g., rejected by a closed Per-Pod PDB or client cancellation).
const recentEvictionTTL = 2 * time.Minute

// DisruptionWebhook handles Pod eviction requests.
type DisruptionWebhook struct {
	Client   client.Client
	decoder  *admission.Decoder
	Recorder events.EventRecorder

	// LeaseTracker receives Per-Pod PDB lease requests for authorized subjects, which are applied
	// asynchronously by the WorkloadClass controller. If nil, no leases are requested.
	LeaseTracker *utils.PendingLeaseTracker

	// BlockedTracker tracks first-blocked/last-seen timestamps and per-class break-glass cooldowns
	// for pods denied by in-window disruption pacing without mutating Pods.
	// Note: state is in-memory per webhook replica and not shared across manager replicas.
	BlockedTracker *utils.BlockedPodTracker

	// pacingMu serializes checkDisruptionPacing budget evaluations and allowedEvictions updates
	// so concurrent eviction requests against a stale informer cache cannot both claim the last slot.
	pacingMu         sync.Mutex
	allowedEvictions map[types.NamespacedName]map[types.UID]time.Time

	// nowFunc overrides the clock in unit tests; defaults to time.Now().UTC().
	nowFunc func() time.Time
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

	// Check Guardrail validation (if it fails, we don't enforce constraints). A WorkloadClass that is only
	// flagged for an overlapping PodSelector (SelectorOverlap) still complies with the guardrails, and this Pod
	// resolved to it as its best match, so its constraints are still enforced.
	if cond := meta.FindStatusCondition(bestWC.Status.Conditions, workloadsv1.ConditionTypeValidated); cond != nil &&
		cond.Status == metav1.ConditionFalse && !utils.IsValidated(cond) {
		log.Info("WorkloadClass failed Guardrail validation, ignoring constraints")
		return admission.Allowed("WorkloadClass failed Guardrail validation")
	}

	// Emergency Override
	if bestWC.Spec.DisruptionPolicy.EmergencyOverride {
		return admission.Allowed("Emergency override active")
	}

	// Maintenance Starvation (Override on Overdue) takes precedence for both normal and authorized subjects.
	if bestWC.Status.MaintenanceReadiness == workloadsv1.ReadinessOverdue {
		return admission.Allowed("Workload class is overdue for maintenance, bypassing constraints")
	}

	// Temporal window check
	now := v.now()
	dryRun := req.DryRun != nil && *req.DryRun
	inWindow, _ := utils.IsTimeInWindows(ctx, now, bestWC.Spec.DisruptionPolicy.AllowedDisruptionWindows)

	// Identity-Based Filtering
	var matchingSubjects []workloadsv1.Subject
	for _, allowedSubject := range bestWC.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow {
		allowed, err := matchesIdentity(req.UserInfo, allowedSubject)
		if err != nil {
			log.Error(err, "Failed to check if UserInfo matches allowed Subject", "subject", allowedSubject)
		}
		if allowed {
			matchingSubjects = append(matchingSubjects, allowedSubject)
		}
	}
	if len(matchingSubjects) > 0 {
		// Outside an allowed window, authorized actors are strictly paced (no break-glass).
		// Inside a window, fall through to in-window pacing (with break-glass) so an authorized
		// actor is never more restricted than an unlisted caller, while still issuing a lease if needed.
		allowBreakGlass := inWindow
		allowed, denyMsg, recorded := v.checkDisruptionPacing(ctx, bestWC, pod, now, allowBreakGlass, dryRun)
		if !allowed {
			return admission.Denied(denyMsg)
		}
		resp := v.handleAuthorizedSubject(ctx, bestWC, pod, matchingSubjects)
		if !resp.Allowed && recorded {
			v.clearAllowedEviction(client.ObjectKeyFromObject(bestWC), pod.UID)
		}
		return resp
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

	// In-Window Concurrent Disruption Pacing (with break-glass after DisruptionStuckThresholdSeconds)
	if allowed, denyMsg, _ := v.checkDisruptionPacing(ctx, bestWC, pod, now, true, dryRun); !allowed {
		return admission.Denied(denyMsg)
	}

	return admission.Allowed("Eviction allowed by WorkloadClass policy")
}

func (v *DisruptionWebhook) now() time.Time {
	if v.nowFunc != nil {
		return v.nowFunc()
	}
	return time.Now().UTC()
}

func (v *DisruptionWebhook) clearAllowedEviction(wcKey types.NamespacedName, uid types.UID) {
	if uid == "" {
		return
	}
	v.pacingMu.Lock()
	if entries := v.allowedEvictions[wcKey]; entries != nil {
		delete(entries, uid)
		if len(entries) == 0 {
			delete(v.allowedEvictions, wcKey)
		}
	}
	v.pacingMu.Unlock()
}

// checkDisruptionPacing enforces MaxUnavailable / MinAvailable across governed Pods belonging to wc.
// Terminating Pods (deletionTimestamp set) count toward total and as unavailable (IsPodReady returns false).
// Evicting a Pod that is not Ready is always allowed, matching upstream AlwaysAllow unhealthy-pod handling.
// Single-replica workloads (total <= 1) bypass pacing. A short-lived in-memory ledger tracks recently
// allowed eviction UIDs (per webhook replica) so rapid or concurrent drains do not overshoot budget before
// informer caches reflect Pod termination or unready transitions.
//
// Returns (allowed, denyMessage, recordedInLedger).
func (v *DisruptionWebhook) checkDisruptionPacing(
	ctx context.Context,
	wc *workloadsv1.WorkloadClass,
	pod *corev1.Pod,
	now time.Time,
	allowStuckBreakGlass bool,
	dryRun bool,
) (bool, string, bool) {
	if !utils.HasPacingPolicy(wc.Spec.DisruptionPolicy) {
		return true, "", false
	}
	// Always allow evicting a Pod that is not Ready (or already terminating), since removing it
	// does not reduce healthy availability.
	if !utils.IsPodReady(pod) {
		if !dryRun {
			v.BlockedTracker.Clear(pod.UID)
		}
		return true, "", false
	}
	log := logf.FromContext(ctx).WithValues("workloadClass", wc.Name, "pod", pod.Name, "namespace", pod.Namespace)

	total, ready, liveUIDs, readyUIDs, ok := v.countGovernedPods(ctx, wc, pod.Namespace)
	if !ok || total <= 1 {
		if ok && !dryRun {
			v.BlockedTracker.Clear(pod.UID)
		}
		return true, "", false
	}

	wcKey := client.ObjectKeyFromObject(wc)
	v.pacingMu.Lock()
	defer v.pacingMu.Unlock()

	effectiveReady := v.effectiveReadyCountLocked(wcKey, pod.UID, now, ready, liveUIDs, readyUIDs)
	budget, err := utils.CalculateDisruptionBudget(wc.Spec.DisruptionPolicy, total, max(0, effectiveReady))
	if err != nil {
		log.Error(err, "Failed to calculate disruption pacing budget; allowing eviction")
		return true, "", false
	}
	if budget > 0 {
		if !dryRun {
			v.BlockedTracker.Clear(pod.UID)
		}
		return true, "", v.recordAllowedEvictionLocked(wcKey, pod.UID, now, dryRun)
	}

	if !allowStuckBreakGlass {
		return false, fmt.Sprintf("Eviction blocked: disruption pacing budget exhausted (%d/%d pods ready) for WorkloadClass %s",
			effectiveReady, total, wc.Name), false
	}

	threshold, breakGlassEnabled := utils.StuckThreshold(wc.Spec.DisruptionPolicy)
	if !breakGlassEnabled {
		return false, fmt.Sprintf("Eviction blocked: disruption pacing budget exhausted (%d/%d pods ready) for WorkloadClass %s (break-glass disabled)",
			effectiveReady, total, wc.Name), false
	}
	if dryRun {
		return false, fmt.Sprintf("Eviction blocked: disruption pacing budget exhausted (%d/%d pods ready) for WorkloadClass %s (dry-run)",
			effectiveReady, total, wc.Name), false
	}

	v.BlockedTracker.Prune(now.Add(-24 * time.Hour))
	elapsed := v.BlockedTracker.RecordOrElapsed(pod.UID, now, threshold)
	if elapsed >= threshold {
		if v.BlockedTracker.TryBreakGlass(wcKey, pod.UID, now, threshold) {
			log.Info("Disruption pacing stuck threshold exceeded, allowing break-glass eviction",
				"elapsed", elapsed.Round(time.Second), "threshold", threshold)
			return true, "", v.recordAllowedEvictionLocked(wcKey, pod.UID, now, dryRun)
		}
		return false, fmt.Sprintf("Eviction blocked: disruption pacing budget exhausted (%d/%d pods ready) for WorkloadClass %s (waiting for break-glass cooldown %v)",
			effectiveReady, total, wc.Name, threshold), false
	}

	return false, fmt.Sprintf("Eviction blocked: disruption pacing budget exhausted (%d/%d pods ready) for WorkloadClass %s (blocked for %v, break-glass after %v)",
		effectiveReady, total, wc.Name, elapsed.Round(time.Second), threshold), false
}

func (v *DisruptionWebhook) countGovernedPods(
	ctx context.Context,
	wc *workloadsv1.WorkloadClass,
	namespace string,
) (total, ready int, liveUIDs, readyUIDs map[types.UID]struct{}, ok bool) {
	log := logf.FromContext(ctx).WithValues("workloadClass", wc.Name, "namespace", namespace)
	selector, err := utils.WorkloadClassPodSelector(ctx, v.Client, wc)
	if err != nil {
		log.Error(err, "Failed to resolve PodSelector for disruption pacing; allowing eviction")
		return 0, 0, nil, nil, false
	}
	podList := &corev1.PodList{}
	if err := v.Client.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		log.Error(err, "Failed to list Pods for disruption pacing; allowing eviction")
		return 0, 0, nil, nil, false
	}
	enforcedWC, err := utils.NamespaceEnforcedWorkloadClass(ctx, v.Client, namespace)
	if err != nil {
		log.Error(err, "Failed to check namespace enforced WorkloadClass for pacing; allowing eviction")
		return 0, 0, nil, nil, false
	}
	wcList := &workloadsv1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcList, client.InNamespace(namespace)); err != nil {
		log.Error(err, "Failed to list WorkloadClasses for disruption pacing; allowing eviction")
		return 0, 0, nil, nil, false
	}

	liveUIDs = make(map[types.UID]struct{}, len(podList.Items))
	readyUIDs = make(map[types.UID]struct{}, len(podList.Items))
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if best := utils.BestMatchWorkloadClassForPod(p, enforcedWC, wcList.Items); best == nil || best.Name != wc.Name {
			continue
		}
		total++
		if p.UID != "" {
			liveUIDs[p.UID] = struct{}{}
		}
		// IsPodReady returns false when DeletionTimestamp is set, so terminating Pods count as unavailable.
		if utils.IsPodReady(p) {
			ready++
			if p.UID != "" {
				readyUIDs[p.UID] = struct{}{}
			}
		}
	}
	return total, ready, liveUIDs, readyUIDs, true
}

func (v *DisruptionWebhook) effectiveReadyCountLocked(
	wcKey types.NamespacedName,
	targetUID types.UID,
	now time.Time,
	ready int,
	liveUIDs, readyUIDs map[types.UID]struct{},
) int {
	entries := v.allowedEvictions[wcKey]
	if entries == nil {
		return ready
	}
	effectiveReady := ready
	for uid, at := range entries {
		if _, stillPresent := liveUIDs[uid]; !stillPresent || now.Sub(at) >= recentEvictionTTL {
			delete(entries, uid)
			continue
		}
		if uid == targetUID {
			continue
		}
		if _, stillReady := readyUIDs[uid]; stillReady {
			effectiveReady--
		}
	}
	if len(entries) == 0 {
		delete(v.allowedEvictions, wcKey)
	}
	return effectiveReady
}

func (v *DisruptionWebhook) recordAllowedEvictionLocked(wcKey types.NamespacedName, uid types.UID, now time.Time, dryRun bool) bool {
	if dryRun || uid == "" {
		return false
	}
	if v.allowedEvictions == nil {
		v.allowedEvictions = map[types.NamespacedName]map[types.UID]time.Time{}
	}
	if v.allowedEvictions[wcKey] == nil {
		v.allowedEvictions[wcKey] = map[types.UID]time.Time{}
	}
	_, alreadyRecorded := v.allowedEvictions[wcKey][uid]
	v.allowedEvictions[wcKey][uid] = now
	return !alreadyRecorded
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
func (v *DisruptionWebhook) handleAuthorizedSubject(ctx context.Context, wc *workloadsv1.WorkloadClass, pod *corev1.Pod, subjects []workloadsv1.Subject) admission.Response {
	subject := subjects[0]
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

	for _, s := range subjects {
		if utils.SubjectAlreadyLeasing(pdb, pod, s) {
			return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s, PDB %s already leased for this Pod", s, pdbName))
		}
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
