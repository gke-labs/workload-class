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

package utils

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	PDBNamePrefix    = "workload-"
	MaxPDBNameLength = 253
	PodUIDLabel      = workloadsv1.PodUIDLabel
	BypassOwner      = "workloads.gke.io/bypass-owner"
	BypassExpiration = "workloads.gke.io/bypass-expiration"
	ExpirationFormat = "2006-01-02 15:04:05.999999999 -0700 MST"

	// DefaultLeaseDuration is the default PDB lease duration (30s) when a Pod's TerminationGracePeriodSeconds is not set.
	DefaultLeaseDuration = 30 * time.Second

	openType  = 1      // Type 1 is a string
	openValue = "100%" // Allows 100% of Pods to be unavailable

	closedType  = 0 // Type 0 is an int
	closedValue = 0 // Allows 0 Pods to be unavailable
)

// PDBName returns the name of the Per-Pod PDB based on the Pod name.
// PDBs managed by WorkloadClass have the prefix `workload-`. If adding the prefix
// exceeds the 253-character Kubernetes DNS-1123 subdomain limit, the name is
// truncated and suffixed with a deterministic 8-character SHA-256 hash of podName.
func PDBName(podName string) string {
	name := PDBNamePrefix + podName
	if len(name) <= MaxPDBNameLength {
		return name
	}
	sum := sha256.Sum256([]byte(podName))
	suffix := fmt.Sprintf("-%x", sum[:4])
	prefix := strings.TrimRight(name[:MaxPDBNameLength-len(suffix)], "-.")
	return prefix + suffix
}

// PodNameFromPDBName extracts the target Pod name from a WorkloadClass-managed PDB name.
func PodNameFromPDBName(pdbName string) string {
	return strings.TrimPrefix(pdbName, PDBNamePrefix)
}

// PDBBase returns a base Per-Pod PodDisruptionBudget configured for the given Pod,
// including its name (`workload-<pod.Name>`), namespace, and `workloads.gke.io/pod-uid` label.
func PDBBase(pod *corev1.Pod) *policyv1.PodDisruptionBudget {
	if pod == nil {
		return &policyv1.PodDisruptionBudget{}
	}
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PDBName(pod.Name),
			Namespace: pod.Namespace,
			Labels: map[string]string{
				PodUIDLabel: string(pod.UID),
			},
		},
	}
}

// SyncPDBWithWorkloadClass configures a Per-Pod PDB to match the governing WorkloadClass's disruption policy
// and validation state for a specific Pod.
//
// If isValid is false (the WorkloadClass failed guardrail validation) or MaintenanceReadiness is Ready/Overdue,
// MaxUnavailable is set to "100%". Otherwise (isValid is true and MaintenanceReadiness is NotReady),
// MaxUnavailable is set to 0.
func SyncPDBWithWorkloadClass(wc *workloadsv1.WorkloadClass, pod *corev1.Pod, pdb *policyv1.PodDisruptionBudget, isValid bool) error {
	if wc == nil {
		return fmt.Errorf("failed to sync PDB with WorkloadClass, WorkloadClass is nil")
	}
	if pod == nil {
		return fmt.Errorf("failed to sync PDB with WorkloadClass, Pod is nil")
	}
	if pdb == nil {
		return fmt.Errorf("failed to sync PDB with WorkloadClass, PDB is nil")
	}

	if pdb.Name == "" {
		pdb.Name = PDBName(pod.Name)
	}
	if pdb.Namespace == "" {
		pdb.Namespace = pod.Namespace
	}

	if pdb.Labels == nil {
		pdb.Labels = map[string]string{}
	}
	pdb.Labels[PodUIDLabel] = string(pod.UID)

	if pdb.Annotations != nil {
		delete(pdb.Annotations, BypassOwner)
		delete(pdb.Annotations, BypassExpiration)
	}

	// Scope the PDB selector strictly to this single Pod instance by UID.
	pdb.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{
			PodUIDLabel: string(pod.UID),
		},
	}

	// IfHealthyBudget will block evictions of unhealthy Pods if there is no budget.
	policy := policyv1.IfHealthyBudget
	pdb.Spec.UnhealthyPodEvictionPolicy = &policy

	open := &intstr.IntOrString{
		Type:   openType,
		StrVal: openValue,
	}
	closed := &intstr.IntOrString{
		Type:   closedType,
		IntVal: closedValue,
	}

	if !isValid {
		pdb.Spec.MaxUnavailable = open
		return nil
	}

	maxUnavailableMap := map[workloadsv1.MaintenanceReadiness]*intstr.IntOrString{
		workloadsv1.ReadinessReady:    open,
		workloadsv1.ReadinessNotReady: closed,
		workloadsv1.ReadinessOverdue:  open,
	}

	if val, ok := maxUnavailableMap[wc.Status.MaintenanceReadiness]; ok {
		pdb.Spec.MaxUnavailable = val
	} else {
		pdb.Spec.MaxUnavailable = closed
	}

	return nil
}

// BypassOwnerValue returns the value of the workloads.gke.io/bypass-owner annotation for a Subject.
func BypassOwnerValue(subject workloadsv1.Subject) string {
	switch subject.Kind {
	case "User":
		return "user:" + subject.Name
	case "Group":
		return "group:" + subject.Name
	default:
		return fmt.Sprintf("system:serviceaccount:%s:%s", subject.Namespace, subject.Name)
	}
}

// LeaseDurationForPod returns the PDB lease duration for a Pod based on its TerminationGracePeriodSeconds
// (or DeletionGracePeriodSeconds), falling back to DefaultLeaseDuration (30s) when unset or non-positive.
func LeaseDurationForPod(pod *corev1.Pod) time.Duration {
	if pod != nil {
		if pod.DeletionGracePeriodSeconds != nil && *pod.DeletionGracePeriodSeconds > 0 {
			return time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second
		}
		if pod.Spec.TerminationGracePeriodSeconds != nil && *pod.Spec.TerminationGracePeriodSeconds > 0 {
			return time.Duration(*pod.Spec.TerminationGracePeriodSeconds) * time.Second
		}
	}
	return DefaultLeaseDuration
}

// LeaseExpired returns true if the PDB lacks a valid, unexpired workloads.gke.io/bypass-expiration annotation.
func LeaseExpired(pdb *policyv1.PodDisruptionBudget) bool {
	return LeaseTimeRemaining(pdb) <= 0
}

// LeaseTimeRemaining returns the time until the PDB's workloads.gke.io/bypass-expiration annotation expires,
// or 0 if the annotation is missing, cannot be parsed, or has already expired.
func LeaseTimeRemaining(pdb *policyv1.PodDisruptionBudget) time.Duration {
	if pdb == nil || len(pdb.Annotations) == 0 {
		return 0
	}

	expirationString, hasExpirationAnnotation := pdb.Annotations[BypassExpiration]
	if !hasExpirationAnnotation {
		return 0
	}

	expirationTime, err := time.Parse(ExpirationFormat, expirationString)
	if err != nil {
		return 0
	}

	return max(time.Until(expirationTime), 0)
}

// AllowLease evaluates a Per-Pod PDB and determines whether a new lease can be granted for the given Pod.
//
// A lease can only be granted on an existing PDB that is controlled by a WorkloadClass
// (controller OwnerReference with Kind == "WorkloadClass"). If the PDB is nil or is not owned
// by a WorkloadClass (e.g., a user-created PDB), AllowLease returns false.
//
// For a WorkloadClass-owned PDB, a new lease is allowed if any of the following are true:
//   - The PDB's workloads.gke.io/pod-uid label does not match the Pod's UID (stale PDB from a prior Pod of the same name)
//   - There is no ongoing lease (missing workloads.gke.io/bypass-owner annotation)
//   - The lease has expired or has an unparseable expiration timestamp
func AllowLease(pdb *policyv1.PodDisruptionBudget, pod *corev1.Pod) bool {
	if pdb == nil || pod == nil {
		return false
	}

	if !IsWorkloadClassManagedPDB(pdb) {
		return false
	}

	if pdb.Labels[PodUIDLabel] != string(pod.UID) {
		return true
	}

	if pdb.Annotations[BypassOwner] == "" {
		return true
	}

	return LeaseExpired(pdb)
}

// SubjectAlreadyLeasing returns true if the PDB is managed by a WorkloadClass and the given subject
// currently holds an active, unexpired lease for the exact Pod instance (matching UID).
func SubjectAlreadyLeasing(pdb *policyv1.PodDisruptionBudget, pod *corev1.Pod, subject workloadsv1.Subject) bool {
	if pdb == nil || pod == nil {
		return false
	}
	if !IsWorkloadClassManagedPDB(pdb) {
		return false
	}
	if pdb.Labels[PodUIDLabel] != string(pod.UID) {
		return false
	}
	owner := pdb.Annotations[BypassOwner]
	if owner == "" || owner != BypassOwnerValue(subject) {
		return false
	}
	return !LeaseExpired(pdb)
}

// IsPDBOpen returns true if the PDB's MaxUnavailable is set to "100%", meaning it does not block evictions.
func IsPDBOpen(pdb *policyv1.PodDisruptionBudget) bool {
	if pdb == nil || pdb.Spec.MaxUnavailable == nil {
		return false
	}
	return pdb.Spec.MaxUnavailable.Type == intstr.String && pdb.Spec.MaxUnavailable.StrVal == openValue
}

// PDBWithLease configures a Per-Pod PDB with temporary lease annotations (workloads.gke.io/bypass-owner and
// workloads.gke.io/bypass-expiration), scopes its selector to the Pod's UID, and opens MaxUnavailable to 100%.
func PDBWithLease(pdb *policyv1.PodDisruptionBudget, pod *corev1.Pod, subject workloadsv1.Subject) error {
	if pdb == nil {
		return fmt.Errorf("failed to update PDB with lease, PDB is nil")
	}
	if pod == nil {
		return fmt.Errorf("failed to update PDB with lease, Pod is nil")
	}

	if pdb.Name == "" {
		pdb.Name = PDBName(pod.Name)
	}
	if pdb.Namespace == "" {
		pdb.Namespace = pod.Namespace
	}

	if pdb.Labels == nil {
		pdb.Labels = map[string]string{}
	}
	pdb.Labels[PodUIDLabel] = string(pod.UID)

	pdb.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{
			PodUIDLabel: string(pod.UID),
		},
	}

	policy := policyv1.IfHealthyBudget
	pdb.Spec.UnhealthyPodEvictionPolicy = &policy

	pdb.Spec.MaxUnavailable = &intstr.IntOrString{
		Type:   openType,
		StrVal: openValue,
	}

	if pdb.Annotations == nil {
		pdb.Annotations = map[string]string{}
	}
	pdb.Annotations[BypassOwner] = BypassOwnerValue(subject)
	pdb.Annotations[BypassExpiration] = time.Now().Add(LeaseDurationForPod(pod)).Format(ExpirationFormat)

	return nil
}

// IsWorkloadClassManagedPDB returns true if the PDB has a controller OwnerReference
// with Kind == "WorkloadClass".
func IsWorkloadClassManagedPDB(pdb *policyv1.PodDisruptionBudget) bool {
	if pdb == nil {
		return false
	}
	owner := metav1.GetControllerOf(pdb)
	return owner != nil && owner.Kind == "WorkloadClass"
}

// ListUserCreatedPDBs lists all PodDisruptionBudgets in the given namespace that are not managed by WorkloadClass.
func ListUserCreatedPDBs(ctx context.Context, c client.Client, namespace string) ([]policyv1.PodDisruptionBudget, error) {
	pdbList := &policyv1.PodDisruptionBudgetList{}
	if err := c.List(ctx, pdbList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list PodDisruptionBudgets in namespace %s: %w", namespace, err)
	}

	var userPDBs []policyv1.PodDisruptionBudget
	for _, pdb := range pdbList.Items {
		if !IsWorkloadClassManagedPDB(&pdb) {
			userPDBs = append(userPDBs, pdb)
		}
	}
	return userPDBs, nil
}

// IsPodCoveredByUserPDB returns true if any user-created PodDisruptionBudget in userPDBs
// is in the Pod's namespace and has a selector that matches the Pod's labels.
func IsPodCoveredByUserPDB(pod *corev1.Pod, userPDBs []policyv1.PodDisruptionBudget) bool {
	if pod == nil {
		return false
	}
	podLabels := labels.Set(pod.Labels)
	for i := range userPDBs {
		pdb := &userPDBs[i]
		if pdb.Namespace != pod.Namespace || IsWorkloadClassManagedPDB(pdb) || pdb.Spec.Selector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		if selector.Matches(podLabels) {
			return true
		}
	}
	return false
}
