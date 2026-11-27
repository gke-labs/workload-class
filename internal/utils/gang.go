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
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	// GroupPDBNamePrefix is the name prefix for WorkloadClass-managed gang PodDisruptionBudgets.
	// It is intentionally disjoint from PDBNamePrefix ("workload-") so Group PDB names can never
	// collide with Per-Pod PDB names (e.g. a Pod literally named "group-...").
	GroupPDBNamePrefix = "wcg-"

	// PodGroupLabel is the label key linking gang Pods to their shared Group PDB.
	PodGroupLabel = workloadsv1.PodGroupLabel

	lwsNameLabel       = "leaderworkerset.sigs.k8s.io/name"
	lwsGroupKeyLabel   = "leaderworkerset.sigs.k8s.io/group-key"
	lwsGroupIndexLabel = "leaderworkerset.sigs.k8s.io/group-index"
)

// IsGroupPDB reports whether pdb is a WorkloadClass-managed Group PDB, identified by its
// PodGroupLabel metadata label (rather than parsing the PDB name).
func IsGroupPDB(pdb *policyv1.PodDisruptionBudget) bool {
	return pdb != nil && pdb.Labels[PodGroupLabel] != ""
}

// IsGroupDisruptionUnit returns true when the WorkloadClass manages disruptions at the gang/group level.
func IsGroupDisruptionUnit(wc *workloadsv1.WorkloadClass) bool {
	return wc != nil && wc.Spec.DisruptionPolicy.DisruptionUnit == workloadsv1.DisruptionUnitGroup
}

// PodGroupID resolves the deterministic DNS-label-safe group identifier for a Pod under a WorkloadClass's
// gang configuration. Scoping by wcName ensures two distinct WorkloadClasses selecting disjoint subsets
// of the same gang cannot produce overlapping Group PDB selectors on the same Pod.
//
// If customLabelKey is non-empty, only that label key is inspected; otherwise well-known labels are
// checked in order: JobSet name, LeaderWorkerSet replica group (group-key, or name+group-index),
// Ray cluster, Kueue pod-group-name, and Kueue workload-name. Returns ("", false) when unmatched.
func PodGroupID(pod *corev1.Pod, wcName, customLabelKey string) (string, bool) {
	if pod == nil || len(pod.Labels) == 0 {
		return "", false
	}
	if customLabelKey != "" {
		if val := pod.Labels[customLabelKey]; val != "" {
			return podGroupLabelValue(wcName, customLabelKey, val, val), true
		}
		return "", false
	}
	if val := pod.Labels["jobset.sigs.k8s.io/jobset-name"]; val != "" {
		return podGroupLabelValue(wcName, "jobset.sigs.k8s.io/jobset-name", val, val), true
	}
	if gk := pod.Labels[lwsGroupKeyLabel]; gk != "" {
		display := gk
		if name := pod.Labels[lwsNameLabel]; name != "" {
			display = name
			if idx := pod.Labels[lwsGroupIndexLabel]; idx != "" {
				display = name + "-" + idx
			}
		}
		return podGroupLabelValue(wcName, lwsGroupKeyLabel, gk, display), true
	}
	if name, idx := pod.Labels[lwsNameLabel], pod.Labels[lwsGroupIndexLabel]; name != "" && idx != "" {
		return podGroupLabelValue(wcName, lwsNameLabel+"+"+lwsGroupIndexLabel, name+"/"+idx, name+"-"+idx), true
	}
	for _, key := range []string{
		"ray.io/cluster",
		"kueue.x-k8s.io/pod-group-name",
		"kueue.x-k8s.io/workload-name",
	} {
		if val := pod.Labels[key]; val != "" {
			return podGroupLabelValue(wcName, key, val, val), true
		}
	}
	return "", false
}

// podGroupLabelValue produces a deterministic, Kubernetes label-safe value (<= 63 chars, alphanumeric
// with '-' separators) incorporating wcName, the label key, and raw value so distinct WorkloadClasses
// receive distinct selector values even for the same underlying gang label.
func podGroupLabelValue(wcName, key, rawVal, display string) string {
	sum := sha256.Sum256([]byte(wcName + "\x00" + key + "=" + rawVal))
	hash := fmt.Sprintf("%x", sum[:6]) // 12 hex chars
	sanitized := sanitizeLabelFragment(display)
	if sanitized == "" {
		return "g-" + hash
	}
	const maxLabelLen = 63
	maxPrefix := maxLabelLen - len(hash) - 1
	if len(sanitized) > maxPrefix {
		sanitized = strings.TrimRight(sanitized[:maxPrefix], "-")
	}
	if sanitized == "" {
		return "g-" + hash
	}
	return sanitized + "-" + hash
}

func sanitizeLabelFragment(s string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(s) {
		isAlnum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlnum {
			b.WriteRune(r)
			prevDash = false
		} else if !prevDash && b.Len() > 0 {
			b.WriteByte('-')
			prevDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

// GroupPDBName returns the deterministic name of a WorkloadClass-managed Group PDB for a gang ID.
func GroupPDBName(wcName, groupID string) string {
	name := GroupPDBNamePrefix + wcName + "-" + groupID
	if len(name) <= MaxPDBNameLength {
		return name
	}
	sum := sha256.Sum256([]byte(wcName + "/" + groupID))
	suffix := fmt.Sprintf("-%x", sum[:4])
	prefix := strings.TrimRight(name[:MaxPDBNameLength-len(suffix)], "-.")
	return prefix + suffix
}

// GroupPDBBase returns a base Group PodDisruptionBudget for the given WorkloadClass and gang ID.
func GroupPDBBase(wc *workloadsv1.WorkloadClass, groupID string) *policyv1.PodDisruptionBudget {
	if wc == nil {
		return &policyv1.PodDisruptionBudget{}
	}
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GroupPDBName(wc.Name, groupID),
			Namespace: wc.Namespace,
			Labels: map[string]string{
				PodGroupLabel: groupID,
			},
		},
	}
}

// SyncGroupPDBWithWorkloadClass configures a Group PDB covering all Pods tagged with groupID.
// The base open/closed protection decision is derived from isValid, EmergencyOverride, and
// wc.Status.MaintenanceReadiness (mirroring SyncPDBWithWorkloadClass), with gangReady acting as
// an extra per-gang restriction when readiness is Ready:
//   - Open (maxUnavailable: "100%", UnhealthyPodEvictionPolicy: AlwaysAllow) when !isValid,
//     EmergencyOverride is true, MaintenanceReadiness is Overdue, or (MaintenanceReadiness is
//     Ready and gangReady is true).
//   - Closed (maxUnavailable: 0, UnhealthyPodEvictionPolicy: IfHealthyBudget) otherwise, preventing
//     unhealthy gang members from being evicted piecemeal while protected.
//
// Note: opening the PDB decides the protection state for the gang uniformly; Kubernetes still admits
// individual Pod eviction calls once open.
func SyncGroupPDBWithWorkloadClass(wc *workloadsv1.WorkloadClass, groupID string, pdb *policyv1.PodDisruptionBudget, isValid, gangReady bool) error {
	if wc == nil {
		return fmt.Errorf("failed to sync Group PDB, WorkloadClass is nil")
	}
	if groupID == "" {
		return fmt.Errorf("failed to sync Group PDB, groupID is empty")
	}
	if pdb == nil {
		return fmt.Errorf("failed to sync Group PDB, PDB is nil")
	}

	if pdb.Name == "" {
		pdb.Name = GroupPDBName(wc.Name, groupID)
	}
	if pdb.Namespace == "" {
		pdb.Namespace = wc.Namespace
	}
	if pdb.Labels == nil {
		pdb.Labels = map[string]string{}
	}
	pdb.Labels[PodGroupLabel] = groupID
	if pdb.Annotations != nil {
		delete(pdb.Annotations, BypassOwner)
		delete(pdb.Annotations, BypassExpiration)
	}

	pdb.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{
			PodGroupLabel: groupID,
		},
	}
	pdb.Spec.MinAvailable = nil

	open := !isValid ||
		wc.Spec.DisruptionPolicy.EmergencyOverride ||
		wc.Status.MaintenanceReadiness == workloadsv1.ReadinessOverdue ||
		(wc.Status.MaintenanceReadiness == workloadsv1.ReadinessReady && gangReady)

	if open {
		policy := policyv1.AlwaysAllow
		pdb.Spec.UnhealthyPodEvictionPolicy = &policy
		pdb.Spec.MaxUnavailable = &intstr.IntOrString{Type: openType, StrVal: openValue}
	} else {
		policy := policyv1.IfHealthyBudget
		pdb.Spec.UnhealthyPodEvictionPolicy = &policy
		pdb.Spec.MaxUnavailable = &intstr.IntOrString{Type: closedType, IntVal: closedValue}
	}
	return nil
}

// GroupPDBWithLease opens a Group PDB temporarily with a bypass lease owned by subject.
func GroupPDBWithLease(wc *workloadsv1.WorkloadClass, groupID string, pdb *policyv1.PodDisruptionBudget, subject workloadsv1.Subject, leaseDuration time.Duration) error {
	if wc == nil || groupID == "" || pdb == nil {
		return fmt.Errorf("failed to create Group PDB lease, missing workload class, groupID, or PDB")
	}
	if leaseDuration <= 0 {
		leaseDuration = DefaultLeaseDuration
	}
	if pdb.Labels == nil {
		pdb.Labels = map[string]string{}
	}
	pdb.Labels[PodGroupLabel] = groupID
	if pdb.Annotations == nil {
		pdb.Annotations = map[string]string{}
	}
	pdb.Annotations[BypassOwner] = BypassOwnerValue(subject)
	pdb.Annotations[BypassExpiration] = time.Now().UTC().Add(leaseDuration).Format(ExpirationFormat)
	pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{PodGroupLabel: groupID}}
	pdb.Spec.MinAvailable = nil
	policy := policyv1.AlwaysAllow
	pdb.Spec.UnhealthyPodEvictionPolicy = &policy
	pdb.Spec.MaxUnavailable = &intstr.IntOrString{Type: openType, StrVal: openValue}
	return nil
}

// AllowGroupLease reports whether a new bypass lease may be placed on a managed Group PDB.
func AllowGroupLease(pdb *policyv1.PodDisruptionBudget, groupID string) bool {
	if !IsWorkloadClassManagedPDB(pdb) || pdb.Labels[PodGroupLabel] != groupID {
		return false
	}
	if pdb.Annotations[BypassOwner] == "" {
		return true
	}
	return LeaseExpired(pdb)
}

// GroupSubjectAlreadyLeasing reports whether subject already holds an active lease on the Group PDB.
func GroupSubjectAlreadyLeasing(pdb *policyv1.PodDisruptionBudget, groupID string, subject workloadsv1.Subject) bool {
	if !IsWorkloadClassManagedPDB(pdb) || pdb.Labels[PodGroupLabel] != groupID {
		return false
	}
	return pdb.Annotations[BypassOwner] == BypassOwnerValue(subject) && !LeaseExpired(pdb)
}
