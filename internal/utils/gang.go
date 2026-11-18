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

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	// GroupPDBNamePrefix is the name prefix for WorkloadClass-managed gang PodDisruptionBudgets.
	GroupPDBNamePrefix = "workload-group-"

	// PodGroupLabel is the label key linking gang Pods to their shared Group PDB.
	PodGroupLabel = workloadsv1.PodGroupLabel
)

// DefaultGangLabelKeys lists well-known Pod label keys identifying distributed gang membership,
// evaluated in deterministic precedence order when DisruptionPolicy.GroupLabelKey is unset.
var DefaultGangLabelKeys = []string{
	"jobset.sigs.k8s.io/jobset-name",
	"leaderworkerset.sigs.k8s.io/name",
	"ray.io/cluster",
	"kueue.x-k8s.io/workload-name",
}

// IsGroupDisruptionUnit returns true when the WorkloadClass manages disruptions at the gang/group level.
func IsGroupDisruptionUnit(wc *workloadsv1.WorkloadClass) bool {
	return wc != nil && wc.Spec.DisruptionPolicy.DisruptionUnit == workloadsv1.DisruptionUnitGroup
}

// PodGroupID resolves the deterministic DNS-label-safe group identifier for a Pod under a WorkloadClass's
// gang configuration. If customLabelKey is non-empty, only that label key is inspected; otherwise
// DefaultGangLabelKeys are checked in order. Returns ("", false) when the Pod has no matching gang label.
func PodGroupID(pod *corev1.Pod, customLabelKey string) (string, bool) {
	if pod == nil || len(pod.Labels) == 0 {
		return "", false
	}
	if customLabelKey != "" {
		if val := pod.Labels[customLabelKey]; val != "" {
			return podGroupLabelValue(customLabelKey, val), true
		}
		return "", false
	}
	for _, key := range DefaultGangLabelKeys {
		if val := pod.Labels[key]; val != "" {
			return podGroupLabelValue(key, val), true
		}
	}
	return "", false
}

// podGroupLabelValue produces a deterministic, Kubernetes label-safe value (<= 63 chars, alphanumeric
// with '-' separators) derived from the gang label key and value.
func podGroupLabelValue(key, val string) string {
	sum := sha256.Sum256([]byte(key + "=" + val))
	hash := fmt.Sprintf("%x", sum[:6]) // 12 hex chars
	sanitized := sanitizeLabelFragment(val)
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

// SyncGroupPDBWithWorkloadClass configures a Group PDB covering all Pods in the gang identified by groupID.
// When isValid is false or gangReady is true, MaxUnavailable is opened to "100%" (all-or-nothing gang eviction);
// otherwise MaxUnavailable is set to 0 so no single worker Pod can be evicted piecemeal while the gang is protected.
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

	pdb.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{
			PodGroupLabel: groupID,
		},
	}
	pdb.Spec.MinAvailable = nil

	policy := policyv1.AlwaysAllow
	pdb.Spec.UnhealthyPodEvictionPolicy = &policy

	if !isValid || gangReady {
		pdb.Spec.MaxUnavailable = &intstr.IntOrString{Type: openType, StrVal: openValue}
	} else {
		pdb.Spec.MaxUnavailable = &intstr.IntOrString{Type: closedType, IntVal: closedValue}
	}
	return nil
}
