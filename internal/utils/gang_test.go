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
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestPodGroupID(t *testing.T) {
	tests := []struct {
		name           string
		labels         map[string]string
		wcName         string
		customLabelKey string
		wantFound      bool
		wantPrefix     string
	}{
		{name: "no_labels", wantFound: false},
		{name: "detects_jobset_label", labels: map[string]string{"jobset.sigs.k8s.io/jobset-name": "train-llm"}, wcName: "wc", wantFound: true, wantPrefix: "train-llm-"},
		{name: "detects_lws_group_key", labels: map[string]string{"leaderworkerset.sigs.k8s.io/name": "vllm", "leaderworkerset.sigs.k8s.io/group-index": "0", "leaderworkerset.sigs.k8s.io/group-key": "abc123"}, wcName: "wc", wantFound: true, wantPrefix: "vllm-0-"},
		{name: "detects_lws_name_and_index", labels: map[string]string{"leaderworkerset.sigs.k8s.io/name": "vllm", "leaderworkerset.sigs.k8s.io/group-index": "2"}, wcName: "wc", wantFound: true, wantPrefix: "vllm-2-"},
		{name: "ignores_bare_lws_name_without_group_identity", labels: map[string]string{"leaderworkerset.sigs.k8s.io/name": "vllm"}, wcName: "wc", wantFound: false},
		{name: "detects_ray_cluster_label", labels: map[string]string{"ray.io/cluster": "ray-job-1"}, wcName: "wc", wantFound: true, wantPrefix: "ray-job-1-"},
		{name: "detects_kueue_pod_group_label", labels: map[string]string{"kueue.x-k8s.io/pod-group-name": "pg-alpha"}, wcName: "wc", wantFound: true, wantPrefix: "pg-alpha-"},
		{name: "detects_kueue_workload_label", labels: map[string]string{"kueue.x-k8s.io/workload-name": "kueue-wl-42"}, wcName: "wc", wantFound: true, wantPrefix: "kueue-wl-42-"},
		{name: "custom_label_key_overrides_defaults", labels: map[string]string{"jobset.sigs.k8s.io/jobset-name": "js-1", "training.io/gang": "gang_A"}, wcName: "wc", customLabelKey: "training.io/gang", wantFound: true, wantPrefix: "gang-a-"},
		{name: "custom_label_key_missing_returns_false", labels: map[string]string{"jobset.sigs.k8s.io/jobset-name": "js-1"}, wcName: "wc", customLabelKey: "training.io/gang", wantFound: false},
		{name: "long_value_truncated_to_63_chars", labels: map[string]string{"ray.io/cluster": strings.Repeat("a", 100)}, wcName: "wc", wantFound: true, wantPrefix: "aaaa"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Labels: tt.labels}}
			got, ok := PodGroupID(pod, tt.wcName, tt.customLabelKey)
			if ok != tt.wantFound {
				t.Fatalf("PodGroupID() found = %v, want %v (got %q)", ok, tt.wantFound, got)
			}
			if ok && (len(got) > 63 || !strings.HasPrefix(got, tt.wantPrefix)) {
				t.Errorf("PodGroupID() = %q (len %d), want prefix %q and len <= 63", got, len(got), tt.wantPrefix)
			}
		})
	}

	// Distinct LWS replica groups get distinct group IDs.
	p0 := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"leaderworkerset.sigs.k8s.io/name": "vllm", "leaderworkerset.sigs.k8s.io/group-index": "0"}}}
	p1 := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"leaderworkerset.sigs.k8s.io/name": "vllm", "leaderworkerset.sigs.k8s.io/group-index": "1"}}}
	id0, _ := PodGroupID(p0, "wc", "")
	id1, _ := PodGroupID(p1, "wc", "")
	if id0 == id1 {
		t.Errorf("distinct LWS replicas got same group ID %q", id0)
	}

	// Distinct WorkloadClasses referencing the same JobSet get distinct scoped group IDs (no overlap).
	jsPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"jobset.sigs.k8s.io/jobset-name": "train"}}}
	idA, _ := PodGroupID(jsPod, "wc-leader", "")
	idB, _ := PodGroupID(jsPod, "wc-worker", "")
	if idA == idB {
		t.Errorf("distinct WorkloadClasses got identical group ID %q", idA)
	}
}

func TestIsGroupPDBAndPrefixIsolation(t *testing.T) {
	wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "ml"}}
	pdb := GroupPDBBase(wc, "g-1")
	if !strings.HasPrefix(pdb.Name, "wcg-") || strings.HasPrefix(pdb.Name, PDBNamePrefix) {
		t.Fatalf("GroupPDBBase name %q must start with wcg- and not %q", pdb.Name, PDBNamePrefix)
	}
	if !IsGroupPDB(pdb) {
		t.Errorf("IsGroupPDB() = false for PDB with %s label", PodGroupLabel)
	}
	if got := PodNameFromPDBName(pdb.Name); got != "" {
		t.Errorf("PodNameFromPDBName(%q) = %q, want empty string", pdb.Name, got)
	}
	if IsGroupPDB(&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "wcg-unlabeled"}}) {
		t.Errorf("IsGroupPDB() must key on label, not name prefix")
	}
	if len(GroupPDBName(strings.Repeat("x", 250), "g-1")) > MaxPDBNameLength {
		t.Errorf("GroupPDBName exceeded %d chars", MaxPDBNameLength)
	}
}

func TestSyncGroupPDBWithWorkloadClassAndLease(t *testing.T) {
	openVal, closedVal := intstr.FromString("100%"), intstr.FromInt32(0)
	baseWC := func(readiness workloadsv1.MaintenanceReadiness, emergency bool) *workloadsv1.WorkloadClass {
		return &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "training-wc", Namespace: "ml"},
			Spec: workloadsv1.WorkloadClassSpec{
				DisruptionPolicy: workloadsv1.DisruptionPolicy{
					DisruptionUnit:    workloadsv1.DisruptionUnitGroup,
					EmergencyOverride: emergency,
				},
			},
			Status: workloadsv1.WorkloadClassStatus{MaintenanceReadiness: readiness},
		}
	}
	cases := []struct {
		name       string
		wc         *workloadsv1.WorkloadClass
		isValid    bool
		gangReady  bool
		wantOpen   bool
		wantPolicy policyv1.UnhealthyPodEvictionPolicyType
	}{
		{"invalid_opens_even_when_not_ready", baseWC(workloadsv1.ReadinessNotReady, false), false, false, true, policyv1.AlwaysAllow},
		{"overdue_opens_even_when_gang_young", baseWC(workloadsv1.ReadinessOverdue, false), true, false, true, policyv1.AlwaysAllow},
		{"emergency_override_opens_even_when_not_ready", baseWC(workloadsv1.ReadinessNotReady, true), true, false, true, policyv1.AlwaysAllow},
		{"not_ready_closed_even_when_gang_ready", baseWC(workloadsv1.ReadinessNotReady, false), true, true, false, policyv1.IfHealthyBudget},
		{"ready_but_gang_not_ready_closed", baseWC(workloadsv1.ReadinessReady, false), true, false, false, policyv1.IfHealthyBudget},
		{"ready_and_gang_ready_open", baseWC(workloadsv1.ReadinessReady, false), true, true, true, policyv1.AlwaysAllow},
	}
	groupID := "train-llm-0123456789ab"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pdb := GroupPDBBase(tc.wc, groupID)
			if err := SyncGroupPDBWithWorkloadClass(tc.wc, groupID, pdb, tc.isValid, tc.gangReady); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			wantSel := &metav1.LabelSelector{MatchLabels: map[string]string{PodGroupLabel: groupID}}
			if !reflect.DeepEqual(pdb.Spec.Selector, wantSel) {
				t.Errorf("Selector = %v, want %v", pdb.Spec.Selector, wantSel)
			}
			if tc.wantOpen && !reflect.DeepEqual(pdb.Spec.MaxUnavailable, &openVal) {
				t.Errorf("MaxUnavailable = %v, want 100%%", pdb.Spec.MaxUnavailable)
			}
			if !tc.wantOpen && !reflect.DeepEqual(pdb.Spec.MaxUnavailable, &closedVal) {
				t.Errorf("MaxUnavailable = %v, want 0", pdb.Spec.MaxUnavailable)
			}
			if pdb.Spec.UnhealthyPodEvictionPolicy == nil || *pdb.Spec.UnhealthyPodEvictionPolicy != tc.wantPolicy {
				t.Errorf("UnhealthyPodEvictionPolicy = %v, want %v", pdb.Spec.UnhealthyPodEvictionPolicy, tc.wantPolicy)
			}
		})
	}

	// Verify lease helpers round-trip and Sync clears expired lease annotations.
	isController := true
	wc := baseWC(workloadsv1.ReadinessNotReady, false)
	pdb := GroupPDBBase(wc, groupID)
	pdb.OwnerReferences = []metav1.OwnerReference{{APIVersion: "workloads.gke.io/v1", Kind: "WorkloadClass", Name: wc.Name, Controller: &isController}}
	subj := workloadsv1.Subject{Kind: "User", Name: "system:cluster-autoscaler"}
	other := workloadsv1.Subject{Kind: "User", Name: "other"}
	if !AllowGroupLease(pdb, groupID) {
		t.Fatal("expected AllowGroupLease=true on unleased Group PDB")
	}
	if err := GroupPDBWithLease(wc, groupID, pdb, subj, time.Minute); err != nil {
		t.Fatalf("GroupPDBWithLease error: %v", err)
	}
	if !GroupSubjectAlreadyLeasing(pdb, groupID, subj) || GroupSubjectAlreadyLeasing(pdb, groupID, other) || AllowGroupLease(pdb, groupID) {
		t.Fatal("unexpected lease ownership/concurrency state after GroupPDBWithLease")
	}
	if err := SyncGroupPDBWithWorkloadClass(wc, groupID, pdb, true, false); err != nil {
		t.Fatalf("Sync after lease error: %v", err)
	}
	if pdb.Annotations[BypassOwner] != "" || pdb.Annotations[BypassExpiration] != "" {
		t.Errorf("expected SyncGroupPDBWithWorkloadClass to clear lease annotations, got %v", pdb.Annotations)
	}
}
