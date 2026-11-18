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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestPodGroupID(t *testing.T) {
	tests := []struct {
		name           string
		labels         map[string]string
		customLabelKey string
		wantFound      bool
		wantPrefix     string
	}{
		{
			name:      "no_labels",
			wantFound: false,
		},
		{
			name:       "detects_jobset_label",
			labels:     map[string]string{"jobset.sigs.k8s.io/jobset-name": "train-llm"},
			wantFound:  true,
			wantPrefix: "train-llm-",
		},
		{
			name:       "detects_ray_cluster_label",
			labels:     map[string]string{"ray.io/cluster": "ray-job-1"},
			wantFound:  true,
			wantPrefix: "ray-job-1-",
		},
		{
			name:       "detects_kueue_workload_label",
			labels:     map[string]string{"kueue.x-k8s.io/workload-name": "kueue-wl-42"},
			wantFound:  true,
			wantPrefix: "kueue-wl-42-",
		},
		{
			name: "jobset_takes_precedence_over_kueue",
			labels: map[string]string{
				"jobset.sigs.k8s.io/jobset-name": "js-1",
				"kueue.x-k8s.io/workload-name":   "wl-1",
			},
			wantFound:  true,
			wantPrefix: "js-1-",
		},
		{
			name:           "custom_label_key_overrides_defaults",
			labels:         map[string]string{"jobset.sigs.k8s.io/jobset-name": "js-1", "training.io/gang": "gang_A"},
			customLabelKey: "training.io/gang",
			wantFound:      true,
			wantPrefix:     "gang-a-",
		},
		{
			name:           "custom_label_key_missing_returns_false",
			labels:         map[string]string{"jobset.sigs.k8s.io/jobset-name": "js-1"},
			customLabelKey: "training.io/gang",
			wantFound:      false,
		},
		{
			name:       "long_value_is_truncated_to_63_chars",
			labels:     map[string]string{"ray.io/cluster": strings.Repeat("a", 100)},
			wantFound:  true,
			wantPrefix: "aaaa",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Labels: tt.labels}}
			got, ok := PodGroupID(pod, tt.customLabelKey)
			if ok != tt.wantFound {
				t.Fatalf("PodGroupID() found = %v, want %v (got %q)", ok, tt.wantFound, got)
			}
			if !tt.wantFound {
				return
			}
			if len(got) > 63 {
				t.Errorf("PodGroupID() len = %d, want <= 63 (%q)", len(got), got)
			}
			if !strings.HasPrefix(got, tt.wantPrefix) {
				t.Errorf("PodGroupID() = %q, want prefix %q", got, tt.wantPrefix)
			}
		})
	}
}

func TestSyncGroupPDBWithWorkloadClass(t *testing.T) {
	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "training-wc", Namespace: "ml"},
		Spec: workloadsv1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1.DisruptionPolicy{
				DisruptionUnit: workloadsv1.DisruptionUnitGroup,
			},
		},
	}
	if !IsGroupDisruptionUnit(wc) {
		t.Fatalf("IsGroupDisruptionUnit() = false, want true")
	}

	groupID := "train-llm-0123456789ab"
	pdb := GroupPDBBase(wc, groupID)
	if pdb.Name != "workload-group-training-wc-"+groupID {
		t.Fatalf("GroupPDBBase() name = %q", pdb.Name)
	}

	// Protected gang -> closed (maxUnavailable: 0).
	if err := SyncGroupPDBWithWorkloadClass(wc, groupID, pdb, true, false); err != nil {
		t.Fatalf("SyncGroupPDBWithWorkloadClass(protected) error = %v", err)
	}
	wantSelector := &metav1.LabelSelector{MatchLabels: map[string]string{PodGroupLabel: groupID}}
	if !reflect.DeepEqual(pdb.Spec.Selector, wantSelector) {
		t.Errorf("Selector = %v, want %v", pdb.Spec.Selector, wantSelector)
	}
	if wantClosed := intstr.FromInt32(0); !reflect.DeepEqual(pdb.Spec.MaxUnavailable, &wantClosed) {
		t.Errorf("MaxUnavailable = %v, want 0", pdb.Spec.MaxUnavailable)
	}

	// Ready gang -> open (maxUnavailable: 100%).
	if err := SyncGroupPDBWithWorkloadClass(wc, groupID, pdb, true, true); err != nil {
		t.Fatalf("SyncGroupPDBWithWorkloadClass(ready) error = %v", err)
	}
	if wantOpen := intstr.FromString("100%"); !reflect.DeepEqual(pdb.Spec.MaxUnavailable, &wantOpen) {
		t.Errorf("MaxUnavailable = %v, want 100%%", pdb.Spec.MaxUnavailable)
	}

	// Long WorkloadClass name stays <= MaxPDBNameLength.
	longName := GroupPDBName(strings.Repeat("x", 250), groupID)
	if len(longName) > MaxPDBNameLength {
		t.Errorf("GroupPDBName(long) len = %d, want <= %d", len(longName), MaxPDBNameLength)
	}
}
